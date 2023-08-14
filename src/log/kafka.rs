use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use bytesize::ByteSize;
use compact_str::ToCompactString;
use derivative::Derivative;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{BorrowedMessage, Message as _};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use tokio::time;

use crate::log::{ByteLogProducer, ByteLogSubscriber, LogClient, LogFactory, LogOffset, LogPosition};
use crate::endpoint::{Endpoint, Params};

#[derive(Derivative)]
#[derivative(Debug)]
struct KafkaPartitionProducer {
    topic: String,
    partition: i32,
    queue: VecDeque<Vec<u8>>,
    #[derivative(Debug = "ignore")]
    producer: FutureProducer,
    #[derivative(Debug = "ignore")]
    deliveries: VecDeque<DeliveryFuture>,
}

impl KafkaPartitionProducer {
    fn new(topic: String, producer: FutureProducer) -> Self {
        KafkaPartitionProducer {
            topic,
            partition: 0,
            queue: Default::default(),
            producer,
            deliveries: Default::default(),
        }
    }

    // Try send payload synchronously. Return delivery future to retrieve result or `None` if
    // underlying queue is full.
    fn sync_send(&self, payload: &[u8]) -> Result<Option<DeliveryFuture>> {
        let record: FutureRecord<'_, [u8], _> =
            FutureRecord::to(&self.topic).partition(self.partition).payload(payload);
        match self.producer.send_result(record) {
            Ok(delivery) => Ok(Some(delivery)),
            Err((err, _)) => {
                if let Some(RDKafkaErrorCode::QueueFull) = err.rdkafka_error_code() {
                    Ok(None)
                } else {
                    Err(Error::new(err))
                }
            },
        }
    }

    /// Try send from queue. Return `false` if there is no queued payload or underlying
    /// `FutureProducer`'s send queue is full.
    fn check_queue(&mut self) -> Result<bool> {
        let Some(payload) = self.queue.front() else {
            return Ok(false);
        };
        let Some(delivery) = self.sync_send(payload)? else {
            return Ok(false);
        };
        self.deliveries.push_back(delivery);
        self.queue.pop_front().unwrap();
        Ok(true)
    }
}

#[async_trait]
impl ByteLogProducer for KafkaPartitionProducer {
    fn queue(&mut self, payload: &[u8]) -> Result<()> {
        if self.queue.is_empty() {
            if let Some(delivery) = self.sync_send(payload)? {
                self.deliveries.push_back(delivery);
            }
            return Ok(());
        }
        self.queue.push_back(payload.to_owned());
        Ok(())
    }

    async fn wait(&mut self) -> Result<LogPosition> {
        loop {
            if let Some(delivery) = self.deliveries.front_mut() {
                let result = match delivery.await? {
                    Ok((_, offset)) => Ok(into_position(offset)),
                    Err((err, _)) => Err(Error::new(err)),
                };
                self.deliveries.pop_front().unwrap();
                return result;
            } else if !self.queue.is_empty() {
                if !self.check_queue()? {
                    // TODO: Queue is full but we have no API to wait for.
                    //
                    // This could happen if the queue is shared among multiple producers. Let me figure it out later.
                    // Sleep a while is harmless anyway.
                    //
                    // The async send of FutureProducer combines queuing and sending, it could cause duplicated queuing
                    // if .await is dropped and retried, so we don't use it.
                    time::sleep(Duration::from_millis(1)).await;
                }
                continue;
            }
            std::future::pending().await
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct PartitionConsumer {
    topic: String,
    partition: i32,
    #[derivative(Debug = "ignore")]
    consumer: StreamConsumer,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct KafkaPartitionConsumer {
    #[derivative(Debug = "ignore")]
    message: Option<BorrowedMessage<'static>>,
    consumer: Arc<PartitionConsumer>,
}

impl KafkaPartitionConsumer {
    fn new(topic: String, partition: i32, consumer: StreamConsumer) -> Self {
        Self { message: None, consumer: Arc::new(PartitionConsumer { topic, partition, consumer }) }
    }
}

#[async_trait]
impl ByteLogSubscriber for KafkaPartitionConsumer {
    async fn read(&mut self) -> Result<(LogPosition, &[u8])> {
        self.message = None;
        let message = self.consumer.consumer.recv().await?;
        let payload = message.payload().unwrap_or(Default::default());
        let offset = message.offset();
        let payload = unsafe { std::mem::transmute(payload) };
        self.message = unsafe { Some(std::mem::transmute(message)) };
        return Ok((into_position(offset), payload));
    }

    async fn seek(&mut self, offset: LogOffset) -> Result<()> {
        let offset = resolve_offset(offset)?;
        let consumer = self.consumer.clone();
        let handle = tokio::task::spawn_blocking(move || {
            consumer.consumer.seek(&consumer.topic, consumer.partition, offset, Duration::from_millis(i32::MAX as u64))
        });
        handle.await??;
        Ok(())
    }

    async fn latest(&self) -> Result<LogPosition> {
        let consumer = self.consumer.clone();
        let handle = tokio::task::spawn_blocking(move || {
            consumer.consumer.fetch_watermarks(
                &consumer.topic,
                consumer.partition,
                Duration::from_millis(i32::MAX as u64),
            )
        });
        let (_, high_watermark) = handle.await??;
        Ok(into_position(high_watermark - 1))
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KafkaLogClient {
    config: ClientConfig,
    #[derivative(Debug = "ignore")]
    client: AdminClient<DefaultClientContext>,
    replication: i32,
}

fn new_topic_config<'a>(name: &'a str, replication: i32, retention_bytes: &'a str) -> NewTopic<'a> {
    NewTopic::new(name, 1, TopicReplication::Fixed(replication))
        .set("cleanup.policy", "delete")
        .set("retention.ms", "-1")
        .set("retention.bytes", retention_bytes)
    // Quotes from Apache Kafka.
    //
    // Allowing retries while setting enable.idempotence to false and max.in.flight.requests.per.connection to
    // greater than 1 will potentially change the ordering of records because if two batches are sent to a single
    // partition, and the first fails and is retried but the second succeeds, then the records in the second batch
    // may appear first.
    //
    // Note that enabling idempotence requires max.in.flight.requests.per.connection to be less than or equal to 5
    // (with message ordering preserved for any allowable value), retries to be greater than 0, and acks must be
    // 'all'.
    //
    // For short, allow retry and in-flight requests without idempotence could cause re-order which is abosolute not
    // desirable. We have two choices here:
    //
    // * Allow retry but smart in-flight requests with idempotence producing.
    // * Disable retry with possible large in-flight requests.
    //
    // But I fail to configure either way. Let me evaluate it later.
    //
    // .set("retries", "5")
    // .set("enable.idempotence", "true")
    // .set("max.in.flight.requests.per.connection", "5")
    // .set("acks", "all")
    //
    // .set("retries", "0")
    // .set("enable.idempotence", "false")
    // .set("max.in.flight.requests.per.connection", "50")
    // .set("acks", "all")
}

fn resolve_offset(offset: LogOffset) -> Result<Offset> {
    Ok(match offset {
        LogOffset::Position(offset) => {
            Offset::Offset(offset.as_u64().ok_or_else(|| anyhow!("invalid position {:?}", offset))? as i64)
        },
        LogOffset::Earliest => Offset::Beginning,
        LogOffset::Latest => Offset::End,
    })
}

fn into_position(offset: i64) -> LogPosition {
    LogPosition::Offset(offset as u128)
}

#[async_trait]
impl LogClient for KafkaLogClient {
    async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>> {
        let producer = FutureProducer::from_config(&self.config)?;
        Ok(Box::new(KafkaPartitionProducer::new(name.to_string(), producer)))
    }

    async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>> {
        let consumer = StreamConsumer::from_config(&self.config)?;
        let offset = resolve_offset(offset)?;
        let mut topics = TopicPartitionList::new();
        topics.add_partition_offset(name, 0, offset)?;
        consumer.assign(&topics)?;
        Ok(Box::new(KafkaPartitionConsumer::new(name.to_string(), 0, consumer)))
    }

    async fn create_log(&self, name: &str, retention: ByteSize) -> Result<()> {
        let retention_bytes = retention.0.to_compact_string();
        let topics = [new_topic_config(name, self.replication, &retention_bytes)];
        let mut results = self.client.create_topics(&topics, &AdminOptions::default()).await?;
        let topic_result = results.pop().ok_or_else(|| anyhow!("no topic results in topic creation"))?;
        match topic_result {
            Ok(_) => Ok(()),
            Err((topic, error_code)) => Err(anyhow!("fail to create kafka topic {}: {}", topic, error_code)),
        }
    }

    async fn delete_log(&self, name: &str) -> Result<()> {
        let mut results = self.client.delete_topics(&[name], &AdminOptions::default()).await?;
        if let Err((_, error_code)) = results.pop().unwrap() {
            return Err(anyhow!("fail to delete kafka topic {}: {}", name, error_code));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KafkaLogFactory {}

#[async_trait]
impl LogFactory for KafkaLogFactory {
    fn scheme(&self) -> &'static str {
        "kafka"
    }

    async fn open_client(&self, endpoint: &Endpoint, params: &Params) -> Result<Arc<dyn LogClient>> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", endpoint.address());
        config.set("client.id", "seamdb");
        config.set("group.id", "seamdb");
        config.set("acks", "all");
        let client = AdminClient::from_config(&config)?;
        let replication = match params.query("replication") {
            None => 1,
            Some(replication) => replication.parse().map_err(|_| anyhow!("invalid kafka topic replication"))?,
        };
        Ok(Arc::new(KafkaLogClient { config, client, replication }))
    }
}

#[cfg(test)]
mod tests {
    use speculoos::*;
    use testcontainers::clients::Cli as DockerCli;
    use testcontainers::core::{Container, Port, RunnableImage, WaitFor};
    use testcontainers::images::generic::GenericImage;

    use super::*;
    use crate::endpoint::*;

    fn kafka_image() -> RunnableImage<GenericImage> {
        let image = GenericImage::new("confluentinc/confluent-local", "7.4.1")
            .with_wait_for(WaitFor::StdOutMessage { message: "Server started, listening for requests".to_string() });
        // I can't find a way to connect to random mapping port for Kafka.
        //
        // Kafka registers listening port to cluster, and advertise it to client. So client will receive the advertised
        // port for connection. And it is bound in configuration. See also:
        //
        // * https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/
        // * https://stackoverflow.com/questions/59343783/run-kafka-using-docker-compose-and-expose-a-different-port-instead-of-default-on
        RunnableImage::from(image).with_mapped_port(Port { local: 9092, internal: 9092 })
    }

    fn kafka_container() -> Container<'static, GenericImage> {
        let docker = DockerCli::default();
        let kafka = docker.run(kafka_image());
        unsafe { std::mem::transmute(kafka) }
    }

    #[tokio::test]
    #[serial_test::serial("kafka_9092")]
    async fn test_kafka_basic() {
        let kafka = kafka_container();
        let server = format!("kafka://127.0.0.1:{}", kafka.get_host_port_ipv4(9092));

        let uri: ServiceUri = server.parse().unwrap();
        let factory = KafkaLogFactory::default();
        let client = factory.open_client(&uri.endpoint(), uri.params()).await.unwrap();

        let name = "xyz";
        client.create_log(name, ByteSize::mib(50)).await.unwrap();

        let mut producer = client.produce_log(name).await.unwrap();
        let mut subscriber = client.subscribe_log(name, LogOffset::Earliest).await.unwrap();

        let write_position = producer.send(b"a0").await.unwrap();
        let (read_position, read_bytes) = subscriber.read().await.unwrap();
        assert_that!(read_position).is_equal_to(write_position);
        assert_that!(read_bytes).is_equal_to("a0".as_bytes());

        producer.send(b"a1").await.unwrap();
        let write_position = producer.send(b"a2").await.unwrap();
        let latest_position = subscriber.latest().await.unwrap();
        assert_that!(latest_position).is_equal_to(&write_position);

        subscriber.seek(write_position.clone().into()).await.unwrap();
        let (read_position, read_bytes) = subscriber.read().await.unwrap();
        assert_that!(read_position).is_equal_to(write_position);
        assert_that!(read_bytes).is_equal_to("a2".as_bytes());

        client.delete_log(name).await.unwrap();
        client.create_log(name, ByteSize::mib(50)).await.unwrap();
        client.delete_log(name).await.unwrap();
    }
}
