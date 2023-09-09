// Copyright 2023 The SeamDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytesize::ByteSize;
use either::Either;
use hashbrown::hash_map::HashMap;
use tokio::sync::futures::Notified;
use tokio::sync::Notify;

use crate::endpoint::{Endpoint, Params};
use crate::log::{ByteLogProducer, ByteLogSubscriber, LogClient, LogFactory, LogOffset, LogPosition};

#[cfg(test)]
const RETENTION_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const RETENTION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct MemoryLogContent {
    size: usize,
    start: usize,
    earliest: usize,
    retention: ByteSize,
    expiration: Option<Instant>,
    messages: VecDeque<Vec<u8>>,
}

impl MemoryLogContent {
    fn check_retention(&mut self) {
        if self.expiration.filter(|e| *e <= Instant::now()).take().is_some() {
            while self.start < self.earliest {
                self.messages.pop_front();
                self.start += 1;
            }
        }
        while self.size > self.retention.as_u64() as usize {
            self.size -= self.messages[self.earliest].len();
            self.earliest += 1;
        }
        if self.earliest != self.start && self.expiration.is_none() {
            self.expiration = Some(Instant::now() + RETENTION_TIMEOUT);
        }
    }
}

#[derive(Debug)]
struct MemoryLog {
    more: Notify,
    content: Arc<Mutex<MemoryLogContent>>,
}

impl MemoryLog {
    fn offset(&self) -> (usize, usize) {
        let content = self.content.lock().unwrap();
        let end = content.start + content.messages.len();
        let latest = if end > 0 { end - 1 } else { 0 };
        (content.earliest, latest)
    }

    fn read(&self, position: usize) -> Result<Either<Vec<u8>, Notified<'_>>> {
        let content = self.content.lock().unwrap();
        if position < content.start {
            bail!("read position out of range")
        }
        let i = position - content.start;
        if i >= content.messages.len() {
            return Ok(Either::Right(self.more.notified()));
        }
        let data = content.messages[i].clone();
        Ok(Either::Left(data))
    }

    fn append(&self, message: Vec<u8>) -> Result<LogPosition> {
        let mut content = self.content.lock().unwrap();
        let position = content.start + content.messages.len();
        content.size += message.len();
        content.messages.push_back(message);
        content.check_retention();
        drop(content);
        self.more.notify_waiters();
        Ok(LogPosition::Offset(position as u128))
    }
}

#[derive(Debug)]
struct MemoryLogProducer {
    log: Arc<MemoryLog>,
    queue: VecDeque<Vec<u8>>,
}

#[async_trait]
impl ByteLogProducer for MemoryLogProducer {
    fn queue(&mut self, payload: &[u8]) -> Result<()> {
        self.queue.push_back(payload.to_owned());
        Ok(())
    }

    async fn wait(&mut self) -> Result<LogPosition> {
        if let Some(payload) = self.queue.pop_front() {
            self.log.append(payload)
        } else {
            std::future::pending().await
        }
    }
}

#[derive(Debug)]
struct MemoryLogSubscriber {
    log: Arc<MemoryLog>,
    cache: Vec<u8>,
    offset: usize,
}

#[async_trait]
impl ByteLogSubscriber for MemoryLogSubscriber {
    async fn read(&mut self) -> Result<(LogPosition, &[u8])> {
        loop {
            match self.log.read(self.offset)? {
                Either::Left(data) => {
                    let position = LogPosition::Offset(self.offset as u128);
                    self.offset += 1;
                    self.cache = data;
                    return Ok((position, self.cache.as_slice()));
                },
                Either::Right(notified) => notified.await,
            }
        }
    }

    async fn seek(&mut self, offset: LogOffset) -> Result<()> {
        let (earliest, latest) = self.log.offset();
        match offset {
            LogOffset::Earliest => self.offset = earliest,
            LogOffset::Latest => self.offset = latest,
            LogOffset::Position(position) => {
                let position = position.as_u64().unwrap() as usize;
                if position < earliest || position > latest {
                    bail!("position out of range")
                }
                self.offset = position;
            },
        }
        Ok(())
    }

    async fn latest(&self) -> Result<LogPosition> {
        let (_earliest, latest) = self.log.offset();
        Ok(LogPosition::Offset(latest as u128))
    }
}

#[derive(Debug, Default)]
struct MemoryLogClient {
    logs: Arc<Mutex<HashMap<String, Arc<MemoryLog>>>>,
}

impl MemoryLogClient {
    fn get_log(&self, name: &str) -> Result<Arc<MemoryLog>> {
        let logs = self.logs.lock().unwrap();
        logs.get(name).cloned().ok_or_else(|| anyhow!("no log named {}", name))
    }
}

#[async_trait]
impl LogClient for MemoryLogClient {
    async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>> {
        let log = self.get_log(name)?;
        let producer = MemoryLogProducer { log, queue: Default::default() };
        Ok(Box::new(producer))
    }

    async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>> {
        let log = self.get_log(name)?;
        let mut subscriber = MemoryLogSubscriber { log, cache: vec![], offset: 0 };
        subscriber.seek(offset).await?;
        Ok(Box::new(subscriber))
    }

    async fn create_log(&self, name: &str, retention: ByteSize) -> Result<()> {
        let mut logs = self.logs.lock().unwrap();
        if logs.contains_key(name) {
            bail!("log {} already exists", name)
        }
        let log = Arc::new(MemoryLog {
            more: Notify::new(),
            content: Arc::new(Mutex::new(MemoryLogContent {
                retention: if retention == ByteSize(0) { ByteSize(u64::MAX) } else { retention },
                ..Default::default()
            })),
        });
        logs.insert(name.to_string(), log);
        Ok(())
    }

    async fn delete_log(&self, name: &str) -> Result<()> {
        let mut logs = self.logs.lock().unwrap();
        logs.remove(name);
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemoryLogFactory {
    client: Arc<dyn LogClient>,
}

impl MemoryLogFactory {
    pub const ENDPOINT: Endpoint<'static> = unsafe { Endpoint::new_unchecked("memory", "memory") };

    pub fn new() -> Self {
        Self { client: Arc::new(MemoryLogClient::default()) }
    }
}

impl Default for MemoryLogFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogFactory for MemoryLogFactory {
    fn scheme(&self) -> &'static str {
        "memory"
    }

    async fn open_client(&self, endpoint: &Endpoint, _params: &Params) -> Result<Arc<dyn LogClient>> {
        assert_eq!(endpoint.scheme(), "memory", "invalid scheme: expect \"memory\", got \"{}\"", endpoint.scheme());
        assert_eq!(endpoint.address(), "memory", "invalid address: expect \"memory\", got \"{}\"", endpoint.address());
        Ok(self.client.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assertor::*;
    use bytesize::ByteSize;
    use tokio::select;

    use super::RETENTION_TIMEOUT;
    use crate::endpoint::{Endpoint, Params};
    use crate::log::{LogFactory, LogOffset, LogPosition, MemoryLogFactory};

    #[test]
    fn test_memory_log_scheme() {
        let factory = MemoryLogFactory::new();
        assert_that!(factory.scheme()).is_equal_to("memory");
    }

    #[tokio::test]
    #[should_panic(expected = "invalid scheme")]
    async fn test_memory_log_open_client_invalid_scheme() {
        let factory = MemoryLogFactory::new();
        factory.open_client(&Endpoint::try_from("test://memory").unwrap(), &Params::default()).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "invalid address")]
    async fn test_memory_log_open_client_invalid_address() {
        let factory = MemoryLogFactory::new();
        factory.open_client(&Endpoint::try_from("memory://test").unwrap(), &Params::default()).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_log_open_client_same() {
        let factory = MemoryLogFactory::new();
        let client1 = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let client2 = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        assert!(Arc::ptr_eq(&client1, &client2));
    }

    #[tokio::test]
    #[should_panic(expected = "log test1 already exists")]
    async fn test_memory_log_create_log_alreay_exists() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "no log named test1")]
    async fn test_memory_log_delete_log() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
        client.delete_log("test1").await.unwrap();
        client.delete_log("test1").await.unwrap();
        client.produce_log("test1").await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "no log named test1")]
    async fn test_memory_log_produce_no_log() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.produce_log("test1").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_log_produce() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
        let mut producer = client.produce_log("test1").await.unwrap();
        let position1 = producer.send(b"message1").await.unwrap();
        let position2 = producer.send(b"message2").await.unwrap();
        assert_that!(position1).is_equal_to(LogPosition::Offset(0));
        assert_that!(position2).is_equal_to(LogPosition::Offset(1));
    }

    #[tokio::test]
    async fn test_memory_log_produce_interleaving() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
        let mut producer_a = client.produce_log("test1").await.unwrap();
        let mut producer_b = client.produce_log("test1").await.unwrap();
        let position_a1 = producer_a.send(b"message_a1").await.unwrap();
        let position_b1 = producer_b.send(b"message_b1").await.unwrap();
        let position_a2 = producer_a.send(b"message_a2").await.unwrap();
        let position_b2 = producer_b.send(b"message_b2").await.unwrap();
        assert_that!(position_a1).is_equal_to(LogPosition::Offset(0));
        assert_that!(position_b1).is_equal_to(LogPosition::Offset(1));
        assert_that!(position_a2).is_equal_to(LogPosition::Offset(2));
        assert_that!(position_b2).is_equal_to(LogPosition::Offset(3));
    }

    #[tokio::test]
    async fn test_memory_log_produce_queue() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
        let mut producer = client.produce_log("test1").await.unwrap();

        producer.queue(b"message1").unwrap();
        producer.queue(b"message2").unwrap();

        let position1 = producer.wait().await.unwrap();
        assert_that!(position1).is_equal_to(LogPosition::Offset(0));

        let position2 = producer.wait().await.unwrap();
        assert_that!(position2).is_equal_to(LogPosition::Offset(1));

        select! {
            _ = producer.wait() => panic!("unexpect producer wait return"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {},
        }
    }

    #[tokio::test]
    #[should_panic(expected = "no log named test1")]
    async fn test_memory_log_subscribe_no_log() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.subscribe_log("test1", LogOffset::Earliest).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_log_subscribe() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();
        let mut producer = client.produce_log("test1").await.unwrap();
        producer.send(b"message1").await.unwrap();
        producer.send(b"message2").await.unwrap();
        producer.send(b"message3").await.unwrap();

        let mut subscriber1 = client.subscribe_log("test1", LogOffset::Earliest).await.unwrap();
        let mut subscriber2 = client.subscribe_log("test1", LogOffset::Latest).await.unwrap();
        let mut subscriber3 = client.subscribe_log("test1", LogOffset::Latest).await.unwrap();

        assert_that!(subscriber1.latest().await.unwrap()).is_equal_to(LogPosition::Offset(2));
        assert_that!(subscriber2.latest().await.unwrap()).is_equal_to(LogPosition::Offset(2));
        assert_that!(subscriber3.latest().await.unwrap()).is_equal_to(LogPosition::Offset(2));

        subscriber3.seek(LogOffset::Position(LogPosition::Offset(1))).await.unwrap();

        assert_eq!(subscriber1.read().await.unwrap(), (LogPosition::Offset(0), b"message1".as_slice()));
        assert_eq!(subscriber1.read().await.unwrap(), (LogPosition::Offset(1), b"message2".as_slice()));
        assert_eq!(subscriber1.read().await.unwrap(), (LogPosition::Offset(2), b"message3".as_slice()));

        assert_eq!(subscriber2.read().await.unwrap(), (LogPosition::Offset(2), b"message3".as_slice()));

        assert_eq!(subscriber3.read().await.unwrap(), (LogPosition::Offset(1), b"message2".as_slice()));
        assert_eq!(subscriber3.read().await.unwrap(), (LogPosition::Offset(2), b"message3".as_slice()));

        producer.send(b"message4").await.unwrap();
        assert_eq!(subscriber1.read().await.unwrap(), (LogPosition::Offset(3), b"message4".as_slice()));
        assert_eq!(subscriber2.read().await.unwrap(), (LogPosition::Offset(3), b"message4".as_slice()));
        assert_eq!(subscriber3.read().await.unwrap(), (LogPosition::Offset(3), b"message4".as_slice()));
    }

    #[tokio::test]
    async fn test_memory_log_subscribe_wait() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", Default::default()).await.unwrap();

        let mut subscriber1 = client.subscribe_log("test1", LogOffset::Earliest).await.unwrap();
        let mut subscriber2 = client.subscribe_log("test1", LogOffset::Latest).await.unwrap();
        let mut subscriber3 = client.subscribe_log("test1", LogOffset::Position(LogPosition::Offset(0))).await.unwrap();

        let mut producer = client.produce_log("test1").await.unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            producer.send(b"message1").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            producer.send(b"message2").await.unwrap();
        });
        assert_eq!(subscriber1.read().await.unwrap(), (LogPosition::Offset(0), b"message1".as_slice()));
        assert_eq!(subscriber2.read().await.unwrap(), (LogPosition::Offset(0), b"message1".as_slice()));
        assert_eq!(subscriber3.read().await.unwrap(), (LogPosition::Offset(0), b"message1".as_slice()));
        assert_eq!(subscriber1.read().await.unwrap(), (LogPosition::Offset(1), b"message2".as_slice()));
        assert_eq!(subscriber2.read().await.unwrap(), (LogPosition::Offset(1), b"message2".as_slice()));
        assert_eq!(subscriber3.read().await.unwrap(), (LogPosition::Offset(1), b"message2".as_slice()));
    }

    #[tokio::test]
    async fn test_memory_log_retention() {
        let factory = MemoryLogFactory::new();
        let client = factory.open_client(&MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        client.create_log("test1", ByteSize::kb(1)).await.unwrap();

        let mut producer = client.produce_log("test1").await.unwrap();
        producer.send(&vec![0; 501]).await.unwrap();
        producer.send(&vec![0; 501]).await.unwrap();

        tokio::time::sleep(RETENTION_TIMEOUT + Duration::from_secs(1)).await;
        producer.send(&vec![0; 400]).await.unwrap();

        let mut subscriber = client.subscribe_log("test1", LogOffset::Earliest).await.unwrap();
        let (position, _) = subscriber.read().await.unwrap();
        assert_that!(position).is_equal_to(LogPosition::Offset(1));
    }
}
