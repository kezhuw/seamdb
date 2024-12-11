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

use std::convert::AsMut;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use derive_where::derive_where;
use tokio::select;
use tracing::trace;

use super::store::{BatchContext, BatchResult, TabletStore, TxnTabletStore};
use super::types::{MessageId, TabletWatermark};
use crate::clock::{Clock, Timestamp};
use crate::log::{ByteLogProducer, ByteLogSubscriber, LogAddress, LogManager, LogOffset, LogPosition};
use crate::protos::{
    BatchError,
    BatchRequest,
    BatchResponse,
    DataMessage,
    ManifestMessage,
    ShardDescription,
    ShardId,
    ShardRequest,
    ShardResponse,
    TabletDeployment,
    TabletDescription,
    TabletId,
    TabletManifest,
    Temporal,
};
use crate::tablet::{Request, TabletClient};

pub trait LogMessage: prost::Message + Sync + Default + 'static {
    fn epoch(&self) -> u64;
    fn sequence(&self) -> u64;

    fn id(&self) -> MessageId {
        MessageId { epoch: self.epoch(), sequence: self.sequence() }
    }
}

impl LogMessage for ManifestMessage {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl LogMessage for DataMessage {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[async_trait]
pub trait LogMessageConsumer<T>: ByteLogSubscriber {
    async fn read_message(&mut self) -> Result<(LogPosition, T)>;
}

#[async_trait]
pub trait LogMessageProducer<T: Sync>: ByteLogProducer {
    fn queue_message(&mut self, message: &T) -> Result<()>;

    async fn send_message(&mut self, message: &T) -> Result<LogPosition> {
        self.queue_message(message)?;
        self.wait().await
    }
}

#[allow(clippy::needless_lifetimes)]
#[derive_where(Debug)]
pub struct TypedLogConsumer<'a, T> {
    consumer: &'a mut dyn ByteLogSubscriber,
    #[derive_where(skip(Debug))]
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T> TypedLogConsumer<'a, T> {
    pub fn new(consumer: &'a mut dyn ByteLogSubscriber) -> Self {
        Self { consumer, _marker: std::marker::PhantomData }
    }
}

#[async_trait]
impl<T: Send + Sync> ByteLogSubscriber for TypedLogConsumer<'_, T> {
    async fn read<'a>(&'a mut self) -> Result<(LogPosition, &'a [u8])> {
        self.consumer.read().await
    }

    async fn seek(&mut self, offset: LogOffset) -> Result<()> {
        self.consumer.seek(offset).await
    }

    async fn latest(&self) -> Result<LogPosition> {
        self.consumer.latest().await
    }
}

#[async_trait]
impl<T: LogMessage> LogMessageConsumer<T> for TypedLogConsumer<'_, T> {
    async fn read_message(&mut self) -> Result<(LogPosition, T)> {
        let (offset, payload) = self.consumer.read().await?;
        let message = T::decode(payload)?;
        Ok((offset, message))
    }
}

struct LimitedLogConsumer<'a, T> {
    last: LogPosition,
    limit: LogPosition,
    consumer: &'a mut dyn LogMessageConsumer<T>,
}

impl<'a, T> LimitedLogConsumer<'a, T> {
    fn new(consumer: &'a mut dyn LogMessageConsumer<T>, limit: LogPosition) -> Self {
        Self { last: LogPosition::Offset(0), limit, consumer }
    }

    async fn read(&mut self) -> Result<Option<T>> {
        if self.last >= self.limit {
            return Ok(None);
        }
        let (position, message) = self.consumer.read_message().await?;
        self.last = position;
        if self.last > self.limit {
            Ok(None)
        } else {
            Ok(Some(message))
        }
    }
}

#[derive_where(Debug)]
pub struct MessageConsumer<T> {
    last_recv: LogPosition,
    last_epoch: u64,
    last_sequence: u64,
    consumer: Box<dyn ByteLogSubscriber>,
    #[derive_where(skip(Debug))]
    _marker: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T: LogMessage> ByteLogSubscriber for MessageConsumer<T> {
    async fn read<'a>(&'a mut self) -> Result<(LogPosition, &'a [u8])> {
        let (offset, _, payload) = (self as &mut MessageConsumer<T>).read().await?;
        Ok((offset, payload))
    }

    async fn seek(&mut self, offset: LogOffset) -> Result<()> {
        self.consumer.seek(offset).await
    }

    async fn latest(&self) -> Result<LogPosition> {
        self.consumer.latest().await
    }
}

#[async_trait]
impl<T: LogMessage> LogMessageConsumer<T> for MessageConsumer<T> {
    async fn read_message(&mut self) -> Result<(LogPosition, T)> {
        let (position, message, _) = (self as &mut MessageConsumer<T>).read().await?;
        Ok((position, message))
    }
}

#[derive_where(Debug)]
pub struct BufMessageProducer<T> {
    buf: Vec<u8>,
    producer: Box<dyn ByteLogProducer>,
    #[derive_where(skip(Debug))]
    _marker: std::marker::PhantomData<T>,
}

impl<T> BufMessageProducer<T> {
    pub fn new(producer: Box<dyn ByteLogProducer>) -> Self {
        Self { buf: Vec::with_capacity(4096), producer, _marker: std::marker::PhantomData }
    }
}

#[async_trait]
impl<T: Send + 'static> ByteLogProducer for BufMessageProducer<T> {
    fn queue(&mut self, payload: &[u8]) -> Result<()> {
        self.producer.queue(payload)
    }

    async fn wait(&mut self) -> Result<LogPosition> {
        self.producer.wait().await
    }
}

impl<T: LogMessage> LogMessageProducer<T> for BufMessageProducer<T> {
    fn queue_message(&mut self, message: &T) -> Result<()> {
        self.buf.clear();
        message.encode(&mut self.buf)?;
        self.producer.queue(&self.buf)
    }
}

impl<T: LogMessage> MessageConsumer<T> {
    fn into_producer(self, producer: BufMessageProducer<T>) -> MessageProducer<T> {
        if producer.exclusive() {
            MessageProducer {
                epoch: self.last_epoch,
                last_sent: self.last_recv,
                last_sequence: self.last_sequence,
                pending: 0,
                producer,
                consumer: None,
            }
        } else {
            MessageProducer {
                epoch: self.last_epoch,
                last_sent: self.last_recv,
                last_sequence: self.last_sequence,
                pending: 0,
                producer,
                consumer: Some(self.consumer),
            }
        }
    }

    async fn read(&mut self) -> Result<(LogPosition, T, &[u8])> {
        loop {
            let (position, payload) = self.consumer.read().await?;
            let message = T::decode(payload)?;
            let (epoch, sequence) = (message.epoch(), message.sequence());
            if (epoch, sequence) <= (self.last_epoch, self.last_sequence) {
                continue;
            } else if epoch == self.last_epoch && sequence != self.last_sequence + 1 {
                bail!("consumer({}:{}) got message at {}:{}", self.last_epoch, self.last_sequence, epoch, sequence);
            }
            self.last_recv = position.clone();
            self.last_epoch = epoch;
            self.last_sequence = sequence;
            return Ok((position, message, unsafe { std::mem::transmute(payload) }));
        }
    }
}

pub struct MessageProducer<T> {
    epoch: u64,
    last_sent: LogPosition,
    last_sequence: u64,
    pending: u64,
    producer: BufMessageProducer<T>,
    // Detect fence if producer is not exclusive.
    consumer: Option<Box<dyn ByteLogSubscriber>>,
}

impl<T: LogMessage> MessageProducer<T> {
    pub fn queue(&mut self, message: &T) -> Result<()> {
        let (epoch, sequence) = (message.epoch(), message.sequence());
        if epoch != self.epoch || sequence != self.last_sequence + 1 + self.pending {
            bail!("producer@{} got message {}:{}", self.epoch, epoch, sequence);
        }
        self.producer.queue_message(message)?;
        self.pending += 1;
        Ok(())
    }

    async fn wait_internally(&mut self) -> Result<LogPosition> {
        let Some(consumer) = &mut self.consumer else {
            return self.producer.wait().await;
        };
        #[allow(clippy::if_same_then_else)]
        loop {
            select! {
                Ok((position, payload)) = consumer.read() => {
                    let message = T::decode(payload)?;
                    let (epoch, sequence) = (message.epoch(), message.sequence());
                    #[allow(clippy::if_same_then_else)]
                    if epoch > self.epoch {
                        bail!("epoch {} producer fenced by {}", self.epoch, message.epoch());
                    } else if epoch < self.epoch {
                        // It is possible that old producer succeed to persist message after
                        // being fenced due to delayed fence detection.
                        continue;
                    } else if sequence <= self.last_sequence {
                        // last_sequence could advance due to producer response.
                        continue;
                    } else if sequence != self.last_sequence + 1 {
                        bail!("producer({}:{}) got next sequence {} at {}", self.last_sent, self.last_sequence, sequence, position);
                    }
                    return Ok(position);
                },
                result = self.producer.wait() => {
                    let position = result?;
                    if position.is_next_of(&self.last_sent) {
                        return Ok(position);
                    }
                    trace!("interleaving sending detected: last sent position {}, but got next {}", self.last_sent, position);
                },
            }
        }
    }

    pub async fn wait(&mut self) -> Result<LogPosition> {
        let position = self.wait_internally().await?;
        if self.pending == 0 {
            return Err(anyhow!("expect no send, but got sent position {}", position));
        }
        self.last_sent = position.clone();
        self.last_sequence += 1;
        self.pending -= 1;
        Ok(position)
    }

    pub async fn send(&mut self, message: &T) -> Result<LogPosition> {
        if self.pending != 0 {
            bail!("no empty pending messages");
        }
        self.queue(message)?;
        self.wait().await
    }
}

pub struct FollowingTabletStore {
    pub cursor: MessageId,
    pub store: TabletStore,
    pub consumer: MessageConsumer<DataMessage>,
}

pub struct LeadingTabletStore {
    pub cursor: MessageId,
    pub store: TxnTabletStore,
    pub producer: MessageProducer<DataMessage>,
}

impl LeadingTabletStore {
    pub fn new_message(&mut self) -> DataMessage {
        self.store.step();
        self.cursor.sequence += 1;
        DataMessage {
            epoch: self.cursor.epoch,
            sequence: self.cursor.sequence,
            temporal: Temporal::default(),
            closed_timestamp: None,
            leader_expiration: None,
            operation: None,
        }
    }
}

pub struct LeadingTabletManifest {
    pub cursor: MessageId,
    pub manifest: TabletManifest,
    pub producer: MessageProducer<ManifestMessage>,
}

impl LeadingTabletManifest {
    pub async fn publish(&mut self) -> Result<LogPosition> {
        let message = self.new_message();
        self.producer.send(&message).await
    }

    pub fn new_message(&mut self) -> ManifestMessage {
        self.cursor.sequence += 1;
        ManifestMessage {
            epoch: self.cursor.epoch,
            sequence: self.cursor.sequence,
            manifest: Some(self.manifest.clone()),
        }
    }
}

pub struct FollowingTabletManifest {
    pub cursor: MessageId,
    pub manifest: TabletManifest,
    pub consumer: MessageConsumer<ManifestMessage>,
}

pub struct LeadingTablet {
    pub manifest: LeadingTabletManifest,
    pub store: LeadingTabletStore,
}

impl LeadingTablet {
    pub fn id(&self) -> TabletId {
        self.manifest.manifest.tablet.id.into()
    }

    pub fn shards(&self) -> &[ShardDescription] {
        self.store.store.shards()
    }

    pub fn deployment_shard_id(&self) -> Option<ShardId> {
        if self.manifest.manifest.tablet.id == TabletId::ROOT.into_raw() {
            return Some(ShardId::DEPLOYMENT);
        }
        None
    }

    pub fn new_data_message(&mut self) -> DataMessage {
        DataMessage {
            closed_timestamp: Some(self.closed_timestamp()),
            leader_expiration: Some(self.leader_expiration()),
            ..self.store.new_message()
        }
    }

    pub fn new_manifest_message(&mut self) -> ManifestMessage {
        self.manifest.new_message()
    }

    pub fn closed_timestamp(&self) -> Timestamp {
        self.manifest.manifest.watermark.closed_timestamp
    }

    pub fn leader_expiration(&self) -> Timestamp {
        self.manifest.manifest.watermark.leader_expiration
    }

    pub fn update_closed_timestamp(&mut self, closed_timestamp: Timestamp) {
        if closed_timestamp > self.leader_expiration() {
            panic!("claim closed timestamp {} > leader expiration {}", closed_timestamp, self.leader_expiration())
        }
        if closed_timestamp > self.manifest.manifest.watermark.closed_timestamp {
            self.manifest.manifest.watermark.closed_timestamp = closed_timestamp;
            self.store.store.close_ts(closed_timestamp);
        }
    }

    pub fn update_manifest(&mut self, mut manifest: TabletManifest) {
        manifest.watermark.closed_timestamp = self.closed_timestamp().max(manifest.watermark.closed_timestamp);
        self.manifest.manifest = manifest;
    }

    pub async fn rotate(&mut self, closing_timestamp: Timestamp, leader_expiration: Timestamp) -> Result<()> {
        self.manifest.manifest.rotate();
        self.update_watermark(closing_timestamp, leader_expiration);
        self.manifest.publish().await?;
        Ok(())
    }

    pub fn update_watermark(&mut self, closing_timestamp: Timestamp, leader_expiration: Timestamp) {
        let closed_timestamp = self.store.store.close_ts(closing_timestamp);
        let watermark = TabletWatermark::new(self.store.cursor, closed_timestamp, leader_expiration);
        self.manifest.manifest.update_watermark(watermark);
    }

    pub fn process_request(&mut self, request: Request) -> Result<Option<BatchResult>> {
        if !request.is_readonly() {
            let ts = request.temporal.timestamp();
            let watermark = &self.manifest.manifest.watermark;
            if ts > watermark.leader_expiration {
                return Ok(Some(BatchResult::Error {
                    error: BatchError::with_message("write above leader expiration"),
                    responser: request.responser,
                }));
            }
        }
        self.store.store.process_request(request)
    }
}

pub struct FollowingTablet {
    pub manifest: FollowingTabletManifest,
    pub store: FollowingTabletStore,
}

impl FollowingTablet {
    pub fn id(&self) -> TabletId {
        self.manifest.manifest.tablet.id.into()
    }

    pub fn shards(&self) -> &[ShardDescription] {
        self.store.store.shards()
    }

    pub fn apply_manifest_message(&mut self, message: ManifestMessage) -> Result<()> {
        self.manifest.cursor = MessageId::new(message.epoch, message.sequence);
        self.manifest.manifest.update(message.manifest);
        self.store.store.update_watermark(self.manifest.manifest.watermark);
        Ok(())
    }

    pub fn closed_timestamp(&self) -> Timestamp {
        let cursor = self.store.store.watermark();
        cursor.closed_timestamp
    }

    fn extract_read_timestamp(&self, batch: &BatchRequest) -> Option<Timestamp> {
        if !batch.is_readonly() {
            return None;
        }
        match batch.temporal {
            Temporal::Timestamp(timestamp) => {
                let closed_timestamp = self.closed_timestamp();
                if timestamp > closed_timestamp {
                    return None;
                }
                Some(timestamp)
            },
            _ => None,
        }
    }

    fn query(&mut self, ts: Timestamp, requests: Vec<ShardRequest>) -> Result<Vec<ShardResponse>, BatchError> {
        let mut context = BatchContext::default();
        self.store.store.batch_timestamped(&mut context, ts, requests)
    }

    pub fn query_batch(
        &mut self,
        deployment: &TabletDeployment,
        batch: BatchRequest,
    ) -> Result<BatchResponse, BatchError> {
        let mut response = BatchResponse::default();
        if let Some(ts) = self.extract_read_timestamp(&batch) {
            response.responses = self.query(ts, batch.requests)?;
        } else {
            response.deployments.push(deployment.clone());
        }
        Ok(response)
    }
}

#[derive(Clone)]
pub struct TabletLoader {
    log: Arc<LogManager>,
}

impl TabletLoader {
    pub fn new(log: Arc<LogManager>) -> Self {
        Self { log }
    }

    async fn read_manifest(
        &self,
        cursor: &mut MessageId,
        manifest: &mut TabletManifest,
        consumer: &mut dyn LogMessageConsumer<ManifestMessage>,
        limit: Option<LogPosition>,
    ) -> Result<()> {
        let limit = match limit {
            None => consumer.latest().await?,
            Some(limit) => limit,
        };
        let mut subscriber = LimitedLogConsumer::new(consumer, limit);
        while let Some(message) = subscriber.read().await? {
            if cursor.advance(MessageId::new(message.epoch, message.sequence))? {
                manifest.update(message.manifest);
            }
        }
        Ok(())
    }

    async fn load_manifest(&self, uri: &LogAddress<'_>, limit: Option<LogPosition>) -> Result<FollowingTabletManifest> {
        let mut consumer = self.log.subscribe_log(uri, LogOffset::Earliest).await?;
        let limit = match limit {
            None => consumer.latest().await?,
            Some(limit) => limit,
        };
        let mut typed_consumer: TypedLogConsumer<ManifestMessage> = TypedLogConsumer::new(consumer.as_mut());
        let mut subscriber = LimitedLogConsumer::new(&mut typed_consumer, limit.clone());
        let (mut manifest, mut cursor) = loop {
            match subscriber.read().await {
                Ok(Some(message)) => match (message.id(), message.manifest) {
                    (cursor, Some(manifest)) => break (manifest, cursor),
                    (_, None) => continue,
                },
                Ok(None) => bail!("no manifest message"),
                Err(err) => return Err(err),
            }
        };
        while let Some(message) = subscriber.read().await? {
            if cursor.advance(message.id())? {
                manifest.update(message.manifest);
            }
        }
        let consumer = MessageConsumer {
            last_epoch: cursor.epoch,
            last_sequence: cursor.sequence,
            last_recv: limit,
            consumer,
            _marker: std::marker::PhantomData,
        };
        Ok(FollowingTabletManifest { cursor, manifest, consumer })
    }

    async fn fence_load_manifest(&self, epoch: u64, uri: &LogAddress<'_>) -> Result<LeadingTabletManifest> {
        let mut producer = BufMessageProducer::new(self.log.produce_log(uri).await?);
        let offset = producer.send_message(&ManifestMessage::new_fenced(epoch)).await?;
        let manifest = self.load_manifest(uri, Some(offset)).await?;
        if !(manifest.cursor.epoch == epoch && manifest.cursor.sequence == 0) {
            return Err(anyhow!("fence@{} load stop at {}:{}", epoch, manifest.cursor.epoch, manifest.cursor.sequence));
        }
        Ok(LeadingTabletManifest {
            cursor: manifest.cursor,
            manifest: manifest.manifest,
            producer: manifest.consumer.into_producer(producer),
        })
    }

    pub async fn fence_load_tablet(
        &self,
        clock: Clock,
        client: TabletClient,
        epoch: u64,
        uri: impl TryInto<LogAddress<'_>, Error = anyhow::Error>,
    ) -> Result<LeadingTablet> {
        let uri = uri.try_into()?;
        let manifest = self.fence_load_manifest(epoch, &uri).await?;
        let mut store = self.fence_load_store(clock, client, epoch, &manifest.manifest.tablet).await?;
        store.store.update_watermark(manifest.manifest.watermark);
        Ok(LeadingTablet { manifest, store })
    }

    pub async fn load_tablet(
        &self,
        uri: impl TryInto<LogAddress<'_>, Error = anyhow::Error>,
    ) -> Result<FollowingTablet> {
        let uri = uri.try_into()?;
        let manifest = self.load_manifest(&uri, None).await?;
        let mut store = self.load_store(&manifest.manifest.tablet, None).await?;
        store.store.update_watermark(manifest.manifest.watermark);
        Ok(FollowingTablet { manifest, store })
    }

    async fn lead_manifest(
        &self,
        epoch: u64,
        uri: &LogAddress<'_>,
        mut manifest: FollowingTabletManifest,
    ) -> Result<LeadingTabletManifest> {
        let mut producer = BufMessageProducer::new(self.log.produce_log(uri).await?);
        let position = producer.send_message(&ManifestMessage::new_fenced(epoch)).await?;
        self.read_manifest(&mut manifest.cursor, &mut manifest.manifest, &mut manifest.consumer, Some(position))
            .await?;
        Ok(LeadingTabletManifest {
            cursor: manifest.cursor,
            manifest: manifest.manifest,
            producer: manifest.consumer.into_producer(producer),
        })
    }

    pub async fn lead_tablet(
        &self,
        clock: Clock,
        client: TabletClient,
        epoch: u64,
        uri: impl TryInto<LogAddress<'_>, Error = anyhow::Error>,
        tablet: FollowingTablet,
    ) -> Result<LeadingTablet> {
        assert!(epoch > tablet.manifest.cursor.epoch);
        assert!(epoch > tablet.store.cursor.epoch);
        let uri = uri.try_into()?;
        let manifest = self.lead_manifest(epoch, &uri, tablet.manifest).await?;
        let store_uri = manifest.manifest.tablet.data_log.as_str().try_into()?;
        let store = self.lead_store(clock, client, epoch, &store_uri, tablet.store).await?;
        Ok(LeadingTablet { manifest, store })
    }

    async fn read_store(
        &self,
        cursor: &mut MessageId,
        store: &mut TabletStore,
        consumer: &mut dyn LogMessageConsumer<DataMessage>,
        limit: Option<LogPosition>,
    ) -> Result<()> {
        let limit = match limit {
            None => consumer.latest().await?,
            Some(limit) => limit,
        };
        let mut subscriber = LimitedLogConsumer::new(consumer, limit);
        while let Some(message) = subscriber.read().await? {
            if cursor.advance(MessageId::new(message.epoch, message.sequence))? {
                store.apply(message)?;
            }
        }
        Ok(())
    }

    async fn load_store(&self, tablet: &TabletDescription, limit: Option<LogPosition>) -> Result<FollowingTabletStore> {
        if tablet.shards.iter().any(|r| !r.segments.is_empty()) {
            return Err(anyhow!("do not support file compaction and transaction rotation for now"));
        }
        let uri = tablet.data_log.as_str().try_into()?;
        trace!("subscribing log {uri}");
        let mut consumer: Box<dyn ByteLogSubscriber> = self.log.subscribe_log(&uri, LogOffset::Earliest).await?;
        let limit = match limit {
            None => consumer.latest().await?,
            Some(limit) => limit,
        };
        let mut typed_consumer: TypedLogConsumer<DataMessage> = TypedLogConsumer::new(consumer.as_mut());
        let mut subscriber = LimitedLogConsumer::new(&mut typed_consumer, limit.clone());
        let mut cursor = MessageId::default();
        let mut store = TabletStore::new(tablet.id.into(), &tablet.shards);
        while let Some(message) = subscriber.read().await? {
            if cursor.advance(MessageId::new(message.epoch, message.sequence))? {
                store.apply(message)?;
            }
        }
        let consumer = MessageConsumer {
            last_epoch: cursor.epoch,
            last_sequence: cursor.sequence,
            last_recv: limit,
            consumer,
            _marker: std::marker::PhantomData,
        };
        Ok(FollowingTabletStore { cursor, store, consumer })
    }

    async fn lead_store(
        &self,
        clock: Clock,
        client: TabletClient,
        epoch: u64,
        uri: &LogAddress<'_>,
        mut store: FollowingTabletStore,
    ) -> Result<LeadingTabletStore> {
        let mut producer = BufMessageProducer::new(self.log.produce_log(uri).await?);
        let position = producer.send_message(&DataMessage::new_fenced(epoch)).await?;
        self.read_store(&mut store.cursor, &mut store.store, &mut store.consumer, Some(position)).await?;
        Ok(LeadingTabletStore {
            cursor: store.cursor,
            store: TxnTabletStore::new(store.store, clock, client),
            producer: store.consumer.into_producer(producer),
        })
    }

    async fn fence_load_store(
        &self,
        clock: Clock,
        client: TabletClient,
        epoch: u64,
        tablet: &TabletDescription,
    ) -> Result<LeadingTabletStore> {
        let uri = tablet.data_log.as_str().try_into()?;
        let mut producer = BufMessageProducer::new(self.log.produce_log(&uri).await?);
        let position = producer.send_message(&DataMessage::new_fenced(epoch)).await?;
        let store = self.load_store(tablet, Some(position)).await?;
        if store.cursor != MessageId::new_fenced(epoch) {
            bail!("fence@{} load store stop at {:?}", epoch, store.cursor)
        }
        Ok(LeadingTabletStore {
            cursor: store.cursor,
            store: TxnTabletStore::new(store.store, clock, client),
            producer: store.consumer.into_producer(producer),
        })
    }
}
