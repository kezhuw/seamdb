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

use std::borrow::Cow;
use std::sync::Arc;

use lazy_init::Lazy;
use thiserror::Error;

use crate::cluster::NodeId;
use crate::protos::{ShardId, TabletId, Temporal, Timestamp, TimestampedKeyValue, Uuid, Value};
use crate::tablet::{TabletClient, TabletClientError};

type Result<T, E = KvError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("cluster not ready")]
    ClusterNotReady,
    #[error("cluster not deployed")]
    ClusterNotDeployed,
    #[error("tablet {id} deployment not found")]
    DeploymentNotFound { id: TabletId },
    #[error("tablet {id} not deployed")]
    TabletNotDeployed { id: TabletId },
    #[error("node {node} not available")]
    NodeNotAvailable { node: NodeId },
    #[error("node {node} not connectable: {message}")]
    NodeNotConnectable { node: NodeId, message: String },
    #[error("{status}")]
    GrpcError { status: tonic::Status },
    #[error("unexpected: {message}")]
    UnexpectedError { message: String },
    #[error("tablet {tablet_id} shard {shard_id} contains not shard for {key:?}")]
    ShardNotFound { tablet_id: TabletId, shard_id: ShardId, key: Vec<u8> },
    #[error("data corruption: {message}")]
    DataCorruption { message: String },
    #[error("key {key:?} already exist")]
    KeyAlreadyExists { key: Vec<u8> },
    #[error("key {key:?} get overwritten at {actual}")]
    KeyTimestampMismatch { key: Vec<u8>, actual: Timestamp },
    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },
    #[error("txn {txn_id} restarted from epoch {from_epoch} to {to_epoch}")]
    TxnRestarted { txn_id: Uuid, from_epoch: u32, to_epoch: u32 },
    #[error("txn {txn_id} aborted in epoch {epoch}")]
    TxnAborted { txn_id: Uuid, epoch: u32 },
    #[error("txn {txn_id} already committed with epoch {epoch}")]
    TxnCommitted { txn_id: Uuid, epoch: u32 },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl KvError {
    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::UnexpectedError { message: message.into() }
    }

    pub fn corrupted(message: impl Into<String>) -> Self {
        Self::DataCorruption { message: message.into() }
    }

    pub fn node_not_available(node: impl Into<NodeId>) -> Self {
        Self::NodeNotAvailable { node: node.into() }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument { message: message.into() }
    }
}

impl From<TabletClientError> for KvError {
    fn from(err: TabletClientError) -> Self {
        match err {
            TabletClientError::ClusterNotReady => Self::ClusterNotReady,
            TabletClientError::ClusterNotDeployed => Self::ClusterNotDeployed,
            TabletClientError::DeploymentNotFound { id } => Self::DeploymentNotFound { id },
            TabletClientError::TabletNotDeployed { id } => Self::TabletNotDeployed { id },
            TabletClientError::NodeNotAvailable { node } => Self::NodeNotAvailable { node },
            TabletClientError::NodeNotConnectable { node, message } => Self::NodeNotConnectable { node, message },
            TabletClientError::GrpcError { status } => Self::GrpcError { status },
            TabletClientError::UnexpectedError { message } => Self::UnexpectedError { message },
            TabletClientError::ShardNotFound { tablet_id, shard_id, key } => {
                Self::ShardNotFound { tablet_id, shard_id, key }
            },
            TabletClientError::DataCorruption { message } => Self::DataCorruption { message },
            TabletClientError::KeyAlreadyExists { key } => Self::KeyAlreadyExists { key },
            TabletClientError::KeyTimestampMismatch { key, actual } => Self::KeyTimestampMismatch { key, actual },
            TabletClientError::InvalidArgument { message } => Self::InvalidArgument { message },
            TabletClientError::Internal(err) => Self::Internal(err),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum KvSemantics {
    Snapshot,
    Transactional,
    Inconsistent,
}

#[async_trait::async_trait]
pub trait KvClient: Send + Sync {
    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>>;
    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)>;

    async fn put(&self, key: Cow<'_, [u8]>, value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<Timestamp>;
    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64>;

    fn client(&self) -> &TabletClient;

    fn semantics(&self) -> KvSemantics;

    async fn commit(&self) -> Result<Timestamp> {
        Err(KvError::unexpected("not supported".to_string()))
    }

    async fn abort(&self) -> Result<()> {
        Err(KvError::unexpected("not supported".to_string()))
    }

    fn restart(&self) -> Result<()> {
        Err(KvError::unexpected("not supported".to_string()))
    }
}

pub trait KvClientExt: KvClient {
    fn shared(self) -> SharedKvClient
    where
        Self: Sized + 'static, {
        let client: Arc<dyn KvClient> = Arc::new(self);
        SharedKvClient::from(client)
    }

    fn wrapped(self) -> WrappedKvClient<Self>
    where
        Self: Sized, {
        WrappedKvClient::new(self)
    }
}

impl<T> KvClientExt for T where T: KvClient {}

pub struct WrappedKvClient<T> {
    client: T,
}

impl<T: KvClient> WrappedKvClient<T> {
    fn new(client: T) -> Self {
        Self { client }
    }

    pub fn wrapped(&self) -> &T {
        &self.client
    }

    pub async fn get(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<Option<(Timestamp, Value)>> {
        self.client.get(key.into()).await
    }

    pub async fn scan(
        &self,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        self.client.scan(start.into(), end.into(), limit).await
    }

    pub async fn put(
        &self,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        self.client.put(key.into(), value, expect_ts).await
    }

    pub async fn increment(&self, key: impl Into<Cow<'_, [u8]>>, increment: i64) -> Result<i64> {
        self.client.increment(key.into(), increment).await
    }

    pub fn client(&self) -> &TabletClient {
        self.client.client()
    }

    pub fn semantics(&self) -> KvSemantics {
        self.client.semantics()
    }

    pub async fn commit(&self) -> Result<Timestamp> {
        self.client.commit().await
    }

    pub async fn abort(&self) -> Result<()> {
        self.client.abort().await
    }

    pub fn restart(&self) -> Result<()> {
        self.client.restart()
    }
}

#[derive(Clone)]
pub struct SharedKvClient {
    client: Arc<dyn KvClient>,
}

impl From<Arc<dyn KvClient>> for SharedKvClient {
    fn from(client: Arc<dyn KvClient>) -> Self {
        Self { client }
    }
}

impl SharedKvClient {
    pub fn new<T: KvClient + 'static>(client: T) -> Self {
        Self { client: Arc::new(client) }
    }

    pub async fn get(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<Option<(Timestamp, Value)>> {
        self.client.get(key.into()).await
    }

    pub async fn scan(
        &self,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        self.client.scan(start.into(), end.into(), limit).await
    }

    pub async fn put(
        &self,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        self.client.put(key.into(), value, expect_ts).await
    }

    pub async fn increment(&self, key: impl Into<Cow<'_, [u8]>>, increment: i64) -> Result<i64> {
        self.client.increment(key.into(), increment).await
    }

    pub fn client(&self) -> &TabletClient {
        self.client.client()
    }

    pub fn semantics(&self) -> KvSemantics {
        self.client.semantics()
    }

    pub async fn commit(&self) -> Result<Timestamp> {
        self.client.commit().await
    }

    pub async fn abort(&self) -> Result<()> {
        self.client.abort().await
    }

    pub fn restart(&self) -> Result<()> {
        self.client.restart()
    }
}

#[async_trait::async_trait]
impl KvClient for TabletClient {
    fn client(&self) -> &TabletClient {
        self
    }

    fn semantics(&self) -> KvSemantics {
        KvSemantics::Inconsistent
    }

    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>> {
        match self.get_directly(Temporal::default(), key, 0).await? {
            (_temporal, None) => Err(KvError::unexpected("timestamp get get no response")),
            (_temporal, Some(response)) => Ok(response.value.map(|v| v.into_parts())),
        }
    }

    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        match self.scan_directly(Temporal::default(), start, end, limit).await? {
            (_temporal, None) => Err(KvError::unexpected("no scan response")),
            (_temporal, Some(response)) => Ok(response),
        }
    }

    async fn put(&self, key: Cow<'_, [u8]>, value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<Timestamp> {
        match self.put_directly(Temporal::default(), key, value, expect_ts, 0).await? {
            (_temporal, None) => Err(KvError::unexpected("timestamp put get aborted")),
            (_temporal, Some(write_ts)) => Ok(write_ts),
        }
    }

    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64> {
        match self.increment_directly(Temporal::default(), key, increment, 0).await? {
            (_temporal, None) => Err(KvError::unexpected("timestamp increment get aborted")),
            (_temporal, Some(incremented)) => Ok(incremented),
        }
    }
}

pub struct LazyInitTimestampedKvClient {
    client: TabletClient,
    lazy: Lazy<TimestampedKvClient>,
}

impl LazyInitTimestampedKvClient {
    pub fn new(client: TabletClient) -> Self {
        Self { client, lazy: Lazy::new() }
    }

    fn client(&self) -> &TimestampedKvClient {
        self.lazy.get_or_create(|| TimestampedKvClient::new(self.client.clone(), self.client.now()))
    }
}

#[async_trait::async_trait]
impl KvClient for LazyInitTimestampedKvClient {
    fn client(&self) -> &TabletClient {
        &self.client
    }

    fn semantics(&self) -> KvSemantics {
        KvSemantics::Snapshot
    }

    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>> {
        self.client().get(key).await
    }

    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        self.client().scan(start, end, limit).await
    }

    async fn put(&self, key: Cow<'_, [u8]>, value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<Timestamp> {
        self.client().put(key, value, expect_ts).await
    }

    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64> {
        self.client().increment(key, increment).await
    }
}

pub struct TimestampedKvClient {
    client: TabletClient,
    timestamp: Timestamp,
}

impl TimestampedKvClient {
    pub fn new(client: TabletClient, timestamp: Timestamp) -> Self {
        Self { client, timestamp }
    }
}

#[async_trait::async_trait]
impl KvClient for TimestampedKvClient {
    fn client(&self) -> &TabletClient {
        &self.client
    }

    fn semantics(&self) -> KvSemantics {
        KvSemantics::Snapshot
    }

    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>> {
        match self.client.get_directly(self.timestamp.into(), key, 0).await? {
            (_temporal, None) => Err(KvError::unexpected("timestamp get get no response")),
            (_temporal, Some(response)) => Ok(response.value.map(|r| r.into_parts())),
        }
    }

    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        match self.client.scan_directly(self.timestamp.into(), start, end, limit).await? {
            (_temporal, None) => Err(KvError::unexpected("no scan response")),
            (_temporal, Some(response)) => Ok(response),
        }
    }

    async fn put(&self, key: Cow<'_, [u8]>, value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<Timestamp> {
        match self.client.put_directly(self.timestamp.into(), key, value, expect_ts, 0).await? {
            (_temporal, None) => Err(KvError::unexpected("timestamp put get aborted")),
            (_temporal, Some(write_ts)) => Ok(write_ts),
        }
    }

    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64> {
        match self.client.increment_directly(self.timestamp.into(), key, increment, 0).await? {
            (_temporal, None) => Err(KvError::unexpected("timestamp increment get aborted")),
            (_temporal, Some(incremented)) => Ok(incremented),
        }
    }
}
