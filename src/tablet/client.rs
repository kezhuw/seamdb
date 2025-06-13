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
use std::time::Duration;

use anyhow::anyhow;
use enum_dispatch::enum_dispatch;
use prost::Message as _;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{debug, trace};

use crate::cluster::{ClusterEnv, NodeId};
use crate::keys;
use crate::protos::{
    BatchError,
    BatchRequest,
    BatchResponse,
    DataError,
    DataRequest,
    DataResponse,
    FindRequest,
    FindResponse,
    GetRequest,
    GetResponse,
    IncrementRequest,
    KeyRange,
    KeySpan,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    PutRequest,
    RefreshReadRequest,
    ScanRequest,
    ShardDescriptor,
    ShardId,
    ShardRequest,
    TabletDeployment,
    TabletDescriptor,
    TabletId,
    TabletServiceClient,
    Temporal,
    Timestamp,
    TimestampedKeyValue,
    Transaction,
    TxnMeta,
    Value,
};

// TODO: cache and invalidate on error
struct RootTabletClient {
    root: Arc<ShardDescriptor>,
    descriptor: Arc<ShardDescriptor>,
    deployment: Arc<ShardDescriptor>,
    cluster: ClusterEnv,
}

#[derive(Clone)]
struct ScopedTabletClient {
    prefix: Vec<u8>,
    client: Arc<RootTabletClient>,
}

#[enum_dispatch]
#[derive(Clone)]
enum InnerTabletClient {
    Root(Arc<RootTabletClient>),
    Scoped(ScopedTabletClient),
}

#[enum_dispatch(InnerTabletClient)]
trait TabletClientTrait {
    fn now(&self) -> Timestamp;

    fn prefix(&self) -> &[u8];

    fn root(&self) -> &RootTabletClient;

    #[allow(clippy::uninit_vec)]
    fn prefix_key<'a>(&self, key: impl Into<Cow<'a, [u8]>>) -> Vec<u8> {
        let key = key.into();
        let prefix = self.prefix();
        if prefix.is_empty() {
            return key.into_owned();
        }
        match key {
            Cow::Borrowed(key) => {
                let mut prefixed_key = Vec::with_capacity(prefix.len() + key.len());
                prefixed_key.extend_from_slice(prefix);
                prefixed_key.extend_from_slice(key);
                prefixed_key
            },
            Cow::Owned(mut key) => {
                let key_len = key.len();
                let prefix_len = prefix.len();
                key.reserve(prefix_len);
                unsafe {
                    key.set_len(key_len + prefix_len);
                    std::ptr::copy(key.as_ptr(), key.as_mut_ptr().wrapping_add(prefix_len), key_len);
                    std::ptr::copy_nonoverlapping(prefix.as_ptr(), key.as_mut_ptr(), prefix.len());
                }
                key
            },
        }
    }

    fn prefix_span(&self, span: KeySpan) -> KeySpan {
        let key = self.prefix_key(span.key);
        let end = if span.end.is_empty() { span.end } else { self.prefix_key(span.end) };
        KeySpan { key, end }
    }

    fn prefix_range(&self, range: KeyRange) -> KeyRange {
        let start = self.prefix_key(range.start);
        let end = self.prefix_key(range.end);
        KeyRange { start, end }
    }

    fn unprefix_key(&self, mut key: Vec<u8>) -> Result<Vec<u8>, Vec<u8>> {
        let Some(stripped) = key.strip_prefix(self.prefix()) else {
            return Err(key);
        };
        let (ptr, len) = (stripped.as_ptr(), stripped.len());
        unsafe {
            std::ptr::copy(ptr, key.as_mut_ptr(), len);
            key.set_len(len);
        }
        Ok(key)
    }

    fn new_transaction(&self, key: Vec<u8>) -> Transaction {
        let key = self.prefix_key(key);
        let meta = TxnMeta::new(key, self.now());
        Transaction { meta, ..Default::default() }
    }

    async fn heartbeat_txn(&self, txn: Transaction) -> Result<Transaction> {
        self.root().heartbeat_txn(txn).await
    }

    async fn get_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        sequence: u32,
    ) -> Result<(Temporal, Option<GetResponse>)> {
        let key = self.prefix_key(key);
        self.root().get_directly(temporal, key, sequence).await
    }

    async fn get(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<Option<(Timestamp, Value)>> {
        match self.get_directly(Temporal::default(), key, 0).await? {
            (_temporal, None) => Err(TabletClientError::unexpected("timestamp get get no response")),
            (_temporal, Some(response)) => Ok(response.value.map(|v| v.into_parts())),
        }
    }

    async fn put_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
        sequence: u32,
    ) -> Result<(Temporal, Option<Timestamp>)> {
        let key = self.prefix_key(key);
        self.root().put_directly(temporal, key, value, expect_ts, sequence).await
    }

    async fn put(
        &self,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        match self.put_directly(Temporal::default(), key, value, expect_ts, 0).await? {
            (_temporal, None) => Err(TabletClientError::unexpected("timestamp put get aborted")),
            (_temporal, Some(write_ts)) => Ok(write_ts),
        }
    }

    async fn refresh_read(
        &self,
        temporal: Temporal,
        span: KeySpan,
        from: Timestamp,
    ) -> Result<(Temporal, Option<Vec<u8>>)> {
        let span = self.prefix_span(span);
        let (temporal, resume_key) = self.root().refresh_read(temporal, span, from).await?;
        let resume_key = match resume_key {
            Some(resume_key) if !resume_key.is_empty() => Some(self.unprefix_key(resume_key).unwrap()),
            _ => resume_key,
        };
        Ok((temporal, resume_key))
    }

    async fn increment_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        increment: i64,
        sequence: u32,
    ) -> Result<(Temporal, Option<i64>)> {
        let key = self.prefix_key(key);
        self.root().increment_directly(temporal, key, increment, sequence).await
    }

    async fn increment(&self, key: impl Into<Cow<'_, [u8]>>, increment: i64) -> Result<i64> {
        match self.increment_directly(Temporal::default(), key, increment, 0).await? {
            (_temporal, None) => Err(TabletClientError::unexpected("timestamp increment get aborted")),
            (_temporal, Some(incremented)) => Ok(incremented),
        }
    }

    async fn scan_directly(
        &self,
        temporal: Temporal,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Temporal, Option<(Vec<u8>, Vec<TimestampedKeyValue>)>)> {
        let start = self.prefix_key(start);
        let end = self.prefix_key(end);
        match self.root().scan_directly(temporal, start, end, limit).await? {
            (temporal, None) => Ok((temporal, None)),
            (temporal, Some((resume_key, mut rows))) => {
                let resume_key =
                    if resume_key.is_empty() { resume_key } else { self.unprefix_key(resume_key).unwrap() };
                for row in rows.iter_mut() {
                    row.key = self.unprefix_key(std::mem::take(&mut row.key)).unwrap();
                }
                Ok((temporal, Some((resume_key, rows))))
            },
        }
    }

    async fn scan(
        &self,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        match self.scan_directly(Temporal::default(), start, end, limit).await? {
            (_temporal, None) => Err(TabletClientError::unexpected("no scan response")),
            (_temporal, Some(response)) => Ok(response),
        }
    }

    async fn locate(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<ShardDeployment> {
        let key = self.prefix_key(key);
        self.root().locate(key).await
    }

    async fn participate_txn(
        &self,
        request: ParticipateTxnRequest,
        coordinator: bool,
    ) -> Result<(mpsc::Sender<ParticipateTxnRequest>, tonic::Streaming<ParticipateTxnResponse>)> {
        self.root().participate_txn(request, coordinator).await
    }

    async fn open_participate_txn(
        &self,
        request: ParticipateTxnRequest,
        coordinator: bool,
    ) -> (mpsc::Sender<ParticipateTxnRequest>, tonic::Streaming<ParticipateTxnResponse>) {
        self.root().open_participate_txn(request, coordinator).await
    }

    async fn service(&self, key: &[u8]) -> Result<(ShardDeployment, TabletServiceClient<tonic::transport::Channel>)> {
        let key = self.prefix_key(key);
        self.root().service(&key).await
    }

    async fn batch(&self, mut requests: Vec<DataRequest>) -> Result<Vec<DataResponse>> {
        if self.prefix().is_empty() {
            return self.root().batch(requests).await;
        }
        for request in requests.iter_mut() {
            match request {
                DataRequest::Get(get) => {
                    get.key = self.prefix_key(Cow::Owned(std::mem::take(&mut get.key)));
                },
                DataRequest::Put(put) => {
                    put.key = self.prefix_key(Cow::Owned(std::mem::take(&mut put.key)));
                },
                DataRequest::Increment(increment) => {
                    increment.key = self.prefix_key(Cow::Owned(std::mem::take(&mut increment.key)));
                },
                DataRequest::Find(find) => {
                    find.key = self.prefix_key(Cow::Owned(std::mem::take(&mut find.key)));
                },
                DataRequest::Scan(scan) => {
                    scan.range = self.prefix_range(std::mem::take(&mut scan.range));
                },
                DataRequest::RefreshRead(refresh_read) => {
                    refresh_read.span = self.prefix_span(std::mem::take(&mut refresh_read.span));
                },
            }
        }
        let mut responses = self.root().batch(requests).await?;
        for response in responses.iter_mut() {
            match response {
                DataResponse::Get(_) => {},
                DataResponse::Put(_) => {},
                DataResponse::Increment(_) => {},
                DataResponse::Find(find) => {
                    if !find.key.is_empty() {
                        find.key = self.unprefix_key(std::mem::take(&mut find.key)).unwrap();
                    }
                },
                DataResponse::Scan(scan) => {
                    if !scan.resume_key.is_empty() {
                        scan.resume_key = self.unprefix_key(std::mem::take(&mut scan.resume_key)).unwrap();
                    }
                    for row in scan.rows.iter_mut() {
                        row.key = self.unprefix_key(std::mem::take(&mut row.key)).unwrap();
                    }
                },
                DataResponse::RefreshRead(refresh_read) => {
                    if !refresh_read.resume_key.is_empty() {
                        refresh_read.resume_key =
                            self.unprefix_key(std::mem::take(&mut refresh_read.resume_key)).unwrap();
                    }
                },
            }
        }
        Ok(responses)
    }

    async fn find(&self, key: &[u8]) -> Result<Option<(Timestamp, Vec<u8>, Value)>> {
        let key = self.prefix_key(key);
        let Some((ts, key, value)) = self.root().find(key).await? else {
            return Ok(None);
        };
        Ok(Some((ts, self.unprefix_key(key).unwrap(), value)))
    }

    async fn get_tablet_descriptor(&self, id: TabletId) -> Result<(Timestamp, TabletDescriptor)> {
        self.root().get_tablet_descriptor(id).await
    }
}

#[derive(Debug, Error)]
pub enum TabletClientError {
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
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<tonic::Status> for TabletClientError {
    fn from(status: tonic::Status) -> Self {
        Self::GrpcError { status }
    }
}

impl From<TabletClientError> for tonic::Status {
    fn from(err: TabletClientError) -> Self {
        match err {
            TabletClientError::ClusterNotReady
            | TabletClientError::ClusterNotDeployed
            | TabletClientError::TabletNotDeployed { .. } => Status::unavailable(err.to_string()),
            TabletClientError::DeploymentNotFound { .. }
            | TabletClientError::DataCorruption { .. }
            | TabletClientError::ShardNotFound { .. } => Status::data_loss(err.to_string()),
            TabletClientError::NodeNotAvailable { .. } | TabletClientError::NodeNotConnectable { .. } => {
                Status::unavailable(err.to_string())
            },
            TabletClientError::GrpcError { status } => status,
            TabletClientError::Internal(_) | TabletClientError::InvalidArgument { .. } => {
                Status::internal(err.to_string())
            },
            TabletClientError::UnexpectedError { .. } => Status::unknown(err.to_string()),
            TabletClientError::KeyAlreadyExists { .. } | TabletClientError::KeyTimestampMismatch { .. } => todo!(),
        }
    }
}

impl TabletClientError {
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

type Result<T, E = TabletClientError> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct ShardDeployment {
    shard: Arc<ShardDescriptor>,
    tablet: Arc<TabletDeployment>,
}

impl ShardDeployment {
    pub fn new(shard: Arc<ShardDescriptor>, tablet: Arc<TabletDeployment>) -> Self {
        Self { shard, tablet }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard.id.into()
    }

    pub fn tablet_id(&self) -> TabletId {
        self.tablet.id.into()
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.tablet.servers.first().map(NodeId::new)
    }

    pub fn shard(&self) -> &ShardDescriptor {
        self.shard.as_ref()
    }

    pub fn deployment(&self) -> &TabletDeployment {
        self.tablet.as_ref()
    }
}

impl TabletClientTrait for RootTabletClient {
    fn now(&self) -> Timestamp {
        self.cluster.clock().now()
    }

    fn prefix(&self) -> &[u8] {
        Default::default()
    }

    fn root(&self) -> &RootTabletClient {
        self
    }
}

impl TabletClientTrait for Arc<RootTabletClient> {
    fn now(&self) -> Timestamp {
        self.cluster.clock().now()
    }

    fn prefix(&self) -> &[u8] {
        Default::default()
    }

    fn root(&self) -> &RootTabletClient {
        self.as_ref()
    }
}

impl RootTabletClient {
    pub fn new(cluster: ClusterEnv) -> Self {
        Self {
            cluster,
            root: Arc::new(ShardDescriptor::root()),
            descriptor: Arc::new(ShardDescriptor::descriptor()),
            deployment: Arc::new(ShardDescriptor::deployment()),
        }
    }

    fn get_cluster_descriptor(&self) -> Result<(Timestamp, TabletDescriptor)> {
        let Some(descriptor) = self.cluster.latest_descriptor() else {
            return Err(TabletClientError::ClusterNotReady);
        };
        Ok((descriptor.timestamp, descriptor.to_tablet()))
    }

    fn get_root_tablet_deployment(&self) -> Result<Arc<TabletDeployment>> {
        let Some(deployment) = self.cluster.latest_deployment() else {
            return Err(TabletClientError::ClusterNotReady);
        };
        if deployment.servers.is_empty() {
            return Err(TabletClientError::ClusterNotDeployed);
        }
        Ok(deployment)
    }

    fn get_root_shard_deployment(&self) -> Result<ShardDeployment> {
        let Some(deployment) = self.cluster.latest_deployment() else {
            return Err(TabletClientError::ClusterNotReady);
        };
        if deployment.servers.is_empty() {
            return Err(TabletClientError::ClusterNotDeployed);
        }
        Ok(ShardDeployment::new(self.root.clone(), deployment))
    }

    fn get_descriptor_shard_deployment(&self) -> Result<ShardDeployment> {
        let deployment = self.get_root_tablet_deployment()?;
        Ok(ShardDeployment::new(self.descriptor.clone(), deployment))
    }

    fn get_deployment_shard_deployment(&self) -> Result<ShardDeployment> {
        let deployment = self.get_root_tablet_deployment()?;
        Ok(ShardDeployment::new(self.deployment.clone(), deployment))
    }

    pub async fn get_tablet_descriptor(&self, id: TabletId) -> Result<(Timestamp, TabletDescriptor)> {
        let deployment = self.get_descriptor_shard_deployment()?;
        if id == TabletId::ROOT {
            return self.get_cluster_descriptor();
        }
        let key = keys::descriptor_key(id);
        let Some((ts, value)) = self.raw_get(&deployment, key).await? else {
            return Err(TabletClientError::DeploymentNotFound { id });
        };
        let Value::Bytes(bytes) = value else {
            return Err(TabletClientError::corrupted(format!(
                "tablet {} expect descriptor bytes, but got {:?}",
                id, value
            )));
        };
        let descriptor =
            TabletDescriptor::decode(bytes.as_slice()).map_err(|e| TabletClientError::corrupted(e.to_string()))?;
        Ok((ts, descriptor))
    }

    pub async fn get_tablet_deployment(&self, id: TabletId) -> Result<Arc<TabletDeployment>> {
        if id == TabletId::ROOT {
            return self.get_root_tablet_deployment();
        }
        let deployment = self.get_deployment_shard_deployment()?;
        let key = keys::deployment_key(id);
        let Some((_ts, value)) = self.raw_get(&deployment, key).await? else {
            return Err(TabletClientError::DeploymentNotFound { id });
        };
        let Value::Bytes(bytes) = value else {
            return Err(TabletClientError::corrupted(format!(
                "tablet {} expect deployment bytes, but got {:?}",
                id, value
            )));
        };
        let deployment =
            TabletDeployment::decode(bytes.as_slice()).map_err(|e| TabletClientError::corrupted(e.to_string()))?;
        Ok(Arc::new(deployment))
    }

    async fn get_shard(&self, deployment: &ShardDeployment, key: impl Into<Vec<u8>>) -> Result<Arc<ShardDescriptor>> {
        let key = key.into();
        let find = self
            .batch_request(deployment, BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                uncertainty: None,
                temporal: Temporal::default(),
                requests: vec![ShardRequest {
                    shard_id: deployment.shard_id().into(),
                    request: DataRequest::Find(FindRequest { key: key.clone(), sequence: 0 }),
                }],
            })
            .await?
            .into_find()
            .map_err(|r| TabletClientError::unexpected(format!("unexpected find response: {:?}", r)))?;

        let FindResponse { key: located_key, value: Some(value) } = find else {
            return Err(TabletClientError::ShardNotFound {
                tablet_id: deployment.tablet_id(),
                shard_id: deployment.shard_id(),
                key,
            });
        };
        let bytes = value
            .read_bytes(&located_key, "read shard descritpor bytes")
            .map_err(|e| TabletClientError::corrupted(e.to_string()))?;
        let descritpor = ShardDescriptor::decode(bytes).map_err(|e| TabletClientError::corrupted(e.to_string()))?;
        Ok(Arc::new(descritpor))
    }

    async fn get_shard_deployment(
        &self,
        deployment: &ShardDeployment,
        key: impl Into<Vec<u8>>,
    ) -> Result<ShardDeployment> {
        let shard = self.get_shard(deployment, key).await?;
        let tablet = self.get_tablet_deployment(shard.tablet_id.into()).await?;
        Ok(ShardDeployment::new(shard, tablet))
    }

    pub async fn locate(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<ShardDeployment> {
        let root = self.get_root_shard_deployment()?;

        let key = key.into();
        if key.is_empty() {
            return Ok(root);
        }

        let (kind, _raw_key) = keys::identify_key(&key).map_err(|e| TabletClientError::unexpected(e.to_string()))?;
        if kind.is_root() {
            return Ok(root);
        }

        let shard_key = match kind.is_range() {
            true => key,
            false => Cow::Owned(keys::range_key(&key)),
        };

        let root_key = keys::root_key(&shard_key);
        let shard = self.get_shard_deployment(&root, root_key).await?;
        if kind.is_range() {
            return Ok(shard);
        }

        self.get_shard_deployment(&shard, shard_key).await
    }

    pub async fn connect(
        &self,
        deployment: &ShardDeployment,
    ) -> Result<TabletServiceClient<tonic::transport::Channel>> {
        let Some(node) = deployment.node_id() else {
            return Err(TabletClientError::TabletNotDeployed { id: deployment.tablet_id() });
        };
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else {
            return Err(TabletClientError::NodeNotAvailable { node: node.clone() });
        };
        trace!("connecting addr: {}, shard: {:?}", addr, deployment);
        TabletServiceClient::connect(addr.to_string())
            .await
            .map_err(|e| TabletClientError::NodeNotConnectable { node: node.clone(), message: e.to_string() })
    }

    pub async fn service(
        &self,
        key: &[u8],
    ) -> Result<(ShardDeployment, TabletServiceClient<tonic::transport::Channel>)> {
        let deployment = self.locate(key).await?;
        let service = self.connect(&deployment).await?;
        Ok((deployment, service))
    }

    async fn batch_request(&self, deployment: &ShardDeployment, request: BatchRequest) -> Result<BatchResponse> {
        let tablet_id = request.tablet_id;
        let mut service = self.connect(deployment).await?;
        match service.batch(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => {
                let details = status.details();
                if !details.is_empty() {
                    let err = BatchError::decode(details).unwrap();
                    trace!("batch error: {err:?}");
                    let err = match err.error {
                        DataError::ConflictWrite(err) => TabletClientError::KeyAlreadyExists { key: err.key },
                        DataError::ShardNotFound(err) => TabletClientError::ShardNotFound {
                            tablet_id: tablet_id.into(),
                            shard_id: err.shard_id.into(),
                            key: err.key,
                        },
                        DataError::TimestampMismatch(err) => {
                            TabletClientError::KeyTimestampMismatch { key: err.key, actual: err.actual }
                        },
                        err => TabletClientError::Internal(anyhow!("{err}")),
                    };
                    return Err(err);
                }
                Err(TabletClientError::from(status))
            },
        }
    }

    async fn request_batch(
        &self,
        deployment: &ShardDeployment,
        requests: Vec<ShardRequest>,
    ) -> Result<Vec<DataResponse>> {
        let n = requests.len();
        let batch = BatchRequest {
            tablet_id: deployment.tablet.id,
            uncertainty: None,
            temporal: Temporal::default(),
            requests,
        };
        let response = self.batch_request(deployment, batch).await?;
        if response.responses.len() != n {
            return Err(TabletClientError::unexpected(format!("unexpected responses: {:?}", response)));
        }
        Ok(response.responses.into_iter().map(|response| response.response).collect())
    }

    pub async fn batch(&self, mut requests: Vec<DataRequest>) -> Result<Vec<DataResponse>> {
        let Some((first, remains)) = requests.split_first_mut() else {
            return Err(TabletClientError::invalid_argument("empty requests"));
        };
        let deployment = self.locate(first.key()).await?;
        let mut shards = vec![deployment.shard_id()];
        for request in remains {
            let new_deployment = self.locate(request.key()).await?;
            if deployment.tablet_id() != new_deployment.tablet_id() {
                return Err(TabletClientError::invalid_argument(""));
            }
            shards.push(deployment.shard_id());
        }
        let requests = shards
            .into_iter()
            .zip(requests.into_iter())
            .map(|(shard_id, request)| ShardRequest { shard_id: shard_id.into(), request })
            .collect();
        self.request_batch(&deployment, requests).await
    }

    async fn get_internally(
        &self,
        deployment: &ShardDeployment,
        temporal: Temporal,
        key: Vec<u8>,
        sequence: u32,
    ) -> Result<(Temporal, GetResponse)> {
        let mut response = self
            .batch_request(deployment, BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                uncertainty: None,
                temporal,
                requests: vec![ShardRequest {
                    shard_id: deployment.shard_id().into(),
                    request: DataRequest::Get(GetRequest { key, sequence }),
                }],
            })
            .await?;
        let get_response = response
            .responses
            .remove(0)
            .response
            .into_get()
            .map_err(|r| TabletClientError::unexpected(format!("expect get response, got {r:?}")))?;
        Ok((response.temporal, get_response))
    }

    async fn raw_get(
        &self,
        deployment: &ShardDeployment,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<(Timestamp, Value)>> {
        let (_, response) = self.get_internally(deployment, Temporal::default(), key.into(), 0).await?;
        Ok(response.value.map(|v| (v.timestamp, v.value)))
    }

    pub async fn heartbeat_txn(&self, txn: Transaction) -> Result<Transaction> {
        let (shard, mut service) = self.service(&txn.meta.key).await?;
        let response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(response.temporal.into_transaction())
    }

    pub async fn get_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        sequence: u32,
    ) -> Result<(Temporal, Option<GetResponse>)> {
        let key = key.into().into_owned();
        let shard = self.locate(&key).await?;
        self.get_internally(&shard, temporal, key, sequence)
            .await
            .map(|(temporal, response)| (temporal, Some(response)))
    }

    async fn find(&self, key: Vec<u8>) -> Result<Option<(Timestamp, Vec<u8>, Value)>> {
        let shard = self.locate(&key).await?;
        let batch = BatchRequest {
            tablet_id: shard.tablet_id().into(),
            uncertainty: None,
            temporal: Temporal::default(),
            requests: vec![ShardRequest {
                shard_id: shard.shard_id().into(),
                request: DataRequest::Find(FindRequest { key, sequence: 0 }),
            }],
        };
        let mut response = self.batch_request(&shard, batch).await?;
        if response.responses.is_empty() {
            return Err(TabletClientError::unexpected("find get no response"));
        }
        let find_response = response
            .responses
            .remove(0)
            .response
            .into_find()
            .map_err(|r| TabletClientError::unexpected(format!("expect find response, got {r:?}")))?;
        Ok(find_response.value.map(|v| (v.timestamp, find_response.key, v.value)))
    }

    pub async fn put_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
        sequence: u32,
    ) -> Result<(Temporal, Option<Timestamp>)> {
        let key = key.into().into_owned();
        let shard = self.locate(&key).await?;
        let batch = BatchRequest {
            tablet_id: shard.tablet_id().into(),
            uncertainty: None,
            temporal,
            requests: vec![ShardRequest {
                shard_id: shard.shard_id().into(),
                request: DataRequest::Put(PutRequest { key, value, expect_ts, sequence }),
            }],
        };
        let mut response = self.batch_request(&shard, batch).await?;
        if response.responses.is_empty() {
            return Ok((response.temporal, None));
        }
        let put_response = response
            .responses
            .remove(0)
            .response
            .into_put()
            .map_err(|r| TabletClientError::unexpected(format!("expect put response, got {r:?}")))?;
        Ok((response.temporal, Some(put_response.write_ts)))
    }

    pub async fn refresh_read(
        &self,
        temporal: Temporal,
        span: KeySpan,
        from: Timestamp,
    ) -> Result<(Temporal, Option<Vec<u8>>)> {
        let shard = self.locate(&span.key).await?;
        let batch = BatchRequest {
            tablet_id: shard.tablet_id().into(),
            uncertainty: None,
            temporal,
            requests: vec![ShardRequest {
                shard_id: shard.shard_id().into(),
                request: DataRequest::RefreshRead(RefreshReadRequest { span, from }),
            }],
        };
        let mut response = self.batch_request(&shard, batch).await?;
        if response.responses.is_empty() {
            return Ok((response.temporal, None));
        }
        let refresh_read_response = response
            .responses
            .remove(0)
            .response
            .into_refresh_read()
            .map_err(|r| TabletClientError::unexpected(format!("expect refresh read response, got {r:?}")))?;
        Ok((response.temporal, Some(refresh_read_response.resume_key)))
    }

    pub async fn increment_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        increment: i64,
        sequence: u32,
    ) -> Result<(Temporal, Option<i64>)> {
        let key = key.into().into_owned();
        let shard = self.locate(&key).await?;
        let batch = BatchRequest {
            tablet_id: shard.tablet_id().into(),
            uncertainty: None,
            temporal,
            requests: vec![ShardRequest {
                shard_id: shard.shard_id().into(),
                request: DataRequest::Increment(IncrementRequest { key, increment, sequence }),
            }],
        };
        let mut response = self.batch_request(&shard, batch).await?;
        if response.responses.is_empty() {
            return Ok((response.temporal, None));
        }
        let increment_response = response
            .responses
            .remove(0)
            .response
            .into_increment()
            .map_err(|r| TabletClientError::unexpected(format!("expect put response, got {r:?}")))?;
        Ok((response.temporal, Some(increment_response.value)))
    }

    pub async fn scan_directly(
        &self,
        temporal: Temporal,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Temporal, Option<(Vec<u8>, Vec<TimestampedKeyValue>)>)> {
        let start = start.into().into_owned();
        let shard = self.locate(&start).await?;
        let batch = BatchRequest {
            tablet_id: shard.tablet_id().into(),
            uncertainty: None,
            temporal,
            requests: vec![ShardRequest {
                shard_id: shard.shard_id().into(),
                request: DataRequest::Scan(ScanRequest {
                    range: KeyRange { start, end: end.into().into_owned() },
                    limit,
                    sequence: 0,
                }),
            }],
        };
        let mut response = self.batch_request(&shard, batch).await?;
        if response.responses.is_empty() {
            return Ok((response.temporal, None));
        }
        let scan_response = response
            .responses
            .remove(0)
            .response
            .into_scan()
            .map_err(|r| TabletClientError::unexpected(format!("expect scan response, got {r:?}")))?;
        Ok((response.temporal, Some(scan_response.into_parts())))
    }

    pub async fn open_participate_txn(
        &self,
        request: ParticipateTxnRequest,
        coordinator: bool,
    ) -> (mpsc::Sender<ParticipateTxnRequest>, tonic::Streaming<ParticipateTxnResponse>) {
        let mut timeout = Duration::from_millis(5);
        loop {
            match self.participate_txn(request.clone(), coordinator).await {
                Err(err) => {
                    debug!("fail to open participate txn stream: {err:?}");
                    tokio::time::sleep(timeout).await;
                    timeout += Duration::from_millis(timeout.as_millis() as u64 / 2);
                    continue;
                },
                Ok(result) => return result,
            }
        }
    }

    pub async fn participate_txn(
        &self,
        request: ParticipateTxnRequest,
        coordinator: bool,
    ) -> Result<(mpsc::Sender<ParticipateTxnRequest>, tonic::Streaming<ParticipateTxnResponse>)> {
        let (_deployment, mut service) = self.service(&request.txn.meta.key).await?;
        let (sender, receiver) = mpsc::channel(128);
        sender.send(request).await.unwrap();
        let mut request = tonic::Request::new(ReceiverStream::new(receiver));
        let metadata = request.metadata_mut();
        metadata.insert("seamdb-txn-coordinator", coordinator.to_string().parse().unwrap());
        let responses = service.participate_txn(request).await?.into_inner();
        Ok((sender, responses))
    }
}

impl TabletClientTrait for ScopedTabletClient {
    fn now(&self) -> Timestamp {
        self.client.now()
    }

    fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    fn root(&self) -> &RootTabletClient {
        &self.client
    }
}

impl ScopedTabletClient {
    pub fn scope(mut self, prefix: &[u8]) -> Self {
        self.prefix.extend_from_slice(prefix);
        self
    }
}

impl From<ScopedTabletClient> for TabletClient {
    fn from(client: ScopedTabletClient) -> Self {
        Self { inner: InnerTabletClient::Scoped(client) }
    }
}

impl From<Arc<RootTabletClient>> for ScopedTabletClient {
    fn from(client: Arc<RootTabletClient>) -> Self {
        ScopedTabletClient { prefix: Default::default(), client }
    }
}

#[derive(Clone)]
pub struct TabletClient {
    inner: InnerTabletClient,
}

impl TabletClient {
    pub fn new(cluster: ClusterEnv) -> Self {
        let client = RootTabletClient::new(cluster);
        Self { inner: InnerTabletClient::Root(client.into()) }
    }

    fn scoped_client(self) -> ScopedTabletClient {
        match self.inner {
            InnerTabletClient::Root(client) => ScopedTabletClient { prefix: vec![], client },
            InnerTabletClient::Scoped(client) => client,
        }
    }

    pub fn env(&self) -> &ClusterEnv {
        &self.inner.root().cluster
    }

    pub fn scope(self, prefix: &[u8]) -> Self {
        self.scoped_client().scope(prefix).into()
    }

    pub fn now(&self) -> Timestamp {
        self.inner.now()
    }

    pub fn prefix(&self) -> &[u8] {
        self.inner.prefix()
    }

    pub fn prefix_key<'a>(&self, key: impl Into<Cow<'a, [u8]>>) -> Vec<u8> {
        self.inner.prefix_key(key)
    }

    pub fn unprefix_key(&self, key: Vec<u8>) -> Result<Vec<u8>, Vec<u8>> {
        self.inner.unprefix_key(key)
    }

    pub fn new_transaction(&self, key: Vec<u8>) -> Transaction {
        self.inner.new_transaction(key)
    }

    pub async fn heartbeat_txn(&self, txn: Transaction) -> Result<Transaction> {
        self.inner.heartbeat_txn(txn).await
    }

    pub async fn get_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        sequence: u32,
    ) -> Result<(Temporal, Option<GetResponse>)> {
        self.inner.get_directly(temporal, key, sequence).await
    }

    pub async fn put_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
        sequence: u32,
    ) -> Result<(Temporal, Option<Timestamp>)> {
        self.inner.put_directly(temporal, key, value, expect_ts, sequence).await
    }

    pub async fn refresh_read(
        &self,
        temporal: Temporal,
        span: KeySpan,
        from: Timestamp,
    ) -> Result<(Temporal, Option<Vec<u8>>)> {
        self.inner.refresh_read(temporal, span, from).await
    }

    pub async fn increment_directly(
        &self,
        temporal: Temporal,
        key: impl Into<Cow<'_, [u8]>>,
        increment: i64,
        sequence: u32,
    ) -> Result<(Temporal, Option<i64>)> {
        self.inner.increment_directly(temporal, key, increment, sequence).await
    }

    pub async fn scan_directly(
        &self,
        temporal: Temporal,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Temporal, Option<(Vec<u8>, Vec<TimestampedKeyValue>)>)> {
        self.inner.scan_directly(temporal, start, end, limit).await
    }

    pub async fn get(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<Option<(Timestamp, Value)>> {
        self.inner.get(key).await
    }

    pub async fn put(
        &self,
        key: impl Into<Cow<'_, [u8]>>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        self.inner.put(key, value, expect_ts).await
    }

    pub async fn increment(&self, key: impl Into<Cow<'_, [u8]>>, increment: i64) -> Result<i64> {
        self.inner.increment(key, increment).await
    }

    pub async fn scan(
        &self,
        start: impl Into<Cow<'_, [u8]>>,
        end: impl Into<Cow<'_, [u8]>>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        self.inner.scan(start, end, limit).await
    }

    pub async fn locate(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<ShardDeployment> {
        self.inner.locate(key).await
    }

    pub async fn participate_txn(
        &self,
        request: ParticipateTxnRequest,
        coordinator: bool,
    ) -> Result<(mpsc::Sender<ParticipateTxnRequest>, tonic::Streaming<ParticipateTxnResponse>)> {
        self.inner.participate_txn(request, coordinator).await
    }

    pub async fn open_participate_txn(
        &self,
        request: ParticipateTxnRequest,
        coordinator: bool,
    ) -> (mpsc::Sender<ParticipateTxnRequest>, tonic::Streaming<ParticipateTxnResponse>) {
        self.inner.open_participate_txn(request, coordinator).await
    }

    pub async fn service(
        &self,
        key: &[u8],
    ) -> Result<(ShardDeployment, TabletServiceClient<tonic::transport::Channel>)> {
        self.inner.service(key).await
    }

    pub async fn batch(&self, requests: Vec<DataRequest>) -> Result<Vec<DataResponse>> {
        self.inner.batch(requests).await
    }

    pub async fn find(&self, key: &[u8]) -> Result<Option<(Timestamp, Vec<u8>, Value)>> {
        self.inner.find(key).await
    }

    pub async fn get_tablet_descriptor(&self, id: TabletId) -> Result<(Timestamp, TabletDescriptor)> {
        self.inner.get_tablet_descriptor(id).await
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::time::Duration;

    use assertor::*;
    use asyncs::select;
    use tokio::net::TcpListener;
    use tracing::debug;

    use crate::cluster::tests::etcd_container;
    use crate::cluster::{ClusterEnv, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
    use crate::endpoint::Endpoint;
    use crate::fs::MemoryFileSystemFactory;
    use crate::keys;
    use crate::log::{LogManager, MemoryLogFactory};
    use crate::protos::{
        BatchRequest,
        DataRequest,
        FindRequest,
        GetRequest,
        HasTxnMeta,
        IncrementRequest,
        KeyRange,
        KeySpan,
        PutRequest,
        RefreshReadRequest,
        ScanRequest,
        SequenceRange,
        ShardRequest,
        Temporal,
        Timestamp,
        TimestampedValue,
        Transaction,
        TxnMeta,
        TxnStatus,
        Value,
    };
    use crate::tablet::{TabletClient, TabletNode};

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn test_tablet_client_basic() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());

        tokio::time::sleep(Duration::from_secs(20)).await;

        let client = TabletClient::new(cluster_env).scope(keys::USER_KEY_PREFIX);

        let count = client.increment(b"count", 5).await.unwrap();
        assert_eq!(count, 5);
        let count = client.increment(b"count", 5).await.unwrap();
        assert_eq!(count, 10);

        let put_ts = client.put(b"k1", Some(Value::Bytes(b"v1_1".to_vec())), None).await.unwrap();
        let (get_ts, value) = client.get(b"k1").await.unwrap().unwrap();
        assert_that!(get_ts).is_equal_to(put_ts);
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_1".to_vec());

        let put_ts = client.put(b"k1", Some(Value::Bytes(b"v1_2".to_vec())), Some(put_ts)).await.unwrap();
        let (get_ts, value) = client.get(b"k1").await.unwrap().unwrap();
        assert_that!(get_ts).is_equal_to(put_ts);
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_2".to_vec());

        client.put(b"k1", None, Some(put_ts)).await.unwrap();
        assert_that!(client.get(b"k1").await.unwrap().is_none()).is_true();
        let put_ts = client.put(b"k1", Some(Value::Bytes(b"v1_3".to_vec())), Some(Timestamp::default())).await.unwrap();

        let (ts, key, value) = client.find(b"k").await.unwrap().unwrap();
        assert_that!(ts).is_equal_to(put_ts);
        assert_that!(key.as_slice()).is_equal_to(b"k1".as_slice());
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_3".to_vec());

        let (ts, key, value) = client.find(b"k1").await.unwrap().unwrap();
        assert_that!(ts).is_equal_to(put_ts);
        assert_that!(key.as_slice()).is_equal_to(b"k1".as_slice());
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_3".to_vec());

        assert_that!(client.find(b"kz").await.unwrap().is_none()).is_true();
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn test_tablet_client_batch() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(keys::USER_KEY_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: b"count".into(), increment: 5, sequence: 0 }),
            DataRequest::Get(GetRequest { key: b"count".into(), sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: b"count".into(), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: b"k1".into(),
                value: Some(Value::String("v1_1".to_owned())),
                expect_ts: None,
                sequence: 0,
            }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let put_ts = responses.pop().unwrap().into_put().unwrap().write_ts;
        assert_that!(responses.pop().unwrap().into_increment().unwrap().value).is_equal_to(10);
        assert_that!(responses.pop().unwrap().into_get().unwrap().value.unwrap().value.into_int().unwrap())
            .is_equal_to(5);
        assert_that!(responses.pop().unwrap().into_increment().unwrap().value).is_equal_to(5);

        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: b"count".into(), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: b"k1".into(),
                value: Some(Value::String("v1_1".to_owned())),
                expect_ts: Some(Timestamp::ZERO),
                sequence: 0,
            }),
        ];
        client.batch(requests).await.unwrap_err();

        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: b"count".into(), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: b"k1".into(),
                value: Some(Value::String("v1_2".to_owned())),
                expect_ts: Some(put_ts),
                sequence: 0,
            }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let put_ts = responses.pop().unwrap().into_put().unwrap().write_ts;
        assert_that!(responses.pop().unwrap().into_increment().unwrap().value).is_equal_to(15);

        let requests = vec![
            DataRequest::Get(GetRequest { key: b"k1".into(), sequence: 0 }),
            DataRequest::Find(FindRequest { key: b"k1".into(), sequence: 0 }),
        ];
        let mut responses = client.batch(requests).await.unwrap();

        let expect_value = TimestampedValue { value: Value::String("v1_2".to_owned()), timestamp: put_ts };

        let find = responses.pop().unwrap().into_find().unwrap();
        assert_that!(find.key).is_equal_to(b"k1".to_vec());
        assert_that!(find.value.unwrap()).is_equal_to(&expect_value);

        let get = responses.pop().unwrap().into_get().unwrap();
        assert_that!(get.value.unwrap()).is_equal_to(&expect_value);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn test_tablet_client_find_timestamped() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(keys::USER_KEY_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let requests = vec![
            DataRequest::Find(FindRequest { key: b"count".into(), sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: b"count0".into(), increment: 5, sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: b"count1".into(), increment: 10, sequence: 0 }),
            DataRequest::Find(FindRequest { key: b"count".into(), sequence: 0 }),
            DataRequest::Find(FindRequest { key: b"count01".into(), sequence: 0 }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let find = responses.remove(0).into_find().unwrap();
        assert_that!(find.key).is_empty();
        assert_that!(find.value).is_none();

        responses.remove(0);
        responses.remove(0);

        let find0 = responses.remove(0).into_find().unwrap();
        assert_that!(find0.key).is_equal_to(b"count0".to_vec());
        assert_that!(find0.value).is_some();
        assert_that!(find0.value.unwrap().value).is_equal_to(Value::Int(5));

        let find1 = responses.remove(0).into_find().unwrap();
        assert_that!(find1.key).is_equal_to(b"count1".to_vec());
        assert_that!(find1.value.unwrap().value).is_equal_to(Value::Int(10));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn test_tablet_client_scan_timestamped() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(keys::USER_KEY_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let requests = vec![
            DataRequest::Scan(ScanRequest {
                range: KeyRange { start: b"count".into(), end: b"counu".into() },
                limit: 0,
                sequence: 0,
            }),
            DataRequest::Increment(IncrementRequest { key: b"count0".into(), increment: 5, sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: b"count1".into(), increment: 10, sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: b"count2".into(), increment: 15, sequence: 0 }),
            DataRequest::Scan(ScanRequest {
                range: KeyRange { start: b"count".into(), end: b"counu".into() },
                limit: 2,
                sequence: 0,
            }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let scan = responses.remove(0).into_scan().unwrap();
        debug!("resume key: {:?}", scan.resume_key);
        assert_that!(scan.rows).is_empty();

        responses.remove(0);
        responses.remove(0);
        responses.remove(0);

        let scan = responses.remove(0).into_scan().unwrap();
        debug!("resume key: {:?}", scan.resume_key);
        assert_that!(scan.rows).has_length(2);
        assert_that!(scan.rows[0].key).is_equal_to(b"count0".to_vec());
        assert_that!(scan.rows[0].value).is_equal_to(Value::Int(5));
        assert_that!(scan.rows[1].key).is_equal_to(b"count1".to_vec());
        assert_that!(scan.rows[1].value).is_equal_to(Value::Int(10));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn test_tablet_client_scan_transactional() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let txn = client.new_transaction(keys::user_key(b"count"));

        let requests = vec![
            DataRequest::Scan(ScanRequest {
                range: KeyRange { start: keys::user_key(b"count"), end: keys::user_key(b"counu") },
                limit: 0,
                sequence: 0,
            }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count0"), increment: 5, sequence: 1 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count1"), increment: 10, sequence: 2 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count2"), increment: 15, sequence: 3 }),
            DataRequest::Scan(ScanRequest {
                range: KeyRange { start: keys::user_key(b"count"), end: keys::user_key(b"counu") },
                limit: 2,
                sequence: 3,
            }),
        ];

        let (deployment, mut service) = client.service(&txn.meta.key).await.unwrap();

        let mut responses = service
            .batch(BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                requests: requests.into_iter().map(|request| ShardRequest { shard_id: 0, request }).collect(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .responses;

        let scan = responses.remove(0).response.into_scan().unwrap();
        debug!("resume key: {:?}", scan.resume_key);
        assert_that!(scan.rows).is_empty();

        responses.remove(0);
        responses.remove(0);
        responses.remove(0);

        let scan = responses.remove(0).response.into_scan().unwrap();
        debug!("resume key: {:?}", scan.resume_key);
        assert_that!(scan.rows).has_length(2);
        assert_that!(scan.rows[0].key).is_equal_to(keys::user_key(b"count0"));
        assert_that!(scan.rows[0].value).is_equal_to(Value::Int(5));
        assert_that!(scan.rows[1].key).is_equal_to(keys::user_key(b"count1"));
        assert_that!(scan.rows[1].value).is_equal_to(Value::Int(10));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_read_your_write() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let locating_key = keys::user_key(b"count");
        let (deployment, mut service) = client.service(&locating_key).await.unwrap();
        let txn = client.new_transaction(locating_key);
        service
            .batch(BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: deployment.shard_id().into(),
                    request: DataRequest::Increment(IncrementRequest {
                        key: keys::user_key(b"count"),
                        increment: 5,
                        sequence: 1,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let get = service
            .batch(BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: deployment.shard_id().into(),
                    request: DataRequest::Get(GetRequest { key: keys::user_key(b"count"), sequence: 1 }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .into_get()
            .unwrap();
        assert_eq!(get.value.unwrap().value, Value::Int(5));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_once() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let locating_key = keys::user_key(b"count");
        let (deployment, mut service) = client.service(&locating_key).await.unwrap();
        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 1 }),
            DataRequest::Get(GetRequest { key: keys::user_key(b"count"), sequence: 1 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 2 }),
            DataRequest::Put(PutRequest {
                key: keys::user_key(b"k1"),
                value: Some(Value::String("v1_1".to_owned())),
                expect_ts: None,
                sequence: 2,
            }),
        ];
        let mut txn = client.new_transaction(locating_key);
        txn.status = TxnStatus::Committed;
        let batch_request = BatchRequest {
            tablet_id: deployment.tablet_id().into(),
            temporal: Temporal::Transaction(txn),
            requests: requests
                .into_iter()
                .map(|request| ShardRequest { shard_id: deployment.shard_id().into(), request })
                .collect(),
            ..Default::default()
        };
        service.batch(batch_request).await.unwrap().into_inner();

        let incremented = client.increment(keys::user_key(b"count"), 100).await.unwrap();
        assert_eq!(incremented, 110);

        let (_ts, value) = client.get(keys::user_key(b"k1")).await.unwrap().unwrap();
        assert_eq!(value, Value::String("v1_1".to_owned()));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_stepped() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let locating_key = keys::user_key(b"count");
        let (deployment, mut service) = client.service(&locating_key).await.unwrap();
        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 1 }),
            DataRequest::Get(GetRequest { key: keys::user_key(b"count"), sequence: 1 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 2 }),
            DataRequest::Put(PutRequest {
                key: keys::user_key(b"k1"),
                value: Some(Value::String("v1_1".to_owned())),
                expect_ts: None,
                sequence: 3,
            }),
        ];
        let mut txn = client.new_transaction(locating_key);
        let batch_request = BatchRequest {
            tablet_id: deployment.tablet_id().into(),
            temporal: Temporal::Transaction(txn.clone()),
            requests: requests
                .into_iter()
                .map(|request| ShardRequest { shard_id: deployment.shard_id().into(), request })
                .collect(),
            ..Default::default()
        };
        service.batch(batch_request).await.unwrap().into_inner();

        let incremented_future = client.increment(keys::user_key(b"count"), 100);
        let get_future = client.get(keys::user_key(b"k1"));
        let mut incremented_future = pin!(incremented_future);
        let mut get_future = pin!(get_future);

        for _ in 0..10 {
            select! {
                r = incremented_future.as_mut() => unreachable!("{r:?}"),
                r = get_future.as_mut() => unreachable!("{r:?}"),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {},
            }
            service
                .batch(BatchRequest {
                    tablet_id: deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(txn.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        txn.status = TxnStatus::Committed;
        txn.rollbacked_sequences.push(SequenceRange { start: 2, end: 3 });
        let response = service
            .batch(BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let txn = response.temporal.into_transaction();
        assert_eq!(txn.status, TxnStatus::Committed);

        let mut get_done = false;
        let mut incremented_done = false;
        loop {
            select! {
                r = incremented_future.as_mut(), if !incremented_done => {
                    let incremented = r.unwrap();
                    assert_eq!(incremented, 105);
                    incremented_done = true;
                }
                r = get_future.as_mut(), if !get_done => {
                    let (_ts, value) = r.unwrap().unwrap();
                    assert_eq!(value, Value::String("v1_1".to_owned()));
                    get_done = true;
                },
                complete => break,
            }
        }
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_resolve_committed() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");
        let tablet_id_counter = client
            .get(tablet_id_counter_key.clone())
            .await
            .unwrap()
            .unwrap()
            .1
            .read_int(b"tablet-id-counter", "read")
            .unwrap();

        let (system_tablet_deployment, mut system_tablet_service) =
            client.service(&tablet_id_counter_key).await.unwrap();

        let user_tablet_key = keys::user_key(b"counter");
        let (user_tablet_deployment, mut user_tablet_service) = client.service(&user_tablet_key).await.unwrap();

        let mut txn = client.new_transaction(tablet_id_counter_key.clone());

        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: tablet_id_counter_key.clone(),
                        increment: 100,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        user_tablet_service
            .batch(BatchRequest {
                tablet_id: user_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: user_tablet_key.clone(),
                        increment: 100,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let user_tablet_get_future = client.get(user_tablet_key.clone());
        let system_tablet_get_future = client.get(tablet_id_counter_key.clone());

        let mut user_tablet_get_future = pin!(user_tablet_get_future);
        let mut system_tablet_get_future = pin!(system_tablet_get_future);

        for _ in 0..10 {
            select! {
                r = user_tablet_get_future.as_mut() => unreachable!("{r:?}"),
                r = system_tablet_get_future.as_mut() => unreachable!("{r:?}"),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {},
            }
            system_tablet_service
                .batch(BatchRequest {
                    tablet_id: system_tablet_deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(txn.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        txn.status = TxnStatus::Committed;
        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(Transaction {
                    commit_set: vec![KeySpan { key: keys::user_key(b"counter"), end: vec![] }, KeySpan {
                        key: keys::system_key(b"tablet-id-counter"),
                        end: vec![],
                    }],
                    ..txn.clone()
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut user_tablet_get_done = false;
        let mut system_tablet_get_done = false;
        loop {
            select! {
                r = user_tablet_get_future.as_mut(), if !user_tablet_get_done => {
                    let (_ts, value) = r.unwrap().unwrap();
                    assert_eq!(value, Value::Int(100));
                    user_tablet_get_done = true;
                }
                r = system_tablet_get_future.as_mut(), if !system_tablet_get_done => {
                    let (_ts, value) = r.unwrap().unwrap();
                    assert_eq!(value, Value::Int(tablet_id_counter + 100));
                    system_tablet_get_done = true;
                },
                complete => break,
            }
        }
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_resolve_aborted() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");
        let tablet_id_counter = client
            .get(tablet_id_counter_key.clone())
            .await
            .unwrap()
            .unwrap()
            .1
            .read_int(b"tablet-id-counter", "read")
            .unwrap();

        let (system_tablet_deployment, mut system_tablet_service) =
            client.service(&tablet_id_counter_key).await.unwrap();

        let user_tablet_key = keys::user_key(b"counter");
        let (user_tablet_deployment, mut user_tablet_service) = client.service(&user_tablet_key).await.unwrap();

        let mut txn = client.new_transaction(tablet_id_counter_key.clone());

        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: tablet_id_counter_key.clone(),
                        increment: 100,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        user_tablet_service
            .batch(BatchRequest {
                tablet_id: user_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: user_tablet_key.clone(),
                        increment: 100,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let user_tablet_get_future = client.get(user_tablet_key.clone());
        let system_tablet_get_future = client.get(tablet_id_counter_key.clone());

        let mut user_tablet_get_future = pin!(user_tablet_get_future);
        let mut system_tablet_get_future = pin!(system_tablet_get_future);

        for _ in 0..10 {
            select! {
                r = user_tablet_get_future.as_mut() => unreachable!("{r:?}"),
                r = system_tablet_get_future.as_mut() => unreachable!("{r:?}"),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {},
            }
            system_tablet_service
                .batch(BatchRequest {
                    tablet_id: system_tablet_deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(txn.clone()),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        txn.status = TxnStatus::Aborted;
        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(Transaction {
                    commit_set: vec![KeySpan { key: keys::user_key(b"counter"), end: vec![] }, KeySpan {
                        key: keys::system_key(b"tablet-id-counter"),
                        end: vec![],
                    }],
                    ..txn.clone()
                }),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut user_tablet_get_done = false;
        let mut system_tablet_get_done = false;
        loop {
            select! {
                r = user_tablet_get_future.as_mut(), if !user_tablet_get_done => {
                    assert!(r.unwrap().is_none());
                    user_tablet_get_done = true;
                }
                r = system_tablet_get_future.as_mut(), if !system_tablet_get_done => {
                    let (_ts, value) = r.unwrap().unwrap();
                    assert_eq!(value, Value::Int(tablet_id_counter));
                    system_tablet_get_done = true;
                },
                complete => break,
            }
        }
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_timeout_aborted() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");
        let tablet_id_counter = client
            .get(tablet_id_counter_key.clone())
            .await
            .unwrap()
            .unwrap()
            .1
            .read_int(b"tablet-id-counter", "read")
            .unwrap();

        let (system_tablet_deployment, mut system_tablet_service) =
            client.service(&tablet_id_counter_key).await.unwrap();

        let user_tablet_key = keys::user_key(b"counter");
        let (user_tablet_deployment, mut user_tablet_service) = client.service(&user_tablet_key).await.unwrap();

        let txn = client.new_transaction(tablet_id_counter_key.clone());

        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: tablet_id_counter_key.clone(),
                        increment: 100,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        user_tablet_service
            .batch(BatchRequest {
                tablet_id: user_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: user_tablet_key.clone(),
                        increment: 100,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(client.get(tablet_id_counter_key.clone()).await.unwrap().unwrap().1, Value::Int(tablet_id_counter));
        assert!(client.get(user_tablet_key.clone()).await.unwrap().is_none());
    }

    async fn heartbeat_txn(client: TabletClient, txn: TxnMeta) {
        let mut txn = Transaction { meta: txn, ..Default::default() };
        let (deployment, mut service) = client.service(txn.key()).await.unwrap();
        loop {
            txn = service
                .batch(BatchRequest {
                    tablet_id: deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(txn),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .temporal
                .into_transaction();

            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_refresh_read() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");

        let (system_tablet_deployment, mut system_tablet_service) =
            client.service(&tablet_id_counter_key).await.unwrap();

        let user_tablet_key = keys::user_key(b"counter");
        let (user_tablet_deployment, mut user_tablet_service) = client.service(&user_tablet_key).await.unwrap();

        let mut txn = client.new_transaction(tablet_id_counter_key.clone());

        let tablet_id_counter = system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Get(GetRequest { key: tablet_id_counter_key.clone(), sequence: 0 }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .into_get()
            .unwrap()
            .value
            .unwrap()
            .value
            .read_int(&tablet_id_counter_key, "read tablet id")
            .unwrap();

        let heartbeat = heartbeat_txn(client.clone(), txn.meta.clone());
        let mut heartbeat = pin!(heartbeat);

        select! {
            _ = heartbeat.as_mut() => unreachable!(""),
            Ok(_) = user_tablet_service
                .batch(BatchRequest {
                    tablet_id: user_tablet_deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(txn.clone()),
                    requests: vec![ShardRequest {
                        shard_id: 0,
                        request: DataRequest::Increment(IncrementRequest {
                            key: user_tablet_key.clone(),
                            increment: tablet_id_counter,
                            sequence: 1,
                        }),
                    }],
                    ..Default::default()
                }) => {},
        }

        let last_read_ts = txn.commit_ts();
        select! {
            _ = heartbeat.as_mut() => unreachable!(""),
            _ = tokio::time::sleep(Duration::from_millis(2)) => {},
        }
        txn.meta.commit_ts = client.now();

        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::RefreshRead(RefreshReadRequest {
                        span: KeySpan { key: tablet_id_counter_key.clone(), end: vec![] },
                        from: last_read_ts,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        select! {
            _ = heartbeat.as_mut() => unreachable!(""),
            _ = tokio::time::sleep(Duration::from_millis(2)) => {},
        }

        select! {
            _ = heartbeat.as_mut() => unreachable!(""),
            Ok(_) = system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Timestamp(client.now()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: tablet_id_counter_key.clone(),
                        increment: 1,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            }) => {},
        }

        let last_read_ts = txn.commit_ts();
        select! {
            _ = heartbeat.as_mut() => unreachable!(""),
            _ = tokio::time::sleep(Duration::from_millis(2)) => {},
        }
        txn.meta.commit_ts = client.now();

        let status = system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::RefreshRead(RefreshReadRequest {
                        span: KeySpan { key: tablet_id_counter_key.clone(), end: vec![] },
                        from: last_read_ts,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap_err();

        assert_that!(status.message()).contains("conflict with write to key");
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_deadlock() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");

        let (system_tablet_deployment, mut system_tablet_service) =
            client.service(&tablet_id_counter_key).await.unwrap();

        let mut system_tablet_txn = client.new_transaction(tablet_id_counter_key.clone());

        system_tablet_service
            .batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(system_tablet_txn.clone()),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: tablet_id_counter_key.clone(),
                        increment: 1,
                        sequence: 0,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let system_tablet_txn_heartbeat = heartbeat_txn(client.clone(), system_tablet_txn.meta.clone());
        let mut system_tablet_txn_heartbeat = pin!(system_tablet_txn_heartbeat);

        let user_tablet_key = keys::user_key(b"counter");
        let user_tablet_txn = client.new_transaction(user_tablet_key.clone());

        let (user_tablet_deployment, mut user_tablet_service) = select! {
            _ = system_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            Ok((user_tablet_deployment, user_tablet_service)) = client.service(&user_tablet_key) => (user_tablet_deployment, user_tablet_service),
        };

        select! {
            _ = system_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            Ok(_) = user_tablet_service
                .batch(BatchRequest {
                    tablet_id: user_tablet_deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(user_tablet_txn.clone()),
                    requests: vec![ShardRequest {
                        shard_id: 0,
                        request: DataRequest::Increment(IncrementRequest {
                            key: user_tablet_key.clone(),
                            increment: 100,
                            sequence: 1,
                        }),
                    }],
                    ..Default::default()
                }) => {},
        }

        let user_tablet_txn_heartbeat = heartbeat_txn(client.clone(), user_tablet_txn.meta.clone());
        let mut user_tablet_txn_heartbeat = pin!(user_tablet_txn_heartbeat);

        let mut system_tablet_txn_user_operation_client = user_tablet_service.clone();
        let system_tablet_txn_user_operation = {
            let user_tablet_key = user_tablet_key.clone();
            let system_tablet_txn = system_tablet_txn.clone();
            system_tablet_txn_user_operation_client.batch(BatchRequest {
                tablet_id: user_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(system_tablet_txn),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: user_tablet_key,
                        increment: 50,
                        sequence: 1,
                    }),
                }],
                ..Default::default()
            })
        };
        let mut system_tablet_txn_user_operation = pin!(system_tablet_txn_user_operation);

        select! {
            _ = user_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            _ = system_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            _ = system_tablet_txn_user_operation.as_mut() => unreachable!(""),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
        }

        let mut user_tablet_txn_system_client = Box::new(system_tablet_service.clone());
        let user_tablet_txn_system_operation = {
            let tablet_id_counter_key = tablet_id_counter_key.clone();
            let user_tablet_txn = user_tablet_txn.clone();
            user_tablet_txn_system_client.batch(BatchRequest {
                tablet_id: system_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(user_tablet_txn),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Increment(IncrementRequest {
                        key: tablet_id_counter_key,
                        increment: 50,
                        sequence: 1,
                    }),
                }],
                ..Default::default()
            })
        };
        let mut user_tablet_txn_system_operation = pin!(user_tablet_txn_system_operation);

        select! {
            _ = user_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            _ = system_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            _ = system_tablet_txn_user_operation.as_mut() => {},
            _ = user_tablet_txn_system_operation.as_mut() => {},
        }

        system_tablet_txn.status = TxnStatus::Committed;
        select! {
            _ = user_tablet_txn_heartbeat.as_mut() => unreachable!(""),
            Ok(_) = system_tablet_service
                .batch(BatchRequest {
                    tablet_id: system_tablet_deployment.tablet_id().into(),
                    temporal: Temporal::Transaction(Transaction {
                        commit_set: vec![KeySpan { key: keys::user_key(b"counter"), end: vec![] }, KeySpan {
                            key: keys::system_key(b"tablet-id-counter"),
                            end: vec![],
                        }],
                        ..system_tablet_txn.clone()
                    }),
                    ..Default::default()
                }) => {},
        }

        let piggybacked_user_tablet_txn = user_tablet_service
            .batch(BatchRequest {
                tablet_id: user_tablet_deployment.tablet_id().into(),
                temporal: Temporal::Transaction(user_tablet_txn.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .temporal
            .into_transaction();
        assert_eq!(piggybacked_user_tablet_txn.epoch(), user_tablet_txn.epoch() + 1);

        let (_ts, value) = client.get(keys::user_key(b"counter")).await.unwrap().unwrap();
        assert_eq!(value, Value::Int(50));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_write_beneath_closed_timestamp() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);

        let write_ts = client.now();
        tokio::time::sleep(Duration::from_secs(30)).await;

        let counter_key = keys::user_key(b"counter");

        let (deployment, mut service) = client.service(&counter_key).await.unwrap();

        let response = service
            .batch(BatchRequest {
                tablet_id: deployment.tablet_id().into(),
                temporal: Temporal::Timestamp(write_ts),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Put(PutRequest {
                        key: counter_key.clone(),
                        value: Some(Value::Int(1)),
                        sequence: 0,
                        expect_ts: None,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let written_ts = response.temporal.timestamp();
        assert_that!(written_ts).is_greater_than(write_ts);
        assert_that!(response.into_put().unwrap().write_ts).is_equal_to(written_ts);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_timestamped_write_push_forward() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let counter_key = keys::user_key(b"counter");

        let (shard, mut service) = client.service(&counter_key).await.unwrap();

        let write_ts = client.now();
        let not_found_read_ts = client.now();
        assert_that!(not_found_read_ts).is_greater_than(write_ts);

        service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Timestamp(not_found_read_ts),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Get(GetRequest { key: counter_key.clone(), sequence: 0 }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Timestamp(write_ts),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Put(PutRequest {
                        key: counter_key.clone(),
                        value: Some(Value::Int(1)),
                        sequence: 0,
                        expect_ts: None,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let written_ts = response.temporal.timestamp();
        assert_that!(written_ts).is_greater_than(write_ts);
        assert_that!(response.into_put().unwrap().write_ts).is_equal_to(written_ts);

        let write_ts = client.now();
        let read_ts = client.now();
        assert_that!(read_ts).is_greater_than(write_ts);

        let read_value = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Timestamp(read_ts),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Get(GetRequest { key: counter_key.clone(), sequence: 0 }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .into_get()
            .unwrap()
            .value
            .unwrap();
        assert_that!(read_value.value).is_equal_to(Value::Int(1));
        assert_that!(read_value.timestamp).is_equal_to(written_ts);

        let response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Timestamp(write_ts),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Put(PutRequest {
                        key: counter_key.clone(),
                        value: Some(Value::Int(2)),
                        sequence: 0,
                        expect_ts: None,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let written_ts = response.temporal.timestamp();
        assert_that!(written_ts).is_greater_than(write_ts);
        assert_that!(response.into_put().unwrap().write_ts).is_equal_to(written_ts);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_write_push_forward() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let counter_key = keys::user_key(b"counter");
        let txn = client.new_transaction(counter_key.clone());

        let (shard, mut service) = client.service(&counter_key).await.unwrap();

        let not_found_read_ts = client.now();
        let write_ts = txn.commit_ts();
        assert_that!(not_found_read_ts).is_greater_than(write_ts);

        service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Timestamp(not_found_read_ts),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Get(GetRequest { key: counter_key.clone(), sequence: 0 }),
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        let mut response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Put(PutRequest {
                        key: counter_key.clone(),
                        value: Some(Value::Int(1)),
                        sequence: 0,
                        expect_ts: None,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let txn = std::mem::take(&mut response.temporal).into_transaction();
        let written_ts = txn.commit_ts();
        assert_that!(written_ts).is_greater_than(write_ts);

        service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(Transaction { status: TxnStatus::Committed, ..txn }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();

        let read_value = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Get(GetRequest { key: counter_key.clone(), sequence: 0 }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .into_get()
            .unwrap()
            .value
            .unwrap();
        assert_that!(read_value.value).is_equal_to(Value::Int(1));
        assert_that!(read_value.timestamp).is_equal_to(written_ts);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_client_transactional_commit_push_forward() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), MemoryFileSystemFactory.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let counter_key = keys::user_key(b"counter");
        let txn = client.new_transaction(counter_key.clone());

        let (shard, mut service) = client.service(&counter_key).await.unwrap();

        let mut response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                requests: vec![ShardRequest {
                    shard_id: 0,
                    request: DataRequest::Put(PutRequest {
                        key: counter_key.clone(),
                        value: Some(Value::Int(1)),
                        sequence: 0,
                        expect_ts: None,
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let txn = std::mem::take(&mut response.temporal).into_transaction();

        let txn_heartbeat = heartbeat_txn(client.clone(), txn.meta.clone());

        select! {
            _ = txn_heartbeat => unreachable!(""),
            _ = tokio::time::sleep(Duration::from_secs(30)) => {},
        };

        let write_ts = txn.commit_ts();
        let txn = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(Transaction { status: TxnStatus::Committed, ..txn }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .temporal
            .into_transaction();
        assert_that!(txn.status).is_equal_to(TxnStatus::Pending);
        assert_that!(txn.commit_ts()).is_greater_than(write_ts);
    }
}
