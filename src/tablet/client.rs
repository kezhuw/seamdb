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
use prost::Message as _;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::debug;

use crate::cluster::{ClusterEnv, NodeId};
use crate::keys;
use crate::protos::{
    BatchRequest,
    DataRequest,
    DataResponse,
    FindRequest,
    FindResponse,
    GetRequest,
    IncrementRequest,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    PutRequest,
    ShardDescriptor,
    ShardId,
    ShardRequest,
    TabletDeployment,
    TabletDescriptor,
    TabletId,
    TabletServiceClient,
    Temporal,
    Timestamp,
    TimestampedValue,
    Transaction,
    TxnMeta,
    Uuid,
    Value,
};

// TODO: cache and invalidate on error
#[derive(Clone)]
pub struct TabletClient {
    root: Arc<ShardDescriptor>,
    descriptor: Arc<ShardDescriptor>,
    deployment: Arc<ShardDescriptor>,
    cluster: ClusterEnv,
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

impl TabletClient {
    pub fn new(cluster: ClusterEnv) -> Self {
        Self {
            cluster,
            root: Arc::new(ShardDescriptor::root()),
            descriptor: Arc::new(ShardDescriptor::descriptor()),
            deployment: Arc::new(ShardDescriptor::deployment()),
        }
    }

    pub fn now(&self) -> Timestamp {
        self.cluster.clock().now()
    }

    pub fn new_transaction(&self, key: Vec<u8>) -> Transaction {
        let meta = TxnMeta { id: Uuid::new_random(), key, epoch: 0, start_ts: self.cluster.clock().now(), priority: 0 };
        Transaction { meta, ..Default::default() }
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
        let Some(node) = deployment.node_id() else {
            return Err(TabletClientError::TabletNotDeployed { id: deployment.tablet_id() });
        };
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else {
            return Err(TabletClientError::NodeNotAvailable { node: node.clone() });
        };
        let mut client = TabletServiceClient::connect(addr.to_string())
            .await
            .map_err(|e| TabletClientError::NodeNotConnectable { node: node.clone(), message: e.to_string() })?;
        let key = key.into();
        let batch = BatchRequest {
            tablet_id: deployment.tablet_id().into(),
            uncertainty: None,
            temporal: Temporal::default(),
            requests: vec![ShardRequest {
                shard_id: deployment.shard_id().into(),
                request: DataRequest::Find(FindRequest { key, sequence: 0 }),
            }],
        };
        let response = client.batch(batch).await?.into_inner();
        let find = response
            .into_find()
            .map_err(|r| TabletClientError::unexpected(format!("unexpected find response: {:?}", r)))?;
        let FindResponse { key: located_key, value: Some(value) } = find else {
            return Err(TabletClientError::ShardNotFound {
                tablet_id: deployment.tablet_id(),
                shard_id: deployment.shard_id(),
                key: find.key,
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
        let service =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(service)
    }

    pub async fn service(
        &self,
        key: &[u8],
    ) -> Result<(ShardDeployment, TabletServiceClient<tonic::transport::Channel>)> {
        let deployment = self.locate(key).await?;
        let service = self.connect(&deployment).await?;
        Ok((deployment, service))
    }

    async fn request(&self, deployment: &ShardDeployment, request: DataRequest) -> Result<DataResponse> {
        let mut service = self.connect(deployment).await?;
        let tablet_id = deployment.tablet_id().into();
        let shard_id = deployment.shard_id().into();
        let batch = BatchRequest {
            tablet_id,
            uncertainty: None,
            temporal: Temporal::default(),
            requests: vec![ShardRequest { shard_id, request }],
        };
        let response = service.batch(batch).await?.into_inner();
        let response = response
            .into_one()
            .map_err(|r| TabletClientError::unexpected(format!("expect one response, got {:?}", r)))?;
        Ok(response.response)
    }

    async fn request_batch(
        &self,
        deployment: &ShardDeployment,
        requests: Vec<ShardRequest>,
    ) -> Result<Vec<DataResponse>> {
        let Some(node) = deployment.node_id() else {
            return Err(TabletClientError::TabletNotDeployed { id: deployment.tablet_id() });
        };
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else {
            return Err(TabletClientError::NodeNotAvailable { node: node.clone() });
        };
        let mut client =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        let n = requests.len();
        let batch = BatchRequest {
            tablet_id: deployment.tablet.id,
            uncertainty: None,
            temporal: Temporal::default(),
            requests,
        };
        let response = client.batch(batch).await?.into_inner();
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

    async fn raw_get(
        &self,
        deployment: &ShardDeployment,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<(Timestamp, Value)>> {
        let get = GetRequest { key: key.into(), sequence: 0 };
        let response = self.request(deployment, DataRequest::Get(get)).await?;
        let response = response.into_get().map_err(|r| anyhow!("expect get response, get {:?}", r))?;
        Ok(response.value.map(|v| (v.timestamp, v.value)))
    }

    pub async fn get(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<Option<(Timestamp, Value)>> {
        let key = key.into().into_owned();
        let deployment = self.locate(&key).await?;
        let get = GetRequest { key, sequence: 0 };
        let response = self.request(&deployment, DataRequest::Get(get)).await?;
        let response = response.into_get().map_err(|r| anyhow!("expect get response, get {:?}", r))?;
        Ok(response.value.map(|v| (v.timestamp, v.value)))
    }

    async fn put_internally(
        &self,
        key: Cow<'_, [u8]>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        let key = key.into_owned();
        let deployment = self.locate(&key).await?;
        let put = PutRequest { key, value, sequence: 0, expect_ts };
        let response = self.request(&deployment, DataRequest::Put(put)).await?;
        let response = response.into_put().map_err(|r| anyhow!("expect put response, get {:?}", r))?;
        Ok(response.write_ts)
    }

    pub async fn delete(&self, key: impl Into<Cow<'_, [u8]>>, expect_ts: Option<Timestamp>) -> Result<()> {
        self.put_internally(key.into(), None, expect_ts).await?;
        Ok(())
    }

    pub async fn put(
        &self,
        key: impl Into<Cow<'_, [u8]>>,
        value: Value,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        self.put_internally(key.into(), Some(value), expect_ts).await
    }

    pub async fn increment(&self, key: impl Into<Cow<'_, [u8]>>, increment: i64) -> Result<i64> {
        let key = key.into();
        let deployment = self.locate(key.as_ref()).await?;
        let increment = IncrementRequest { key: key.to_vec(), increment, sequence: 0 };
        let response = self.request(&deployment, DataRequest::Increment(increment)).await?;
        let response = response.into_increment().map_err(|r| anyhow!("expect increment response, get {:?}", r))?;
        Ok(response.value)
    }

    pub async fn find(&self, key: &[u8]) -> Result<Option<(Timestamp, Vec<u8>, Value)>> {
        let user_key = keys::user_key(key);
        let deployment = self.locate(&user_key).await?;
        let find = FindRequest { key: user_key, sequence: 0 };
        let response = self.request(&deployment, DataRequest::Find(find)).await?;
        let response = response.into_find().map_err(|r| anyhow!("expect find response, get {:?}", r))?;
        match response.value {
            None => Ok(None),
            Some(value) => {
                let mut key = response.key;
                key.drain(0..keys::USER_KEY_PREFIX.len());
                Ok(Some((value.timestamp, key, value.value)))
            },
        }
    }

    pub async fn transactional_get(
        &self,
        txn: Transaction,
        key: &[u8],
        sequence: u32,
    ) -> Result<(Transaction, Option<TimestampedValue>)> {
        let (shard, mut service) = self.service(key).await?;
        let mut response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                requests: vec![ShardRequest {
                    shard_id: shard.shard_id().into(),
                    request: DataRequest::Get(GetRequest { key: key.to_owned(), sequence }),
                }],
                ..Default::default()
            })
            .await?
            .into_inner();
        let txn = std::mem::take(&mut response.temporal).into_transaction();
        let get = response.into_get().map_err(|r| anyhow!("expect get response, get {r:?}"))?;
        Ok((txn, get.value))
    }

    pub async fn transactional_put(
        &self,
        txn: Transaction,
        key: &[u8],
        value: Option<Value>,
        sequence: u32,
        expect_ts: Option<Timestamp>,
    ) -> Result<Transaction> {
        let (shard, mut service) = self.service(key).await?;
        let mut response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                requests: vec![ShardRequest {
                    shard_id: shard.shard_id().into(),
                    request: DataRequest::Put(PutRequest { key: key.to_owned(), value, sequence, expect_ts }),
                }],
                ..Default::default()
            })
            .await?
            .into_inner();
        let txn = std::mem::take(&mut response.temporal).into_transaction();
        response.into_put().map_err(|r| anyhow!("expect put response, get {r:?}"))?;
        Ok(txn)
    }

    pub async fn transactional_increment(
        &self,
        txn: Transaction,
        key: &[u8],
        increment: i64,
        sequence: u32,
    ) -> Result<(Transaction, i64)> {
        let (shard, mut service) = self.service(key).await?;
        let mut response = service
            .batch(BatchRequest {
                tablet_id: shard.tablet_id().into(),
                temporal: Temporal::Transaction(txn),
                requests: vec![ShardRequest {
                    shard_id: shard.shard_id().into(),
                    request: DataRequest::Increment(IncrementRequest { key: key.to_owned(), increment, sequence }),
                }],
                ..Default::default()
            })
            .await?
            .into_inner();
        let txn = std::mem::take(&mut response.temporal).into_transaction();
        let increment = response.into_increment().map_err(|r| anyhow!("expect increment response, get {r:?}"))?;
        Ok((txn, increment.value))
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
    use crate::endpoint::{Endpoint, Params};
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());

        tokio::time::sleep(Duration::from_secs(20)).await;

        let client = TabletClient::new(cluster_env);

        let count = client.increment(keys::user_key(b"count"), 5).await.unwrap();
        assert_eq!(count, 5);
        let count = client.increment(keys::user_key(b"count"), 5).await.unwrap();
        assert_eq!(count, 10);

        let put_ts = client.put(keys::user_key(b"k1"), Value::Bytes(b"v1_1".to_vec()), None).await.unwrap();
        let (get_ts, value) = client.get(keys::user_key(b"k1")).await.unwrap().unwrap();
        assert_that!(get_ts).is_equal_to(put_ts);
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_1".to_vec());

        let put_ts = client.put(keys::user_key(b"k1"), Value::Bytes(b"v1_2".to_vec()), Some(put_ts)).await.unwrap();
        let (get_ts, value) = client.get(keys::user_key(b"k1")).await.unwrap().unwrap();
        assert_that!(get_ts).is_equal_to(put_ts);
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_2".to_vec());

        client.delete(keys::user_key(b"k1"), Some(put_ts)).await.unwrap();
        assert_that!(client.get(keys::user_key(b"k1")).await.unwrap().is_none()).is_true();
        let put_ts = client
            .put(keys::user_key(b"k1"), Value::Bytes(b"v1_3".to_vec()), Some(Timestamp::default()))
            .await
            .unwrap();

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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 0 }),
            DataRequest::Get(GetRequest { key: keys::user_key(b"count"), sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: keys::user_key(b"k1"),
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
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: keys::user_key(b"k1"),
                value: Some(Value::String("v1_1".to_owned())),
                expect_ts: Some(Timestamp::ZERO),
                sequence: 0,
            }),
        ];
        client.batch(requests).await.unwrap_err();

        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count"), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: keys::user_key(b"k1"),
                value: Some(Value::String("v1_2".to_owned())),
                expect_ts: Some(put_ts),
                sequence: 0,
            }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let put_ts = responses.pop().unwrap().into_put().unwrap().write_ts;
        assert_that!(responses.pop().unwrap().into_increment().unwrap().value).is_equal_to(15);

        let requests = vec![
            DataRequest::Get(GetRequest { key: keys::user_key(b"k1"), sequence: 0 }),
            DataRequest::Find(FindRequest { key: keys::user_key(b"k1"), sequence: 0 }),
        ];
        let mut responses = client.batch(requests).await.unwrap();

        let expect_value = TimestampedValue { value: Value::String("v1_2".to_owned()), timestamp: put_ts };

        let find = responses.pop().unwrap().into_find().unwrap();
        assert_that!(find.key).is_equal_to(keys::user_key(b"k1"));
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let requests = vec![
            DataRequest::Find(FindRequest { key: keys::user_key(b"count"), sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count0"), increment: 5, sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count1"), increment: 10, sequence: 0 }),
            DataRequest::Find(FindRequest { key: keys::user_key(b"count"), sequence: 0 }),
            DataRequest::Find(FindRequest { key: keys::user_key(b"count01"), sequence: 0 }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let find = responses.remove(0).into_find().unwrap();
        assert_that!(find.key).is_empty();
        assert_that!(find.value).is_none();

        responses.remove(0);
        responses.remove(0);

        let find0 = responses.remove(0).into_find().unwrap();
        assert_that!(find0.key).is_equal_to(keys::user_key(b"count0"));
        assert_that!(find0.value).is_some();
        assert_that!(find0.value.unwrap().value).is_equal_to(Value::Int(5));

        let find1 = responses.remove(0).into_find().unwrap();
        assert_that!(find1.key).is_equal_to(keys::user_key(b"count1"));
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let requests = vec![
            DataRequest::Scan(ScanRequest {
                range: KeyRange { start: keys::user_key(b"count"), end: keys::user_key(b"counu") },
                limit: 0,
                sequence: 0,
            }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count0"), increment: 5, sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count1"), increment: 10, sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: keys::user_key(b"count2"), increment: 15, sequence: 0 }),
            DataRequest::Scan(ScanRequest {
                range: KeyRange { start: keys::user_key(b"count"), end: keys::user_key(b"counu") },
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
        assert_that!(scan.rows[0].key).is_equal_to(keys::user_key(b"count0"));
        assert_that!(scan.rows[0].value).is_equal_to(Value::Int(5));
        assert_that!(scan.rows[1].key).is_equal_to(keys::user_key(b"count1"));
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        txn.commit_ts = client.now();

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
        txn.commit_ts = client.now();

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

        assert_that!(status.message()).contains("fail to refresh key");
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
        let log_manager =
            LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::ENDPOINT, &Params::default()).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
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
