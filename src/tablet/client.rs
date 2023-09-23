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

use anyhow::anyhow;
use prost::Message as _;
use thiserror::Error;
use tonic::Status;

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
    PutRequest,
    ShardDescriptor,
    ShardId,
    TabletDeployment,
    TabletDescriptor,
    TabletId,
    TabletServiceClient,
    Timestamp,
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
            TabletClientError::ClusterNotReady | TabletClientError::ClusterNotDeployed => {
                Status::unavailable(err.to_string())
            },
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

    pub fn node_id(&self) -> &NodeId {
        NodeId::new(&self.tablet.servers[0])
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
        let node = deployment.node_id();
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
            atomic: true,
            temporal: None,
            shards: vec![deployment.shard_id().into()],
            requests: vec![DataRequest::Find(FindRequest { key, sequence: 0 })],
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

    async fn request(&self, deployment: &ShardDeployment, request: DataRequest) -> Result<DataResponse> {
        let node = deployment.node_id();
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else {
            return Err(TabletClientError::NodeNotAvailable { node: node.clone() });
        };
        let mut client =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        let tablet_id = deployment.tablet_id().into();
        let shard_id = deployment.shard_id().into();
        let batch = BatchRequest {
            tablet_id,
            uncertainty: None,
            atomic: true,
            temporal: None,
            shards: vec![shard_id],
            requests: vec![request],
        };
        let response = client.batch(batch).await?.into_inner();
        let response = response
            .into_one()
            .map_err(|r| TabletClientError::unexpected(format!("expect one response, got {:?}", r)))?;
        Ok(response)
    }

    async fn relocate_one_request(&self, request: &mut DataRequest) -> Result<ShardDeployment> {
        let user_key = keys::user_key(request.key());
        let deployment = self.locate(&user_key).await?;
        request.set_key(user_key);
        Ok(deployment)
    }

    fn relocate_user_responses(responses: &mut [DataResponse]) {
        for response in responses {
            if let Some(find) = response.as_find_mut() {
                find.key.drain(0..keys::USER_KEY_PREFIX.len());
            }
        }
    }

    async fn request_batch(
        &self,
        deployment: &ShardDeployment,
        shards: Vec<ShardId>,
        requests: Vec<DataRequest>,
    ) -> Result<Vec<DataResponse>> {
        let node = deployment.node_id();
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else {
            return Err(TabletClientError::NodeNotAvailable { node: node.clone() });
        };
        let mut client =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        let n = requests.len();
        let batch = BatchRequest {
            tablet_id: deployment.tablet.id,
            uncertainty: None,
            atomic: true,
            temporal: None,
            requests,
            shards: unsafe { std::mem::transmute(shards) },
        };
        let response = client.batch(batch).await?.into_inner();
        if response.responses.len() != n {
            return Err(TabletClientError::unexpected(format!("unexpected responses: {:?}", response)));
        }
        Ok(response.responses)
    }

    pub async fn batch(&self, mut requests: Vec<DataRequest>) -> Result<Vec<DataResponse>> {
        let Some((first, remains)) = requests.split_first_mut() else {
            return Err(TabletClientError::invalid_argument("empty requests"));
        };
        let deployment = self.relocate_one_request(first).await?;
        let mut shards = vec![deployment.shard_id()];
        for request in remains {
            let new_deployment = self.relocate_one_request(request).await?;
            if deployment.tablet_id() != new_deployment.tablet_id() {
                return Err(TabletClientError::invalid_argument(""));
            }
            shards.push(deployment.shard_id());
        }
        let mut responses = self.request_batch(&deployment, shards, requests).await?;
        Self::relocate_user_responses(&mut responses);
        Ok(responses)
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

    pub async fn get(&self, key: &[u8]) -> Result<Option<(Timestamp, Value)>> {
        let user_key = keys::user_key(key);
        let deployment = self.locate(&user_key).await?;
        let get = GetRequest { key: user_key, sequence: 0 };
        let response = self.request(&deployment, DataRequest::Get(get)).await?;
        let response = response.into_get().map_err(|r| anyhow!("expect get response, get {:?}", r))?;
        Ok(response.value.map(|v| (v.timestamp, v.value)))
    }

    async fn put_internally(
        &self,
        key: &[u8],
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        let user_key = keys::user_key(key);
        let deployment = self.locate(&user_key).await?;
        let put = PutRequest { key: user_key, value, sequence: 0, expect_ts };
        let response = self.request(&deployment, DataRequest::Put(put)).await?;
        let response = response.into_put().map_err(|r| anyhow!("expect put response, get {:?}", r))?;
        Ok(response.write_ts)
    }

    pub async fn delete(&self, key: &[u8], expect_ts: Option<Timestamp>) -> Result<()> {
        self.put_internally(key, None, expect_ts).await?;
        Ok(())
    }

    pub async fn put(&self, key: &[u8], value: Value, expect_ts: Option<Timestamp>) -> Result<Timestamp> {
        self.put_internally(key, Some(value), expect_ts).await
    }

    pub async fn increment(&self, key: &[u8], increment: i64) -> Result<i64> {
        let user_key = keys::user_key(key);
        let deployment = self.locate(&user_key).await?;
        let increment = IncrementRequest { key: user_key, increment, sequence: 0 };
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assertor::*;
    use tokio::net::TcpListener;

    use crate::cluster::tests::etcd_container;
    use crate::cluster::{ClusterEnv, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
    use crate::endpoint::{Endpoint, Params};
    use crate::log::{LogManager, MemoryLogFactory};
    use crate::protos::{
        DataRequest,
        FindRequest,
        GetRequest,
        IncrementRequest,
        PutRequest,
        Timestamp,
        TimestampedValue,
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
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let count = client.increment(b"count", 5).await.unwrap();
        assert_eq!(count, 5);
        let count = client.increment(b"count", 5).await.unwrap();
        assert_eq!(count, 10);

        let put_ts = client.put(b"k1", Value::Bytes(b"v1_1".to_vec()), None).await.unwrap();
        let (get_ts, value) = client.get(b"k1").await.unwrap().unwrap();
        assert_that!(get_ts).is_equal_to(put_ts);
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_1".to_vec());

        let put_ts = client.put(b"k1", Value::Bytes(b"v1_2".to_vec()), Some(put_ts)).await.unwrap();
        let (get_ts, value) = client.get(b"k1").await.unwrap().unwrap();
        assert_that!(get_ts).is_equal_to(put_ts);
        assert_that!(value.into_bytes().unwrap()).is_equal_to(b"v1_2".to_vec());

        client.delete(b"k1", Some(put_ts)).await.unwrap();
        assert_that!(client.get(b"k1").await.unwrap().is_none()).is_true();
        let put_ts = client.put(b"k1", Value::Bytes(b"v1_3".to_vec()), Some(Timestamp::default())).await.unwrap();

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
            DataRequest::Increment(IncrementRequest { key: b"count".to_vec(), increment: 5, sequence: 0 }),
            DataRequest::Get(GetRequest { key: b"count".to_vec(), sequence: 0 }),
            DataRequest::Increment(IncrementRequest { key: b"count".to_vec(), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: b"k1".to_vec(),
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
            DataRequest::Increment(IncrementRequest { key: b"count".to_vec(), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: b"k1".to_vec(),
                value: Some(Value::String("v1_1".to_owned())),
                expect_ts: Some(Timestamp::zero()),
                sequence: 0,
            }),
        ];
        client.batch(requests).await.unwrap_err();

        let requests = vec![
            DataRequest::Increment(IncrementRequest { key: b"count".to_vec(), increment: 5, sequence: 0 }),
            DataRequest::Put(PutRequest {
                key: b"k1".to_vec(),
                value: Some(Value::String("v1_2".to_owned())),
                expect_ts: Some(put_ts),
                sequence: 0,
            }),
        ];
        let mut responses = client.batch(requests).await.unwrap();
        let put_ts = responses.pop().unwrap().into_put().unwrap().write_ts;
        assert_that!(responses.pop().unwrap().into_increment().unwrap().value).is_equal_to(15);

        let requests = vec![
            DataRequest::Get(GetRequest { key: b"k1".to_vec(), sequence: 0 }),
            DataRequest::Find(FindRequest { key: b"k1".to_vec(), sequence: 0 }),
        ];
        let mut responses = client.batch(requests).await.unwrap();

        let expect_value = TimestampedValue { value: Value::String("v1_2".to_owned()), timestamp: put_ts };

        let find = responses.pop().unwrap().into_find().unwrap();
        assert_that!(find.key).is_equal_to(b"k1".to_vec());
        assert_that!(find.value.unwrap()).is_equal_to(&expect_value);

        let get = responses.pop().unwrap().into_get().unwrap();
        assert_that!(get.value.unwrap()).is_equal_to(&expect_value);
    }
}
