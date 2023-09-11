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

use anyhow::{anyhow, bail, Result};
use prost::Message as _;
use tonic::Status;

use crate::cluster::{ClusterEnv, NodeId};
use crate::keys::{self, KeyKind};
use crate::protos::{
    BatchRequest,
    DataRequest,
    DataResponse,
    FindRequest,
    FindResponse,
    GetRequest,
    IncrementRequest,
    PutRequest,
    TabletDeployment,
    TabletServiceClient,
    Timestamp,
    Value,
};

#[derive(Clone)]
pub struct TabletClient {
    cluster: ClusterEnv,
}

impl TabletClient {
    pub fn new(cluster: ClusterEnv) -> Self {
        Self { cluster }
    }

    async fn find_deployment(
        &self,
        tablet_id: u64,
        node: &NodeId,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<TabletDeployment>, Status> {
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else {
            return Err(Status::unavailable(format!("server {} is not available", node)));
        };
        let mut client =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        let key = key.into();
        let batch = BatchRequest {
            tablet_id,
            uncertainty: None,
            atomic: true,
            temporal: None,
            requests: vec![DataRequest::Find(FindRequest { key, sequence: 0 })],
        };
        let response = client.batch(batch).await?.into_inner();
        let find = response.into_find().map_err(|r| Status::internal(format!("unexpected find response: {:?}", r)))?;
        let FindResponse { key: located_key, value: Some(value) } = find else {
            return Err(Status::data_loss(format!("no deployment for key: {:?}", find.key)));
        };
        let bytes =
            value.read_bytes(&located_key, "read deployment bytes").map_err(|e| Status::data_loss(e.to_string()))?;
        let deployment = TabletDeployment::decode(bytes).map_err(|e| Status::data_loss(e.to_string()))?;
        if deployment.servers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(deployment))
        }
    }

    pub async fn locate(&self, key: impl Into<Cow<'_, [u8]>>) -> Result<TabletDeployment, Status> {
        let Some(meta) = self.cluster.latest_deployment() else {
            return Err(Status::unavailable("cluster not ready: no meta"));
        };
        if meta.servers.is_empty() {
            return Err(Status::unavailable("cluster not ready: no deployment"));
        }
        let key = key.into();
        if key.is_empty() {
            return Ok(meta.as_ref().clone());
        }

        let (kind, raw_key) = keys::identify_key(&key).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let root_key = keys::root_key(raw_key);
        if matches!(kind, KeyKind::Range { root: true }) {
            return Ok(meta.as_ref().clone());
        }

        let Some(deployment1) = self.find_deployment(1, NodeId::new(&meta.servers[0]), root_key).await? else {
            return Err(Status::not_found("no deployment found in root range tablet"));
        };
        if matches!(kind, KeyKind::Range { .. }) {
            return Ok(deployment1);
        }

        let range_key = keys::range_key(raw_key);
        let Some(deployment) =
            self.find_deployment(deployment1.tablet.id, NodeId::new(&deployment1.servers[0]), range_key).await?
        else {
            return Ok(deployment1);
        };
        Ok(deployment)
    }

    async fn request(&self, tablet_id: u64, node: &NodeId, request: DataRequest) -> Result<DataResponse> {
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else { bail!("server {} is not available", node) };
        let mut client =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        let batch =
            BatchRequest { tablet_id, uncertainty: None, atomic: true, temporal: None, requests: vec![request] };
        let response = client.batch(batch).await?.into_inner();
        let response = response.into_one().map_err(|r| anyhow!("expect one response, got {:?}", r))?;
        Ok(response)
    }

    async fn locate_key(&self, key: &[u8]) -> Result<TabletDeployment> {
        let deployment = self.locate(key).await?;
        if key < deployment.tablet.range.start.as_slice() || key >= deployment.tablet.range.end.as_slice() {
            bail!("no deployment found")
        }
        Ok(deployment)
    }

    async fn relocate_one_request(&self, request: &mut DataRequest) -> Result<TabletDeployment> {
        let user_key = keys::user_key(request.key());
        let deployment = self.locate_key(&user_key).await?;
        request.set_key(user_key);
        Ok(deployment)
    }

    async fn relocate_deployment_request(
        &self,
        deployment: &TabletDeployment,
        request: &mut DataRequest,
    ) -> Result<()> {
        let user_key = keys::user_key(request.key());
        if !deployment.tablet.range.contains(&user_key) {
            bail!(
                "key {:?} does not resides in tablet {} with range {:?}",
                request.key(),
                deployment.tablet.id,
                deployment.tablet.range
            )
        }
        request.set_key(user_key);
        Ok(())
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
        deployment: &TabletDeployment,
        requests: Vec<DataRequest>,
    ) -> Result<Vec<DataResponse>> {
        let node = NodeId::new(&deployment.servers[0]);
        let Some(addr) = self.cluster.nodes().get_endpoint(node) else { bail!("server {} is not available", node) };
        let mut client =
            TabletServiceClient::connect(addr.to_string()).await.map_err(|e| Status::unavailable(e.to_string()))?;
        let n = requests.len();
        let batch =
            BatchRequest { tablet_id: deployment.tablet.id, uncertainty: None, atomic: true, temporal: None, requests };
        let response = client.batch(batch).await?.into_inner();
        if response.responses.len() != n {
            bail!("unexpected responses: {:?}", response)
        }
        Ok(response.responses)
    }

    pub async fn batch(&self, mut requests: Vec<DataRequest>) -> Result<Vec<DataResponse>> {
        let Some((first, remains)) = requests.split_first_mut() else { bail!("empty requests") };
        let deployment = self.relocate_one_request(first).await?;
        for request in remains {
            self.relocate_deployment_request(&deployment, request).await?;
        }
        let mut responses = self.request_batch(&deployment, requests).await?;
        Self::relocate_user_responses(&mut responses);
        Ok(responses)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<(Timestamp, Value)>> {
        let user_key = keys::user_key(key);
        let deployment = self.locate_key(&user_key).await?;
        let get = GetRequest { key: user_key, sequence: 0 };
        let node_id = NodeId::new(&deployment.servers[0]);
        let response = self.request(deployment.tablet.id, node_id, DataRequest::Get(get)).await?;
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
        let deployment = self.locate_key(&user_key).await?;
        let put = PutRequest { key: user_key, value, sequence: 0, expect_ts };
        let node_id = NodeId::new(&deployment.servers[0]);
        let response = self.request(deployment.tablet.id, node_id, DataRequest::Put(put)).await?;
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
        let deployment = self.locate_key(&user_key).await?;
        let increment = IncrementRequest { key: user_key, increment, sequence: 0 };
        let node_id = NodeId::new(&deployment.servers[0]);
        let response = self.request(deployment.tablet.id, node_id, DataRequest::Increment(increment)).await?;
        let response = response.into_increment().map_err(|r| anyhow!("expect increment response, get {:?}", r))?;
        Ok(response.value)
    }

    pub async fn find(&self, key: &[u8]) -> Result<Option<(Timestamp, Vec<u8>, Value)>> {
        let user_key = keys::user_key(key);
        let deployment = self.locate_key(&user_key).await?;
        let find = FindRequest { key: user_key, sequence: 0 };
        let node_id = NodeId::new(&deployment.servers[0]);
        let response = self.request(deployment.tablet.id, node_id, DataRequest::Find(find)).await?;
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
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_deployment(deployment_watcher.monitor());
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
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_deployment(deployment_watcher.monitor());
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
