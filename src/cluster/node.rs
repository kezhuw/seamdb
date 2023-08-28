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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use etcd_client::{
    Client,
    EventType,
    GetOptions,
    PutOptions,
    ResponseHeader,
    WatchOptions,
    WatchResponse,
    WatchStream,
    Watcher,
};
use ignore_result::Ignore;
use scopeguard::defer;
use tokio::select;
use tokio::sync::watch;
use uuid::Uuid;

use super::etcd::{EtcdHelper, EtcdLease};
use crate::endpoint::{Endpoint, OwnedEndpoint, ServiceUri};
use crate::utils::{self, DropOwner, DropWatcher};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct NodeId(pub(crate) String);

impl NodeId {
    pub fn new_random() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for NodeId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeStatistics {
    offline: bool,
    node_number: usize,
}

impl NodeStatistics {
    fn new(node_number: usize) -> Self {
        Self { offline: false, node_number }
    }

    pub fn new_offline() -> Self {
        Self { offline: true, node_number: 0 }
    }

    pub fn node_number(&self) -> usize {
        self.node_number
    }

    pub fn offline(&self) -> bool {
        self.offline
    }
}

#[derive(Clone)]
pub struct NodeStatisticsWatcher {
    offline: bool,
    receiver: watch::Receiver<NodeStatistics>,
}

impl NodeStatisticsWatcher {
    fn new(receiver: watch::Receiver<NodeStatistics>) -> Self {
        Self { offline: false, receiver }
    }

    pub async fn changed(&mut self) -> Option<NodeStatistics> {
        if self.offline {
            None
        } else if self.receiver.changed().await.is_err() {
            self.latest();
            None
        } else {
            Some(self.latest())
        }
    }

    pub fn latest(&mut self) -> NodeStatistics {
        let statistics = self.receiver.borrow_and_update().clone();
        self.offline = statistics.offline;
        statistics
    }
}

pub trait NodeLease: Send {}

pub trait NodeRegistry: Send + Sync {
    fn node_id(&self) -> &NodeId;

    fn select_node(&self) -> Option<(NodeId, OwnedEndpoint)>;

    fn get_node_endpoint(&self, node_id: &NodeId) -> Option<OwnedEndpoint>;

    fn watch_statistics(&self) -> NodeStatisticsWatcher;
}

pub struct EtcdNodeLease {
    lease: DropOwner,
}

impl NodeLease for EtcdNodeLease {}

impl EtcdNodeLease {
    pub fn new(lease: EtcdLease) -> Self {
        Self { lease: lease.into_keep_alive() }
    }

    pub fn watch(&self) -> DropWatcher {
        self.lease.watch()
    }
}

impl Default for EtcdNodeLease {
    fn default() -> Self {
        let (drop_owner, _drop_watcher) = utils::drop_watcher();
        Self { lease: drop_owner }
    }
}

pub struct EtcdNodeRegistry {
    node: NodeId,
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    tree: String,
    nodes: Mutex<HashMap<NodeId, OwnedEndpoint>>,
    statistics: watch::Sender<NodeStatistics>,
}

impl State {
    fn apply_update(&self, message: WatchResponse) -> Result<()> {
        for event in message.events() {
            match event.event_type() {
                EventType::Put => {
                    let kv = event.kv().unwrap();
                    let key = kv.key_str()?;
                    let value = kv.value_str()?;
                    let node_id = key
                        .strip_prefix(&self.tree)
                        .ok_or_else(|| anyhow!("BUG: expect child of {}, got {}", self.tree, key))?;
                    let node = NodeId(node_id.to_string());
                    let endpoint = Endpoint::try_from(value)?;
                    let mut nodes = self.nodes.lock().unwrap();
                    nodes.insert(node, endpoint.to_owned());
                    self.statistics.send(NodeStatistics::new(nodes.len())).ignore();
                },
                EventType::Delete => {
                    let kv = event.kv().unwrap();
                    let key = kv.key_str()?;
                    let node_id = key
                        .strip_prefix(&self.tree)
                        .ok_or_else(|| anyhow!("BUG: expect child of {}, got {}", self.tree, key))?;
                    let node = NodeId(node_id.to_string());
                    let mut nodes = self.nodes.lock().unwrap();
                    nodes.remove(&node);
                    self.statistics.send(NodeStatistics::new(nodes.len())).ignore();
                },
            }
        }
        Ok(())
    }
}

impl NodeRegistry for EtcdNodeRegistry {
    fn node_id(&self) -> &NodeId {
        &self.node
    }

    fn select_node(&self) -> Option<(NodeId, OwnedEndpoint)> {
        let r = rand::random::<usize>();
        let nodes = self.state.nodes.lock().unwrap();
        let n = nodes.len();
        if n == 0 {
            return None;
        }
        let i = r % n;
        let (node, addr) = nodes.iter().nth(i).unwrap();
        Some((node.clone(), addr.clone()))
    }

    fn get_node_endpoint(&self, node_id: &NodeId) -> Option<OwnedEndpoint> {
        let nodes = self.state.nodes.lock().unwrap();
        nodes.get(node_id).cloned()
    }

    fn watch_statistics(&self) -> NodeStatisticsWatcher {
        NodeStatisticsWatcher::new(self.state.statistics.subscribe())
    }
}

impl EtcdNodeRegistry {
    async fn get_nodes(
        client: &mut Client,
        tree: String,
        revision: i64,
    ) -> Result<(HashMap<NodeId, OwnedEndpoint>, i64)> {
        let options = GetOptions::new().with_prefix().with_revision(revision);
        let prefix_len = tree.len();
        let response = client.get(tree, Some(options)).await?;
        let mut nodes = HashMap::new();
        for kv in response.kvs() {
            let key = kv.key_str()?;
            let value = kv.value_str()?;
            let endpoint = Endpoint::try_from(value)?;
            nodes.insert(NodeId(key[prefix_len..].to_string()), endpoint.to_owned());
        }
        Ok((nodes, revision))
    }

    async fn watch_nodes(client: &mut Client, tree: String, start_revision: i64) -> Result<(Watcher, WatchStream)> {
        let options = WatchOptions::new().with_prefix().with_start_revision(start_revision);
        Ok(client.watch(tree, Some(options)).await?)
    }

    async fn register_node(
        client: &mut Client,
        lease_id: i64,
        tree: &str,
        node: &NodeId,
        addr: Endpoint<'_>,
    ) -> Result<ResponseHeader> {
        let key = format!("{}{}", tree, node);
        let options = PutOptions::new().with_lease(lease_id);
        let mut response = client.put(key, addr.to_string(), Some(options)).await?;
        Ok(response.take_header().unwrap())
    }

    async fn update_nodes(
        state: Arc<State>,
        mut dropper: DropWatcher,
        mut watcher: Watcher,
        mut stream: WatchStream,
    ) -> Result<()> {
        defer! {
            let mut nodes = state.nodes.lock().unwrap();
            nodes.clear();
            drop(nodes);
            state.statistics.send(NodeStatistics::new_offline()).ignore();
        }
        loop {
            select! {
                _ = dropper.dropped() => {
                    watcher.cancel().await?;
                },
                r = stream.message() => match r? {
                    None => break,
                    Some(message) => if message.watch_id() == watcher.watch_id() {
                        if message.canceled() {
                            break;
                        }
                        state.apply_update(message)?;
                    },
                }
            }
        }
        Ok(())
    }

    pub async fn join(
        uri: ServiceUri,
        node: NodeId,
        addr: Option<OwnedEndpoint>,
    ) -> Result<(Arc<dyn NodeRegistry>, Box<dyn NodeLease>)> {
        let (resource_id, params) = uri.into();
        let mut client = EtcdHelper::connect(resource_id.endpoint(), &params).await?;

        let tree = format!("{}/nodes/", resource_id.path());
        let ((nodes, revision), lease) = if let Some(addr) = addr {
            let lease = EtcdHelper::grant_lease(&mut client, None).await?;
            let header = Self::register_node(&mut client, lease.id(), &tree, &node, addr.as_ref()).await?;
            let revision = header.revision();
            (Self::get_nodes(&mut client, tree.clone(), revision).await?, EtcdNodeLease::new(lease))
        } else {
            (Self::get_nodes(&mut client, tree.clone(), 0).await?, EtcdNodeLease::default())
        };

        let (watcher, stream) = Self::watch_nodes(&mut client, tree.clone(), revision + 1).await?;
        let (statistics, _) = watch::channel(NodeStatistics::new(nodes.len()));

        let state = Arc::new(State { tree, nodes: Mutex::new(nodes), statistics });

        tokio::spawn(Self::update_nodes(state.clone(), lease.watch(), watcher, stream));

        Ok((Arc::new(EtcdNodeRegistry { node, state }), Box::new(lease)))
    }
}

#[cfg(test)]
mod tests {

    use assertor::*;

    use crate::cluster::etcd::tests::*;
    use crate::cluster::{EtcdNodeRegistry, NodeId, NodeStatistics};
    use crate::endpoint::*;

    #[tokio::test]
    async fn test_etcd_node_registry() {
        let etcd = etcd_container();
        let cluster = etcd.uri();

        let node1 = NodeId::new_random();
        let endpoint1 = Endpoint::try_from("tcp://192.168.0.1").unwrap();
        let (registry1, lease1) =
            EtcdNodeRegistry::join(cluster.clone(), node1.clone(), Some(endpoint1.to_owned())).await.unwrap();

        assert_that!(registry1.get_node_endpoint(&node1).unwrap()).is_equal_to(endpoint1.to_owned());
        assert_that!(registry1.select_node()).is_equal_to(Some((node1.clone(), endpoint1.to_owned())));

        let mut statistics1 = registry1.watch_statistics();
        assert_that!(statistics1.latest()).is_equal_to(NodeStatistics::new(1));

        let node2 = NodeId::new_random();
        let endpoint2 = Endpoint::try_from("tcp://192.168.0.2").unwrap();
        let (registry2, lease2) =
            EtcdNodeRegistry::join(cluster.clone(), node2.clone(), Some(endpoint2.to_owned())).await.unwrap();

        let mut statistics2 = registry2.watch_statistics();
        assert_that!(statistics2.latest()).is_equal_to(NodeStatistics::new(2));

        assert_that!(statistics1.changed().await.unwrap()).is_equal_to(NodeStatistics::new(2));
        assert_that!(statistics1.latest()).is_equal_to(NodeStatistics::new(2));

        drop(lease1);

        assert_that!(statistics2.changed().await.unwrap()).is_equal_to(NodeStatistics::new(1));
        assert_that!(statistics2.latest()).is_equal_to(NodeStatistics::new(1));

        while let Some(_) = statistics1.changed().await {}
        assert_that!(statistics1.latest()).is_equal_to(NodeStatistics::new_offline());

        drop(lease2);

        while let Some(_) = statistics2.changed().await {}
        assert_that!(statistics2.latest()).is_equal_to(NodeStatistics::new_offline());
    }
}
