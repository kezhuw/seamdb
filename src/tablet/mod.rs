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

mod client;
mod concurrency;
mod deployer;
mod file;
mod loader;
mod memory;
mod node;
mod provision;
mod server;
mod service;
mod store;
mod types;

pub use self::client::{TabletClient, TabletClientError};
pub use self::concurrency::{LockTable, Request, TxnTable};
pub use self::deployer::{TabletDeployServant, TabletDeployer};
pub use self::loader::{
    FollowingTablet,
    FollowingTabletManifest,
    FollowingTabletStore,
    LeadingTablet,
    LeadingTabletManifest,
    LeadingTabletStore,
    LogMessageConsumer,
    TabletLoader,
};
pub use self::node::TabletNode;
pub use self::provision::{ReplicationStage, ReplicationTracker, TimestampedValue};
pub use self::server::TabletServiceImpl;
pub use self::store::BatchResult;
pub use crate::protos::{
    MessageId,
    TabletDeployment,
    TabletDescription,
    TabletDescriptor,
    TabletManifest,
    TabletWatermark,
};

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::stream::{FuturesUnordered, StreamExt};
    use ignore_result::Ignore;
    use tokio::net::TcpListener;

    use crate::cluster::tests::etcd_container;
    use crate::cluster::{ClusterEnv, ClusterMetaHandle, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
    use crate::endpoint::{Endpoint, OwnedServiceUri};
    use crate::fs::{FileSystemManager, MemoryFileSystemFactory};
    use crate::keys;
    use crate::kv::KvClientExt;
    use crate::log::{LogManager, MemoryLogFactory};
    use crate::protos::Value;
    use crate::tablet::{TabletClient, TabletNode};
    use crate::txn::Txn;

    async fn read_keys(client: &TabletClient, prefix: String, n: usize) {
        for i in 0..n {
            let mut key = keys::user_key(prefix.as_bytes());
            write!(&mut key, "_{}", i).ignore();
            let (_ts, value) = client.get(key.clone()).await.unwrap().unwrap();
            let int = value.read_int(key.as_slice(), "read").unwrap();
            assert_eq!(i, int as usize);
        }
    }

    async fn write_keys(client: TabletClient, txn: bool, prefix: String, n: usize) {
        if txn {
            write_txn_keys(client, prefix, n).await
        } else {
            write_ts_keys(client, prefix, n).await
        }
    }

    async fn write_ts_keys(client: TabletClient, prefix: String, n: usize) {
        let mut futures = FuturesUnordered::new();
        for i in 0..n {
            let mut key = keys::user_key(prefix.as_bytes());
            write!(&mut key, "_{}", i).ignore();
            futures.push(client.put(key, Some(Value::Int(i as i64)), None));
        }
        while let Some(result) = futures.next().await {
            result.unwrap();
        }
    }

    async fn write_txn_keys(client: TabletClient, prefix: String, n: usize) {
        let prefix = prefix.into_bytes();
        let txn = Txn::new(client, keys::user_key(&prefix)).wrapped();
        let mut futures = FuturesUnordered::new();
        for i in 0..n {
            let mut key = keys::user_key(&prefix);
            write!(&mut key, "_{}", i).ignore();
            futures.push(txn.put(key, Some(Value::Int(i as i64)), None));
        }
        while let Some(result) = futures.next().await {
            result.unwrap();
        }
        txn.commit().await.unwrap();
    }

    async fn start_tablet_node(
        cluster_uri: OwnedServiceUri,
        log_manager: Arc<LogManager>,
        fs_manager: Arc<FileSystemManager>,
    ) -> (Box<dyn ClusterMetaHandle>, TabletNode, TabletClient) {
        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let cluster_env =
            ClusterEnv::new(log_manager, fs_manager, nodes).with_replicas(1).with_tablet_compaction_messages(200);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env.clone());
        (cluster_meta_handle, node, client)
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn tablet_compact_files() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let log_manager =
            Arc::new(LogManager::new(MemoryLogFactory::new(), &MemoryLogFactory::URI.into()).await.unwrap());
        let fs_manager = Arc::new(FileSystemManager::from(MemoryFileSystemFactory));

        let (node, daemon, client) =
            start_tablet_node(cluster_uri.clone(), log_manager.clone(), fs_manager.clone()).await;

        tokio::time::sleep(Duration::from_secs(20)).await;

        let mut futures = FuturesUnordered::new();
        let n = client.env().tablet_compaction_messages() + 100;
        for i in 0.. {
            if i * 10 >= n {
                break;
            }
            let prefix = format!("keys_{}", i);
            futures.push(write_keys(client.clone(), i % 2 == 0, prefix, 10));
        }
        while futures.next().await.is_some() {}
        tokio::time::sleep(Duration::from_secs(10)).await;

        drop(futures);
        drop(node);
        drop(client);
        drop(daemon);

        let (_daemon, _node1, client1) = start_tablet_node(cluster_uri.clone(), log_manager, fs_manager).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
        for i in 0.. {
            if i * 10 >= n {
                break;
            }
            let prefix = format!("keys_{}", i);
            read_keys(&client1, prefix, 10).await;
        }
    }
}
