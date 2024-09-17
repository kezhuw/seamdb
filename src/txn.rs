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
use std::collections::{HashMap, HashSet};
use std::pin::pin;

use anyhow::anyhow;
use asyncs::select;
use asyncs::sync::watch;
use asyncs::task::TaskHandle;
use thiserror::Error;
use tracing::trace;

use crate::protos::{
    BatchRequest,
    DataRequest,
    HasTxnMeta,
    HasTxnStatus,
    KeySpan,
    RefreshReadRequest,
    ShardRequest,
    Temporal,
    Timestamp,
    Transaction,
    TxnMeta,
    TxnStatus,
    Uuid,
    Value,
};
use crate::tablet::{TabletClient, TabletClientError};
use crate::timer::Timer;

struct WritingTxn {
    txn: Transaction,
    read_set: HashMap<Vec<u8>, Timestamp>,
    write_set: HashSet<Vec<u8>>,
    epoch: u32,
    sequence: u32,
}

impl WritingTxn {
    pub fn new(txn: Transaction) -> Self {
        Self { txn, read_set: Default::default(), write_set: Default::default(), epoch: 0, sequence: 0 }
    }

    fn check_txn_status(&self) -> Result<()> {
        match self.txn.status {
            TxnStatus::Aborted => Err(TxnError::TxnAborted { txn_id: self.txn.id(), epoch: self.txn.epoch() }),
            TxnStatus::Committed => Err(TxnError::TxnCommitted { txn_id: self.txn.id(), epoch: self.txn.epoch() }),
            TxnStatus::Pending => Ok(()),
        }
    }

    fn check_txn(&self, epoch: u32) -> Result<()> {
        self.check_txn_status()?;
        if self.txn.epoch() > epoch {
            return Err(TxnError::TxnRestarted {
                txn_id: self.txn.id(),
                from_epoch: epoch,
                to_epoch: self.txn.epoch(),
            });
        }
        Ok(())
    }

    fn update_and_check(&mut self, txn: &Transaction) -> Result<()> {
        let epoch = self.txn.epoch();
        self.txn.update(txn);
        self.check_txn(epoch)?;
        Ok(())
    }

    pub fn add_read<'a>(&mut self, key: impl Into<Cow<'a, [u8]>>) {
        let key = key.into();
        if !self.write_set.contains(key.as_ref()) {
            self.read_set.insert(key.into_owned(), self.txn.commit_ts());
        }
    }

    pub fn add_write(&mut self, key: &[u8]) {
        match self.read_set.remove_entry(key) {
            Some((key, _value)) => self.write_set.insert(key),
            None => self.write_set.insert(key.to_owned()),
        };
    }

    pub fn txn(&self) -> &Transaction {
        &self.txn
    }

    pub fn committing_txn(&self) -> Transaction {
        let mut txn = self.txn.clone();
        txn.status = TxnStatus::Committed;
        txn.commit_set = self.write_set.iter().map(|key| KeySpan::new_key(key.to_owned())).collect();
        txn
    }

    pub fn aborting_txn(&self) -> Transaction {
        let mut txn = self.txn.clone();
        txn.abort();
        txn.commit_set = self.write_set.iter().map(|key| KeySpan::new_key(key.to_owned())).collect();
        txn
    }

    fn read_sequence(&self) -> u32 {
        self.sequence
    }

    fn write_sequence(&self) -> u32 {
        self.sequence + 1
    }

    fn bump_sequence(&mut self) {
        self.sequence += 1;
    }

    pub fn restart(&mut self) {
        if self.epoch == self.txn.epoch() {
            self.txn.restart();
        }
        self.epoch = self.txn.epoch();
        self.sequence = 0;
        self.read_set.clear();
        self.write_set.clear();
    }

    pub fn outdated_reads(&self) -> Vec<(Vec<u8>, Timestamp)> {
        let commit_ts = self.txn.commit_ts();
        self.read_set.iter().filter(|(_key, ts)| **ts < commit_ts).map(|(key, ts)| (key.to_owned(), *ts)).collect()
    }
}

impl HasTxnMeta for WritingTxn {
    fn meta(&self) -> &TxnMeta {
        self.txn.meta()
    }
}

pub struct Txn {
    txn: WritingTxn,
    client: TabletClient,
    heartbeating_txn: watch::Receiver<Transaction>,
    heartbeating_task: Option<TaskHandle<()>>,
}

#[derive(Debug, Error)]
pub enum TxnError {
    #[error("{0}")]
    ClientError(#[from] TabletClientError),
    #[error("txn {txn_id} restarted from epoch {from_epoch} to {to_epoch}")]
    TxnRestarted { txn_id: Uuid, from_epoch: u32, to_epoch: u32 },
    #[error("txn {txn_id} aborted in epoch {epoch}")]
    TxnAborted { txn_id: Uuid, epoch: u32 },
    #[error("txn {txn_id} already committed with epoch {epoch}")]
    TxnCommitted { txn_id: Uuid, epoch: u32 },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<tonic::Status> for TxnError {
    fn from(status: tonic::Status) -> Self {
        Self::from(TabletClientError::from(status))
    }
}

type Result<T, E = TxnError> = std::result::Result<T, E>;

impl Txn {
    pub fn new(client: TabletClient, key: impl Into<Vec<u8>>) -> Self {
        let txn = client.new_transaction(key.into());
        let (heartbeating_txn, heartbeating_task) = Self::start_heartbeat_task(client.clone(), txn.clone());
        Self { txn: WritingTxn::new(txn), client, heartbeating_txn, heartbeating_task: Some(heartbeating_task) }
    }

    async fn heartbeat_once(client: &TabletClient, txn: Transaction) -> Result<Transaction> {
        let (shard, mut service) = client.service(txn.key()).await?;
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

    async fn heartbeat(client: TabletClient, mut txn: Transaction, sender: watch::Sender<Transaction>) {
        let mut timer = Timer::after(Transaction::HEARTBEAT_INTERVAL / 2);
        loop {
            timer.await;

            match Self::heartbeat_once(&client, txn.clone()).await {
                Err(_) => {},
                Ok(updated_txn) => txn.update(&updated_txn),
            }

            loop {
                if let Ok(updated_txn) = Self::heartbeat_once(&client, txn.clone()).await {
                    txn.update(&updated_txn);
                    break;
                }
                Timer::after(Transaction::HEARTBEAT_INTERVAL / 8).await;
            }
            if sender.send(txn.clone()).is_err() || txn.status().is_terminal() {
                break;
            }
            timer = Timer::after(Transaction::HEARTBEAT_INTERVAL / 2);
        }
    }

    fn start_heartbeat_task(client: TabletClient, txn: Transaction) -> (watch::Receiver<Transaction>, TaskHandle<()>) {
        let (sender, receiver) = watch::channel(txn.clone());
        let task = asyncs::spawn(Self::heartbeat(client, txn, sender)).attach();
        (receiver, task)
    }

    fn sync_heartbeating_txn(&mut self) -> Result<()> {
        let txn = self.heartbeating_txn.borrow_and_update();
        if txn.has_changed() {
            self.txn.update_and_check(&txn)?;
        }
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<(Timestamp, Value)>> {
        self.sync_heartbeating_txn()?;
        let mut get = self.client.transactional_get(self.txn.txn().clone(), key, self.txn.read_sequence());
        let mut get = pin!(get);
        loop {
            select! {
                r = self.heartbeating_txn.changed() => match r {
                    Err(_) => return Err(TxnError::Internal(anyhow!("txn {} heartbeat stopped", self.txn.id()))),
                    Ok(txn) => {
                        self.txn.update_and_check(&txn)?;
                    }
                },
                r = get.as_mut() => match r {
                    Err(err) => return Err(TxnError::from(err)),
                    Ok((txn, value)) => {
                        self.txn.update_and_check(&txn)?;
                        self.txn.add_read(key);
                        return Ok(value.map(|v| (v.timestamp, v.value)));
                    }
                }
            }
        }
    }

    pub async fn put(&mut self, key: &[u8], value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<()> {
        self.sync_heartbeating_txn()?;
        let mut put =
            self.client.transactional_put(self.txn.txn().clone(), key, value, self.txn.write_sequence(), expect_ts);
        let mut put = pin!(put);
        loop {
            select! {
                r = self.heartbeating_txn.changed() => match r {
                    Err(_) => return Err(TxnError::Internal(anyhow!("txn {} heartbeat stopped", self.txn.id()))),
                    Ok(txn) => {
                        self.txn.update_and_check(&txn)?;
                    }
                },
                r = put.as_mut() => match r {
                    Err(err) => return Err(TxnError::from(err)),
                    Ok(txn) => {
                        self.txn.update_and_check(&txn)?;
                        self.txn.bump_sequence();
                        self.txn.add_write(key);
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn increment(&mut self, key: &[u8], increment: i64) -> Result<i64> {
        self.sync_heartbeating_txn()?;
        let mut increment =
            self.client.transactional_increment(self.txn.txn().clone(), key, increment, self.txn.write_sequence());
        let mut increment = pin!(increment);
        loop {
            select! {
                r = self.heartbeating_txn.changed() => match r {
                    Err(_) => return Err(TxnError::Internal(anyhow!("txn {} heartbeat stopped", self.txn.id()))),
                    Ok(txn) => {
                        self.txn.update_and_check(&txn)?;
                    }
                },
                r = increment.as_mut() => match r {
                    Err(err) => return Err(TxnError::from(err)),
                    Ok((txn, incremented)) => {
                        self.txn.update_and_check(&txn)?;
                        self.txn.bump_sequence();
                        self.txn.add_write(key);
                        return Ok(incremented);
                    }
                }
            }
        }
    }

    pub fn restart(&mut self) {
        self.txn.restart();
        self.bump_commit_ts();
    }

    pub fn bump_commit_ts(&mut self) {
        self.txn.txn.commit_ts = self.client.now();
    }

    async fn refresh_reads(&mut self) -> Result<()> {
        self.sync_heartbeating_txn()?;
        loop {
            let commit_ts = self.txn.txn().commit_ts();
            let mut outdated_reads = self.txn.outdated_reads();
            while let Some((key, ts)) = outdated_reads.pop() {
                let (shard, mut service) = self.client.service(&key).await?;
                trace!("refreshing read for key {key:?} from {ts} to {commit_ts}");
                let requests = vec![ShardRequest {
                    shard_id: shard.shard_id().into(),
                    request: DataRequest::RefreshRead(RefreshReadRequest {
                        span: KeySpan::new_key(key.clone()),
                        from: ts,
                    }),
                }];
                let response = service
                    .batch(BatchRequest {
                        tablet_id: shard.tablet_id().into(),
                        temporal: Temporal::Transaction(self.txn.txn().clone()),
                        requests,
                        ..Default::default()
                    })
                    .await?
                    .into_inner();
                let txn = response.temporal.into_transaction();
                self.txn.update_and_check(&txn)?;
                self.txn.add_read(key);
            }
            self.sync_heartbeating_txn()?;
            if self.txn.txn().commit_ts() <= commit_ts {
                break;
            }
        }
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        loop {
            self.refresh_reads().await?;
            let txn = Self::heartbeat_once(&self.client, self.txn.committing_txn()).await?;
            match self.txn.update_and_check(&txn) {
                Err(TxnError::TxnCommitted { .. }) => break,
                Err(err) => return Err(err),
                Ok(()) => {},
            }
        }
        self.heartbeating_task = None;
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<()> {
        let txn = Self::heartbeat_once(&self.client, self.txn.aborting_txn()).await?;
        match self.txn.update_and_check(&txn) {
            Ok(()) => Err(TxnError::Internal(anyhow!("txn not aborted: {:?}", self.txn.txn()))),
            Err(TxnError::TxnAborted { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn txn(&self) -> &Transaction {
        self.txn.txn()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assertor::*;
    use tokio::net::TcpListener;

    use super::Txn;
    use crate::cluster::tests::etcd_container;
    use crate::cluster::{ClusterEnv, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
    use crate::endpoint::{Endpoint, Params};
    use crate::keys;
    use crate::log::{LogManager, MemoryLogFactory};
    use crate::protos::{TxnStatus, Value};
    use crate::tablet::{TabletClient, TabletNode};

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn txn_write_multiple() {
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

        client.put(keys::user_key(b"counter1"), Value::Int(1), None).await.unwrap();

        let mut txn = Txn::new(client.clone(), keys::user_key(b"counter1"));
        txn.put(&keys::system_key(b"counter0"), Some(Value::Int(10)), None).await.unwrap();
        txn.put(&keys::user_key(b"counter1"), None, None).await.unwrap();
        txn.increment(&keys::user_key(b"counter2"), 100).await.unwrap();
        txn.commit().await.unwrap();
        let commit_ts = txn.txn().commit_ts();

        assert_that!(client.get(keys::system_key(b"counter0")).await.unwrap().unwrap())
            .is_equal_to((commit_ts, Value::Int(10)));
        assert_that!(client.get(keys::user_key(b"counter1")).await.unwrap()).is_equal_to(None);
        assert_that!(client.get(keys::user_key(b"counter2")).await.unwrap().unwrap())
            .is_equal_to((commit_ts, Value::Int(100)));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn txn_write_push_forward() {
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
        let mut txn = Txn::new(client.clone(), counter_key.clone());

        assert!(client.get(&counter_key).await.unwrap().is_none());

        let write_ts = txn.txn().commit_ts();
        txn.put(&counter_key, Some(Value::Int(1)), None).await.unwrap();
        let written_ts = txn.txn().commit_ts();
        assert_that!(written_ts).is_greater_than(write_ts);

        txn.commit().await.unwrap();

        let (read_ts, read_value) = client.get(counter_key).await.unwrap().unwrap();
        assert_that!(read_ts).is_equal_to(txn.txn().commit_ts());
        assert_that!(read_value).is_equal_to(Value::Int(1));
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn txn_read_refresh() {
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
        let mut txn = Txn::new(client.clone(), counter_key.clone());

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");

        let (_ts, tablet_id_value) = txn.get(&tablet_id_counter_key).await.unwrap().unwrap();

        client.increment(&tablet_id_counter_key, 100).await.unwrap();

        txn.bump_commit_ts();
        txn.put(&counter_key, Some(tablet_id_value), None).await.unwrap();
        txn.commit().await.unwrap_err();

        // Same to above except no write-to-read.
        let (_ts, tablet_id_value) = txn.get(&tablet_id_counter_key).await.unwrap().unwrap();

        txn.bump_commit_ts();
        txn.put(&counter_key, Some(tablet_id_value.clone()), None).await.unwrap();
        txn.commit().await.unwrap();

        let (ts, value) = client.get(counter_key).await.unwrap().unwrap();
        assert_that!(ts).is_equal_to(txn.txn().commit_ts());
        assert_that!(value).is_equal_to(tablet_id_value);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn txn_commit_push_forward() {
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

        let key = keys::user_key(b"counter");
        let mut txn = Txn::new(client.clone(), key.clone());

        txn.put(&key, Some(Value::Int(1)), None).await.unwrap();

        let commit_ts = txn.txn().commit_ts();
        tokio::time::sleep(Duration::from_secs(30)).await;
        txn.commit().await.unwrap();

        assert_that!(txn.txn().status).is_equal_to(TxnStatus::Committed);
        assert_that!(txn.txn().commit_ts()).is_greater_than(commit_ts);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn txn_abort() {
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

        let key = keys::user_key(b"counter");
        let mut txn = Txn::new(client.clone(), key.clone());
        txn.put(&key, Some(Value::Int(1)), None).await.unwrap();
        txn.abort().await.unwrap();

        assert_eq!(client.get(&key).await.unwrap(), None);
    }
}
