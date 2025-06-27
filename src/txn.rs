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
use std::cmp::Ordering::*;
use std::collections::{HashMap, HashSet};
use std::pin::{pin, Pin};
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use asyncs::select;
use asyncs::sync::{Notified, Notify};
use asyncs::task::TaskHandle;
use ignore_result::Ignore;
use lazy_init::Lazy;
use tracing::trace;

use crate::kv::{KvClient, KvError, KvSemantics};
use crate::protos::{
    HasTxnMeta,
    HasTxnStatus,
    KeySpan,
    Temporal,
    Timestamp,
    TimestampedKeyValue,
    Transaction,
    TxnStatus,
    Value,
};
use crate::tablet::TabletClient;
use crate::timer::Timer;

#[derive(Clone, Debug)]
struct TimestampKeySpan {
    ts: Timestamp,
    span: KeySpan,
}

struct TxnState {
    txn: Transaction,
    scan_set: Vec<TimestampKeySpan>,
    read_set: HashMap<Vec<u8>, Timestamp>,
    write_set: HashSet<Vec<u8>>,
    epoch: u32,
    sequence: u32,
}

impl TxnState {
    pub fn new(txn: Transaction) -> Self {
        Self {
            txn,
            scan_set: Default::default(),
            read_set: Default::default(),
            write_set: Default::default(),
            epoch: 0,
            sequence: 0,
        }
    }

    fn check_txn_status(&self) -> Result<()> {
        match self.txn.status {
            TxnStatus::Aborted => Err(KvError::TxnAborted { txn_id: self.txn.id(), epoch: self.txn.epoch() }),
            TxnStatus::Committed => Err(KvError::TxnCommitted { txn_id: self.txn.id(), epoch: self.txn.epoch() }),
            TxnStatus::Pending => Ok(()),
        }
    }

    fn update(&mut self, txn: &Transaction) -> Result<()> {
        self.txn.update(txn);
        self.check()?;
        Ok(())
    }

    fn update_read(&mut self, txn: &Transaction, key: &[u8]) -> Result<()> {
        self.txn.update(txn);
        self.check()?;
        self.add_read(key);
        Ok(())
    }

    fn update_scan(&mut self, txn: &Transaction, start: &[u8], end: &[u8]) -> Result<()> {
        if end.is_empty() {
            return self.update_read(txn, start);
        }
        self.txn.update(txn);
        self.check()?;
        self.add_scan(start, end);
        Ok(())
    }

    fn update_write(&mut self, txn: &Transaction, key: &[u8]) -> Result<()> {
        self.txn.update(txn);
        self.check()?;
        self.add_write(key);
        Ok(())
    }

    fn check(&self) -> Result<()> {
        self.check_txn_status()?;
        if self.txn.epoch() > self.epoch {
            return Err(KvError::TxnRestarted {
                txn_id: self.txn.id(),
                from_epoch: self.epoch,
                to_epoch: self.txn.epoch(),
            });
        }
        Ok(())
    }

    fn for_write(&mut self) -> Result<(u32, Transaction)> {
        self.check()?;
        self.sequence += 1;
        Ok((self.sequence, self.txn.clone()))
    }

    fn for_read(&self) -> Result<(u32, Transaction)> {
        self.check()?;
        Ok((self.sequence, self.txn.clone()))
    }

    pub fn add_read<'a>(&mut self, key: impl Into<Cow<'a, [u8]>>) {
        let key = key.into();
        if !self.write_set.contains(key.as_ref()) {
            self.read_set.insert(key.into_owned(), self.txn.commit_ts());
        }
    }

    pub fn add_scan(&mut self, start: &[u8], end: &[u8]) {
        self.scan_set.push(TimestampKeySpan { ts: self.txn.commit_ts(), span: KeySpan::new_range(start, end) });
        self.scan_set.sort_by(|a, b| a.span.cmp(&b.span));
        let mut i = 1;
        while i < self.scan_set.len() {
            let previous = unsafe { &mut *self.scan_set.as_mut_ptr().wrapping_add(i - 1) };
            let current = unsafe { &mut *self.scan_set.as_mut_ptr().wrapping_add(i) };
            if previous.span.end <= current.span.key {
                i += 1;
                continue;
            }
            let mut current = self.scan_set.remove(i);
            match (
                previous.span.key == current.span.key,
                previous.span.end.cmp(&current.span.end),
                previous.ts.cmp(&current.ts),
            ) {
                (true, Equal, Less) => previous.ts = current.ts,
                (true, Equal, _) => {},
                (true, Less, Less | Equal) => {
                    previous.ts = current.ts;
                    previous.span.end = current.span.end;
                },
                (true, Less, Greater) => {
                    current.span.key.clone_from(&previous.span.end);
                    self.scan_set.insert(i, current);
                    i += 1;
                },
                (true, Greater, Equal | Greater) => {},
                (true, Greater, Less) => {
                    std::mem::swap(&mut previous.ts, &mut current.ts);
                    std::mem::swap(&mut previous.span.end, &mut current.span.end);
                    current.span.key.clone_from(&previous.span.end);
                    self.scan_set.insert(i, current);
                    i += 1;
                },
                (false, Equal, Less) => {
                    previous.span.end.clone_from(&current.span.key);
                    self.scan_set.insert(i, current);
                    i += 1;
                },
                (false, Equal, _) => {},
                (false, Less, Equal | Greater) => {
                    current.span.key.clone_from(&previous.span.end);
                    self.scan_set.insert(i, current);
                    i += 1;
                },
                (false, Less, Less) => {
                    previous.span.end.clone_from(&current.span.key);
                    self.scan_set.insert(i, current);
                    i += 1;
                },
                (false, Greater, Less) => {
                    let next = TimestampKeySpan {
                        ts: previous.ts,
                        span: KeySpan { key: current.span.end.clone(), end: std::mem::take(&mut previous.span.end) },
                    };
                    previous.span.end = current.span.key.clone();
                    self.scan_set.insert(i, current);
                    self.scan_set.insert(i + 1, next);
                    i += 2;
                },
                (false, Greater, Equal | Greater) => {},
            }
        }
    }

    pub fn add_write(&mut self, key: &[u8]) {
        match self.read_set.remove_entry(key) {
            Some((key, _value)) => self.write_set.insert(key),
            None => self.write_set.insert(key.to_owned()),
        };
    }

    pub fn for_abort(&self) -> Transaction {
        let mut txn = self.txn.clone();
        txn.abort();
        txn.commit_set = self.write_set.iter().map(|key| KeySpan::new_key(key.to_owned())).collect();
        txn
    }

    pub fn restart(&mut self, commit_ts: Timestamp) {
        if self.epoch == self.txn.epoch() {
            self.txn.restart();
        }
        self.epoch = self.txn.epoch();
        self.sequence = 0;
        self.read_set.clear();
        self.write_set.clear();
        self.txn.meta.commit_ts = commit_ts;
    }

    pub fn outdated_reads(&self) -> Vec<TimestampKeySpan> {
        let commit_ts = self.txn.commit_ts();
        let mut read_spans =
            self.scan_set.iter().filter(|TimestampKeySpan { ts, .. }| *ts < commit_ts).cloned().collect::<Vec<_>>();

        self.read_set.iter().filter(|(_key, ts)| **ts < commit_ts).for_each(|(key, ts)| {
            read_spans.push(TimestampKeySpan { span: KeySpan::new_key(key.clone()), ts: *ts });
        });
        read_spans
    }

    pub fn read_refreshes_for_commit(&self) -> Result<(Transaction, Vec<TimestampKeySpan>)> {
        self.check()?;
        let mut txn = self.txn.clone();
        let outdated_reads = self.outdated_reads();
        if outdated_reads.is_empty() {
            txn.status = TxnStatus::Committed;
            txn.commit_set = self.write_set.iter().map(|key| KeySpan::new_key(key.to_owned())).collect();
        }
        Ok((txn, outdated_reads))
    }
}

struct TrackingTxn {
    state: RwLock<TxnState>,
    notify: Notify,
}

impl TrackingTxn {
    pub fn new(txn: Transaction) -> Self {
        Self { notify: Notify::new(), state: RwLock::new(TxnState::new(txn)) }
    }

    fn update(&self, txn: &Transaction) -> Result<()> {
        let mut state = self.state.write().unwrap();
        state.update(txn)?;
        self.notify.notify_all();
        Ok(())
    }

    fn update_read(&self, txn: &Transaction, key: &[u8]) -> Result<()> {
        let mut state = self.state.write().unwrap();
        state.update_read(txn, key)?;
        self.notify.notify_all();
        Ok(())
    }

    fn update_scan(&self, txn: &Transaction, start: &[u8], end: &[u8]) -> Result<()> {
        let mut state = self.state.write().unwrap();
        state.update_scan(txn, start, end)?;
        self.notify.notify_all();
        Ok(())
    }

    fn update_write(&self, txn: &Transaction, key: &[u8]) -> Result<()> {
        let mut state = self.state.write().unwrap();
        state.update_write(txn, key)?;
        self.notify.notify_all();
        Ok(())
    }

    fn check_notified(&self) -> Result<Box<Notified>> {
        let state = self.state.read().unwrap();
        state.check()?;
        let mut notified = Box::new(self.notify.notified());
        Pin::new(notified.as_mut()).enable();
        Ok(notified)
    }

    fn write(&self) -> Result<(u32, Transaction, Box<Notified>)> {
        let mut state = self.state.write().unwrap();
        let (sequence, txn) = state.for_write()?;
        let mut notified = Box::new(self.notify.notified());
        Pin::new(notified.as_mut()).enable();
        Ok((sequence, txn.clone(), notified))
    }

    fn read(&self) -> Result<(u32, Transaction, Box<Notified>)> {
        let state = self.state.read().unwrap();
        let (sequence, txn) = state.for_read()?;
        let mut notified = Box::new(self.notify.notified());
        Pin::new(notified.as_mut()).enable();
        Ok((sequence, txn.clone(), notified))
    }

    fn restart(&self, commit_ts: Timestamp) {
        self.state.write().unwrap().restart(commit_ts);
    }

    pub fn bump_commit_ts(&self, commit_ts: Timestamp) {
        self.state.write().unwrap().txn.meta.commit_ts = commit_ts;
    }

    pub fn read_refreshes_for_commit(&self) -> Result<(Transaction, Vec<TimestampKeySpan>)> {
        self.state.read().unwrap().read_refreshes_for_commit()
    }

    pub fn for_abort(&self) -> Transaction {
        self.state.read().unwrap().for_abort()
    }

    pub fn txn(&self) -> Transaction {
        self.state.read().unwrap().txn.clone()
    }

    pub fn commit_ts(&self) -> Timestamp {
        self.state.read().unwrap().txn.commit_ts()
    }
}

pub struct LazyInitTxn {
    client: TabletClient,
    txn: Lazy<Txn>,
}

impl LazyInitTxn {
    pub fn new(client: TabletClient) -> Self {
        Self { client, txn: Lazy::new() }
    }

    fn txn(&self, key: &[u8]) -> &Txn {
        self.txn.get_or_create(|| Txn::new(self.client.clone(), key.to_owned()))
    }
}

#[async_trait::async_trait]
impl KvClient for LazyInitTxn {
    fn client(&self) -> &TabletClient {
        &self.client
    }

    fn semantics(&self) -> KvSemantics {
        KvSemantics::Transactional
    }

    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>> {
        self.txn(&key).get(key).await
    }

    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        self.txn(&start).scan(start, end, limit).await
    }

    async fn put(&self, key: Cow<'_, [u8]>, value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<Timestamp> {
        self.txn(&key).put(key, value, expect_ts).await
    }

    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64> {
        self.txn(&key).increment(key, increment).await
    }

    async fn commit(&self) -> Result<Timestamp> {
        self.txn.get().unwrap().commit().await
    }

    async fn abort(&self) -> Result<()> {
        self.txn.get().unwrap().abort().await
    }

    fn restart(&self) -> Result<()> {
        self.txn.get().unwrap().restart()
    }
}

pub struct Txn {
    txn: Arc<TrackingTxn>,
    client: TabletClient,
    #[allow(unused)]
    heartbeating_task: Option<TaskHandle<()>>,
}

type Result<T, E = KvError> = std::result::Result<T, E>;

impl Txn {
    pub fn new(client: TabletClient, key: impl Into<Vec<u8>>) -> Self {
        let txn = client.new_transaction(key.into());
        let (txn, heartbeating_task) = Self::start_heartbeat_task(client.clone(), txn.clone());
        Self { txn, client, heartbeating_task: Some(heartbeating_task) }
    }

    pub fn client(&self) -> &TabletClient {
        &self.client
    }

    async fn heartbeat(client: TabletClient, tracking_txn: Arc<TrackingTxn>, mut txn: Transaction) {
        let mut timer = Timer::after(Transaction::HEARTBEAT_INTERVAL / 2);
        loop {
            timer.await;

            loop {
                if let Ok(updated_txn) = client.heartbeat_txn(txn.clone()).await {
                    txn.update(&updated_txn);
                    break;
                }
                Timer::after(Transaction::HEARTBEAT_INTERVAL / 8).await;
            }
            tracking_txn.update(&txn).ignore();
            if txn.status().is_terminal() {
                break;
            }
            timer = Timer::after(Transaction::HEARTBEAT_INTERVAL / 2);
        }
    }

    fn start_heartbeat_task(client: TabletClient, txn: Transaction) -> (Arc<TrackingTxn>, TaskHandle<()>) {
        let tracking_txn = Arc::new(TrackingTxn::new(txn.clone()));
        let task = asyncs::spawn(Self::heartbeat(client, tracking_txn.clone(), txn)).attach();
        (tracking_txn, task)
    }

    pub fn bump_commit_ts(&self) {
        self.txn.bump_commit_ts(self.client.now());
    }

    async fn refresh_reads(&self) -> Result<Transaction> {
        loop {
            let (txn, mut outdated_reads) = self.txn.read_refreshes_for_commit()?;
            if outdated_reads.is_empty() {
                return Ok(txn);
            }
            let commit_ts = txn.commit_ts();
            while let Some(TimestampKeySpan { span, ts }) = outdated_reads.pop() {
                if span.end.is_empty() {
                    trace!("refreshing read for key {:?} from {ts} to {commit_ts}", span.key);
                } else {
                    trace!("refreshing read for span {:?} from {ts} to {commit_ts}", span);
                }
                let (temporal, response) = self.client.refresh_read(txn.clone().into(), span.clone(), ts).await?;
                let txn = temporal.into_transaction();
                match response {
                    None => self.txn.update(&txn)?,
                    Some(resume_key) if resume_key.is_empty() => self.txn.update_scan(&txn, &span.key, &span.end)?,
                    Some(resume_key) => self.txn.update_scan(&txn, &span.key, &resume_key)?,
                }
            }
        }
    }

    pub fn txn(&self) -> Transaction {
        self.txn.txn()
    }

    pub fn commit_ts(&self) -> Timestamp {
        self.txn.commit_ts()
    }
}

#[async_trait::async_trait]
impl KvClient for Txn {
    fn client(&self) -> &TabletClient {
        &self.client
    }

    fn semantics(&self) -> KvSemantics {
        KvSemantics::Transactional
    }

    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>> {
        let (sequence, txn, mut notified) = self.txn.read()?;
        let get = self.client.get_directly(txn.into(), key.clone(), sequence);
        let mut get = pin!(get);
        loop {
            select! {
                _ = notified => notified = self.txn.check_notified()?,
                r = get.as_mut() => {
                    let (temporal, response) = r?;
                    let txn = match temporal {
                        Temporal::Transaction(txn) => txn,
                        Temporal::Timestamp(ts) => return Err(KvError::unexpected(format!("txn get received timestamp piggybacked temporal {ts}"))),
                    };
                    self.txn.update_read(&txn, key.as_ref())?;
                    return match response {
                        None => Err(KvError::unexpected("txn get receives no response")),
                        Some(response) => Ok(response.value.map(|v| v.into_parts())),
                    };
                }
            }
        }
    }

    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>)> {
        let (_sequence, txn, mut notified) = self.txn.read()?;
        let scan = self.client.scan_directly(txn.into(), start.clone(), end.clone(), limit);
        let mut scan = pin!(scan);
        loop {
            select! {
                _ = notified => notified = self.txn.check_notified()?,
                r = scan.as_mut() => {
                    let (temporal, response) = r?;
                    let txn = match temporal {
                        Temporal::Transaction(txn) => txn,
                        Temporal::Timestamp(ts) => return Err(KvError::unexpected(format!("txn scan received timestamp piggybacked temporal {ts}"))),
                    };
                    return match response {
                        None => {
                            self.txn.update(&txn)?;
                            Err(KvError::unexpected("txn scan receives no response"))
                        },
                        Some((resume_key, rows)) => {
                            if resume_key.is_empty() || resume_key.as_slice() >= end.as_ref() {
                                self.txn.update_scan(&txn, start.as_ref(), end.as_ref())?;
                            } else {
                                self.txn.update_scan(&txn, start.as_ref(), &resume_key)?;
                            }
                            Ok((resume_key, rows))
                        }
                    };
                }
            }
        }
    }

    async fn put(&self, key: Cow<'_, [u8]>, value: Option<Value>, expect_ts: Option<Timestamp>) -> Result<Timestamp> {
        let (sequence, txn, mut notified) = self.txn.write()?;
        let put = self.client.put_directly(txn.into(), key.clone(), value, expect_ts, sequence);
        let mut put = pin!(put);
        loop {
            select! {
                _ = notified => notified = self.txn.check_notified()?,
                r = put.as_mut() => {
                    let (temporal, response) = r?;
                    let txn = match temporal {
                        Temporal::Transaction(txn) => txn,
                        Temporal::Timestamp(ts) => return Err(KvError::unexpected(format!("txn put received timestamp piggybacked temporal {ts}"))),
                    };
                    self.txn.update_write(&txn, key.as_ref())?;
                    return match response {
                        None => return Err(KvError::unexpected("txn put receives no response")),
                        Some(ts) => Ok(ts),
                    };
                }
            }
        }
    }

    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64> {
        let (sequence, txn, mut notified) = self.txn.write()?;
        let increment = self.client.increment_directly(txn.into(), key.clone(), increment, sequence);
        let mut increment = pin!(increment);
        loop {
            select! {
                _ = notified => notified = self.txn.check_notified()?,
                r = increment.as_mut() => {
                    let (temporal, response) = r?;
                    let txn = match temporal {
                        Temporal::Transaction(txn) => txn,
                        Temporal::Timestamp(ts) => return Err(KvError::unexpected(format!("txn increment received timestamp piggybacked temporal {ts}"))),
                    };
                    self.txn.update_write(&txn, key.as_ref())?;
                    return match response {
                        None => return Err(KvError::unexpected("txn put receives no response")),
                        Some(incremented) => Ok(incremented),
                    };
                }
            }
        }
    }

    async fn commit(&self) -> Result<Timestamp> {
        loop {
            let txn = self.refresh_reads().await?;
            let txn = self.client.heartbeat_txn(txn).await?;
            match self.txn.update(&txn) {
                Err(KvError::TxnCommitted { .. }) => break,
                Err(err) => return Err(err),
                Ok(()) => {},
            }
        }
        Ok(self.commit_ts())
    }

    async fn abort(&self) -> Result<()> {
        let txn = self.client.heartbeat_txn(self.txn.for_abort()).await?;
        match self.txn.update(&txn) {
            Ok(()) => Err(KvError::Internal(anyhow!("txn not aborted: {:?}", self.txn()))),
            Err(KvError::TxnAborted { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn restart(&self) -> Result<()> {
        self.txn.restart(self.client.now());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::time::Duration;

    use assertor::*;
    use tokio::net::TcpListener;

    use super::Txn;
    use crate::cluster::tests::etcd_container;
    use crate::cluster::{ClusterEnv, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
    use crate::endpoint::Endpoint;
    use crate::keys;
    use crate::kv::KvClientExt;
    use crate::log::{LogManager, MemoryLogFactory};
    use crate::protos::{HasTxnMeta, TxnStatus, Value};
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
        let log_manager = LogManager::from(MemoryLogFactory);
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env);
        tokio::time::sleep(Duration::from_secs(20)).await;

        client.put(keys::user_key(b"counter1"), Some(Value::Int(1)), None).await.unwrap();

        let txn = Txn::new(client.clone(), keys::user_key(b"counter1")).wrapped();
        txn.put(&keys::system_key(b"counter0"), Some(Value::Int(10)), None).await.unwrap();
        txn.put(&keys::user_key(b"counter1"), None, None).await.unwrap();
        txn.increment(&keys::user_key(b"counter2"), 100).await.unwrap();
        txn.commit().await.unwrap();
        let commit_ts = txn.wrapped().txn().commit_ts();

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
        let log_manager = LogManager::from(MemoryLogFactory);
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
        let txn = Txn::new(client.clone(), counter_key.clone()).wrapped();

        assert!(client.get(&counter_key).await.unwrap().is_none());

        let write_ts = txn.wrapped().txn().commit_ts();
        txn.put(&counter_key, Some(Value::Int(1)), None).await.unwrap();
        let written_ts = txn.wrapped().txn().commit_ts();
        assert_that!(written_ts).is_greater_than(write_ts);

        txn.commit().await.unwrap();

        let (read_ts, read_value) = client.get(counter_key).await.unwrap().unwrap();
        assert_that!(read_ts).is_equal_to(txn.wrapped().txn().commit_ts());
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
        let log_manager = LogManager::from(MemoryLogFactory);
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
        let txn = Txn::new(client.clone(), counter_key.clone()).wrapped();

        let tablet_id_counter_key = keys::system_key(b"tablet-id-counter");

        let (_ts, tablet_id_value) = txn.get(&tablet_id_counter_key).await.unwrap().unwrap();

        client.increment(&tablet_id_counter_key, 100).await.unwrap();

        txn.wrapped().bump_commit_ts();
        txn.put(&counter_key, Some(tablet_id_value), None).await.unwrap();
        eprintln!("EEEE: {:?}", txn.commit().await.unwrap_err());

        // Same to above except no write-to-read.
        let (_ts, tablet_id_value) = txn.get(&tablet_id_counter_key).await.unwrap().unwrap();

        txn.wrapped().bump_commit_ts();
        txn.put(&counter_key, Some(tablet_id_value.clone()), None).await.unwrap();
        txn.commit().await.unwrap();

        let (ts, value) = client.get(counter_key).await.unwrap().unwrap();
        assert_that!(ts).is_equal_to(txn.wrapped().txn().commit_ts());
        assert_that!(value).is_equal_to(tablet_id_value);
    }

    #[test_log::test(tokio::test)]
    #[tracing_test::traced_test]
    async fn txn_read_refresh_scan() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::from(MemoryLogFactory);
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(keys::USER_KEY_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let txn = Txn::new(client.clone(), b"counter".to_vec()).wrapped();
        let mut write = false;
        let sum = 'sum: loop {
            let mut sum = 0;
            let mut start = Cow::Borrowed(b"counter".as_slice());
            while !start.is_empty() {
                let (resume_key, rows) = txn.scan(start, b"countes", 0).await.unwrap();
                for row in rows {
                    if let Value::Int(v) = row.value {
                        sum += v;
                    }
                }
                start = Cow::Owned(resume_key);
                if !write {
                    client.increment(b"counter0", 10).await.unwrap();
                    client.increment(b"counter1", 100).await.unwrap();
                    write = true;
                }
                txn.wrapped().bump_commit_ts();
                match txn.commit().await {
                    Ok(_) => break 'sum sum,
                    Err(_) => continue,
                }
            }
        };
        assert_eq!(sum, 110);
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
        let log_manager = LogManager::from(MemoryLogFactory);
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(keys::USER_KEY_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let key = b"counter";
        let txn = Txn::new(client.clone(), key.to_vec()).wrapped();

        txn.put(key, Some(Value::Int(1)), None).await.unwrap();

        let commit_ts = txn.wrapped().txn().commit_ts();
        tokio::time::sleep(Duration::from_secs(30)).await;
        txn.commit().await.unwrap();

        assert_that!(txn.wrapped().txn().status).is_equal_to(TxnStatus::Committed);
        assert_that!(txn.wrapped().txn().commit_ts()).is_greater_than(commit_ts);
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
        let log_manager = LogManager::from(MemoryLogFactory);
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
        let txn = Txn::new(client.clone(), key.clone()).wrapped();
        txn.put(&key, Some(Value::Int(1)), None).await.unwrap();
        txn.abort().await.unwrap();

        assert_eq!(client.get(&key).await.unwrap(), None);
    }
}
