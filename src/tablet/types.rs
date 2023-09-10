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

use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::{bail, Result};
use ignore_result::Ignore;
use tokio::sync::{oneshot, watch};

pub use crate::clock::Timestamp;
pub use crate::protos::{self, MessageId, TabletWatermark};
use crate::protos::{BatchRequest, BatchResponse, HeartbeatResponse, TabletDeployment, TabletRange};

pub enum Temporal {
    Timestamp(Timestamp),
    Transaction(Transaction),
}

impl Temporal {
    pub fn timestamp(&self) -> Timestamp {
        match self {
            Temporal::Timestamp(ts) => *ts,
            Temporal::Transaction(txn) => txn.meta.timestamp(),
        }
    }

    pub fn update_timestamp(&mut self, new_ts: Timestamp) -> Timestamp {
        match self {
            Temporal::Timestamp(ts) => std::mem::replace(ts, new_ts),
            Temporal::Transaction(txn) => {
                let old_ts = txn.meta.timestamp();
                txn.meta.commit_ts = Some(new_ts);
                old_ts
            },
        }
    }
}

pub type TxnStatus = protos::TxnStatus;

#[derive(Clone, Debug)]
pub struct TxnMeta {
    pub id: Vec<u8>,
    pub key: Vec<u8>,
    pub epoch: u32,
    pub sequence: u32,
    pub create_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    pub meta: TxnMeta,
    pub status: TxnStatus,
    pub write_set: Vec<protos::Span>,
}

#[derive(Clone, Debug)]
pub struct TxnIntent {
    pub txn: TxnMeta,

    pub value: Value,

    pub history: Vec<protos::txn_intent::Intent>,
}

impl TxnIntent {
    pub fn new(txn: TxnMeta, value: Value) -> Self {
        Self { txn, value, history: Default::default() }
    }

    pub fn same_txn(&self, txn: &TxnMeta) -> bool {
        self.txn.id == txn.id && self.txn.key == txn.key
    }

    pub fn belongs(&self, txn: &TxnMeta) -> bool {
        self.txn.id == txn.id && self.txn.key == txn.key
    }

    pub fn get_value(&self, epoch: u32, sequence: u32) -> Option<(u32, Value)> {
        if epoch != self.txn.epoch {
            // For dated epoch, we may not have enough information to retrieve correct data, and a
            // None should not harm clients.
            // For newer epoch, we are dated, and None is the correct answer.
            return None;
        }
        if sequence >= self.txn.sequence {
            return Some((self.txn.sequence, self.value.clone()));
        }
        for intent in self.history.iter() {
            if sequence >= intent.sequence {
                return Some((intent.sequence, intent.value.clone().into()));
            }
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, bytemuck::NoUninit)]
#[repr(i32)]
pub enum ReplicationStage {
    #[default]
    Pending,
    Replicated,
    Failed,
}

#[derive(Debug)]
pub struct ReplicationMonitor {
    current: atomic::Atomic<ReplicationStage>,
    receiver: watch::Receiver<ReplicationStage>,
}

impl ReplicationMonitor {
    pub fn stage(&self) -> ReplicationStage {
        self.current.load(atomic::Ordering::Relaxed)
    }

    fn update(&self, stage: ReplicationStage) {
        self.current.store(stage, atomic::Ordering::Relaxed)
    }

    /// Wait till terminal stage.
    pub async fn wait(&self) -> ReplicationStage {
        let stage = self.stage();
        if stage != ReplicationStage::Pending {
            return stage;
        }
        let mut receiver = self.receiver.clone();
        loop {
            let stage = *receiver.borrow_and_update();
            if stage != ReplicationStage::Pending {
                self.update(stage);
                return stage;
            }
            if receiver.changed().await.is_err() {
                self.update(ReplicationStage::Failed);
                return ReplicationStage::Failed;
            }
        }
    }
}

pub struct ReplicationHandle {
    sender: watch::Sender<ReplicationStage>,
}

#[derive(Clone, Debug, Default)]
pub struct Value {
    pub value: Option<protos::Value>,
    replication: Option<Arc<ReplicationMonitor>>,
}

impl Value {
    pub fn new(value: Option<protos::Value>) -> (Self, ReplicationHandle) {
        let (sender, receiver) = watch::channel(ReplicationStage::Pending);
        let listener = ReplicationMonitor { current: atomic::Atomic::new(ReplicationStage::Pending), receiver };
        let handle = ReplicationHandle { sender };
        let value = Value { value, replication: Some(Arc::new(listener)) };
        (value, handle)
    }

    pub fn replicate(self) -> (Self, ReplicationHandle) {
        let (sender, receiver) = watch::channel(ReplicationStage::Pending);
        let listener = ReplicationMonitor { current: atomic::Atomic::new(ReplicationStage::Pending), receiver };
        let handle = ReplicationHandle { sender };
        let value = Self { value: self.value, replication: Some(Arc::new(listener)) };
        (value, handle)
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub fn correct_timestamp(&self, ts: Timestamp) -> Timestamp {
        if self.is_tombstone() {
            Timestamp::default()
        } else {
            ts
        }
    }

    pub fn replication(&self) -> (ReplicationStage, Option<&Arc<ReplicationMonitor>>) {
        match &self.replication {
            None => (ReplicationStage::Replicated, None),
            Some(replication) => match replication.stage() {
                ReplicationStage::Pending => (ReplicationStage::Pending, Some(replication)),
                stage => (stage, None),
            },
        }
    }

    pub fn read_bytes(&self, key: &[u8], operation: &str) -> Result<&[u8]> {
        let Some(v) = &self.value else {
            return Ok(Default::default());
        };
        v.read_bytes(key, operation)
    }

    pub fn read_int(&self, key: &[u8], operation: &str) -> Result<i64> {
        let Some(v) = &self.value else {
            return Ok(0);
        };
        v.read_int(key, operation)
    }
}

impl From<protos::Value> for Value {
    fn from(v: protos::Value) -> Self {
        Self { value: Some(v), replication: None }
    }
}

impl From<Option<protos::Value>> for Value {
    fn from(value: Option<protos::Value>) -> Self {
        Self { value, replication: None }
    }
}

impl From<Value> for Option<protos::Value> {
    fn from(v: Value) -> Self {
        v.value
    }
}

impl From<protos::TxnIntent> for TxnIntent {
    fn from(metadata: protos::TxnIntent) -> Self {
        Self { txn: metadata.txn.unwrap().into(), value: metadata.value.into(), history: metadata.history }
    }
}

impl From<TxnIntent> for protos::TxnIntent {
    fn from(intent: TxnIntent) -> Self {
        Self { txn: Some(intent.txn.into()), value: intent.value.into(), history: intent.history }
    }
}

impl Transaction {
    pub fn sort_spans(&mut self) {
        self.write_set.sort_by(|left, right| left.key.cmp(&right.key))
    }

    pub fn timestamp(&self) -> Timestamp {
        if let Some(t) = self.meta.commit_ts {
            t
        } else {
            self.meta.create_ts
        }
    }
}

impl TxnMeta {
    pub fn timestamp(&self) -> Timestamp {
        if let Some(t) = self.commit_ts {
            t
        } else {
            self.create_ts
        }
    }

    pub fn is_same(&self, other: &TxnMeta) -> bool {
        self.id == other.id && self.key == other.key
    }
}

impl From<protos::TxnMeta> for Transaction {
    fn from(meta: protos::TxnMeta) -> Self {
        Self { meta: meta.into(), status: TxnStatus::Pending, write_set: Default::default() }
    }
}

impl From<Transaction> for protos::TxnMeta {
    fn from(txn: Transaction) -> Self {
        let meta: TxnMeta = txn.into();
        meta.into()
    }
}

impl From<TxnMeta> for Transaction {
    fn from(meta: TxnMeta) -> Self {
        Self { meta, status: TxnStatus::Pending, write_set: Default::default() }
    }
}

impl From<Transaction> for TxnMeta {
    fn from(txn: Transaction) -> Self {
        txn.meta
    }
}

impl From<TxnMeta> for protos::TxnMeta {
    fn from(meta: TxnMeta) -> Self {
        Self {
            id: meta.id,
            key: meta.key,
            epoch: meta.epoch,
            sequence: meta.sequence,
            create_ts: meta.create_ts,
            commit_ts: meta.commit_ts,
        }
    }
}

impl From<protos::TxnMeta> for TxnMeta {
    fn from(meta: protos::TxnMeta) -> Self {
        Self {
            id: meta.id,
            key: meta.key,
            epoch: meta.epoch,
            sequence: meta.sequence,
            create_ts: meta.create_ts,
            commit_ts: meta.commit_ts,
        }
    }
}

impl From<Transaction> for protos::Transaction {
    fn from(txn: Transaction) -> Self {
        Self {
            id: txn.meta.id,
            key: txn.meta.key,
            epoch: txn.meta.epoch,
            sequence: txn.meta.sequence,
            create_ts: txn.meta.create_ts,
            commit_ts: txn.meta.commit_ts,
            status: txn.status,
            write_set: txn.write_set,
            fence_duration: 0,
        }
    }
}

impl TryFrom<protos::Transaction> for Transaction {
    type Error = anyhow::Error;

    fn try_from(txn: protos::Transaction) -> Result<Self, Self::Error> {
        let create_ts = txn.create_ts;
        let commit_ts = txn.commit_ts;
        if let Some(commit_ts) = commit_ts.filter(|ts| *ts < create_ts) {
            bail!("commit_ts({}) < create_ts({})", commit_ts, create_ts)
        }
        Ok(Self {
            meta: TxnMeta { id: txn.id, key: txn.key, epoch: txn.epoch, sequence: txn.sequence, create_ts, commit_ts },
            status: txn.status,
            write_set: txn.write_set,
        })
    }
}

impl From<Timestamp> for Temporal {
    fn from(t: Timestamp) -> Self {
        Temporal::Timestamp(t)
    }
}

impl TryFrom<protos::Temporal> for Temporal {
    type Error = anyhow::Error;

    fn try_from(t: protos::Temporal) -> Result<Self, Self::Error> {
        match t {
            protos::Temporal::Timestamp(timestamp) => Ok(Temporal::Timestamp(timestamp)),
            protos::Temporal::Transaction(transaction) => Ok(Temporal::Transaction(transaction.try_into()?)),
        }
    }
}

impl From<Temporal> for protos::Temporal {
    fn from(t: Temporal) -> Self {
        match t {
            Temporal::Timestamp(timestamp) => protos::Temporal::Timestamp(timestamp),
            Temporal::Transaction(txn) => protos::Temporal::Transaction(txn.into()),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TimestampedValue {
    pub value: Value,
    pub timestamp: Timestamp,
}

impl TimestampedValue {
    pub fn new(timestamp: Timestamp, value: Value) -> TimestampedValue {
        TimestampedValue { timestamp, value }
    }

    pub fn adjust_timestamp(self) -> TimestampedValue {
        let timestamp = if self.value.is_tombstone() { Timestamp::default() } else { self.timestamp };
        TimestampedValue { value: self.value, timestamp }
    }
}

impl From<TimestampedValue> for Option<protos::TimestampedValue> {
    fn from(value: TimestampedValue) -> Option<protos::TimestampedValue> {
        if let Some(raw_value) = value.value.value {
            Some(protos::TimestampedValue { value: raw_value, timestamp: value.timestamp })
        } else {
            None
        }
    }
}

#[derive(Default)]
pub struct ReplicationTracker {
    stage: ReplicationStage,
    replications: Vec<watch::Sender<ReplicationStage>>,
}

impl ReplicationTracker {
    pub fn batch(&mut self, handle: ReplicationHandle) {
        assert_eq!(self.stage, ReplicationStage::Pending);
        self.replications.push(handle.sender);
    }

    pub fn fail(&mut self) {
        assert_eq!(self.stage, ReplicationStage::Pending);
        self.complete(ReplicationStage::Failed)
    }

    pub fn commit(&mut self) {
        assert_eq!(self.stage, ReplicationStage::Pending);
        self.complete(ReplicationStage::Replicated)
    }

    fn complete(&mut self, stage: ReplicationStage) {
        self.stage = stage;
        for sender in self.replications.drain(..) {
            sender.send(stage).ignore();
        }
    }
}

impl Drop for ReplicationTracker {
    fn drop(&mut self) {
        if self.stage == ReplicationStage::Pending {
            self.fail();
        }
    }
}

#[derive(Default)]
pub struct ReplicationWatcher {
    stage: ReplicationStage,
    replications: Vec<Arc<ReplicationMonitor>>,
}

impl ReplicationWatcher {
    pub fn is_empty(&self) -> bool {
        self.replications.is_empty()
    }

    pub fn batch(&mut self, replication: &Arc<ReplicationMonitor>) {
        self.replications.push(replication.clone());
    }

    pub fn watch(&mut self, value: &Value) -> ReplicationStage {
        match value.replication() {
            (ReplicationStage::Pending, Some(replication)) => self.batch(replication),
            (ReplicationStage::Failed, _) => self.stage = ReplicationStage::Failed,
            _ => {},
        }
        self.stage
    }

    pub async fn wait(&mut self) -> ReplicationStage {
        if self.stage != ReplicationStage::Pending {
            return self.stage;
        }
        while let Some(replication) = self.replications.pop() {
            let stage = replication.wait().await;
            if stage == ReplicationStage::Failed {
                self.stage = ReplicationStage::Failed;
                return ReplicationStage::Failed;
            }
        }
        self.stage = ReplicationStage::Replicated;
        ReplicationStage::Replicated
    }
}

pub struct BatchResult {
    pub ts: Timestamp,

    pub blocker: ReplicationWatcher,
    pub responses: Vec<protos::DataResponse>,

    pub writes: Vec<protos::Write>,
    pub replication: ReplicationTracker,
}

impl BatchResult {
    pub fn take_writes(&mut self) -> Vec<protos::Write> {
        std::mem::take(&mut self.writes)
    }
}

pub trait TabletStore: Send + Sync {
    fn cursor(&self) -> MessageId;

    fn watermark(&self) -> &TabletWatermark;

    fn update_watermark(&mut self, watermark: TabletWatermark);

    fn get(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue>;

    fn find(&self, key: &[u8], temporal: &Temporal) -> Result<(Vec<u8>, TimestampedValue)>;

    fn apply(&mut self, message: protos::DataMessage) -> Result<()>;

    fn batch(&mut self, temporal: &mut Temporal, writes: Vec<protos::DataRequest>) -> Result<BatchResult>;
}

pub enum TabletRequest {
    Batch { batch: BatchRequest, responser: oneshot::Sender<Result<BatchResponse>> },
    Deploy { epoch: u64, generation: u64, servers: Vec<String> },
}

pub enum TabletServiceRequest {
    ListTablets { range: TabletRange, responser: oneshot::Sender<Vec<TabletDeployment>> },
    DeployTablet { deployment: TabletDeployment, responser: oneshot::Sender<Result<Vec<TabletDeployment>>> },
    Batch { batch: BatchRequest, responser: oneshot::Sender<Result<BatchResponse>> },
    UnloadTablet { deployment: TabletDeployment },
    HeartbeatTablet { tablet_id: u64, responser: oneshot::Sender<HeartbeatResponse> },
}
