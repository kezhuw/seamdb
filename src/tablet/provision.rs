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

use std::cell::UnsafeCell;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{bail, Result};
use ignore_result::Ignore;
use tokio::sync::watch;

use crate::protos::{self, HasTxnMeta, KeySpan, Timestamp, Transaction, TxnMeta};

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

impl ReplicationHandle {
    pub fn new() -> (Self, Arc<ReplicationMonitor>) {
        let (sender, receiver) = watch::channel(ReplicationStage::Pending);
        let listener = ReplicationMonitor { current: atomic::Atomic::new(ReplicationStage::Pending), receiver };
        let handle = Self { sender };
        (handle, Arc::new(listener))
    }
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

    pub fn new_replicated(value: Option<protos::Value>) -> Self {
        Self { value, replication: None }
    }

    pub fn new_replicating(value: Option<protos::Value>, replication: Arc<ReplicationMonitor>) -> Self {
        Self { value, replication: Some(replication) }
    }

    pub fn replicate(self) -> (Self, ReplicationHandle) {
        Self::new(self.value)
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

#[derive(Clone, Debug, Default)]
pub struct TimestampedValue {
    pub value: Value,
    pub timestamp: Timestamp,
}

impl TimestampedValue {
    pub fn new(timestamp: Timestamp, value: Value) -> TimestampedValue {
        TimestampedValue { timestamp, value }
    }

    pub fn into_client(self) -> TimestampedValue {
        let timestamp = self.client_ts();
        TimestampedValue { value: self.value, timestamp }
    }

    pub fn client_ts(&self) -> Timestamp {
        if self.value.is_tombstone() {
            Timestamp::ZERO
        } else {
            self.timestamp
        }
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

#[derive(Clone, Debug, Default)]
pub struct TimestampedKeyValue {
    pub key: Vec<u8>,
    pub value: TimestampedValue,
}

impl TimestampedKeyValue {
    pub fn new(key: Vec<u8>, value: TimestampedValue) -> Self {
        Self { key, value }
    }
}

impl From<TimestampedKeyValue> for protos::TimestampedKeyValue {
    fn from(value: TimestampedKeyValue) -> protos::TimestampedKeyValue {
        let key = value.key;
        let timestamp = value.value.timestamp;
        let value = value.value.value.value.unwrap();
        protos::TimestampedKeyValue { timestamp, key, value }
    }
}

#[derive(Default, Debug)]
pub struct ReplicationTracker {
    stage: ReplicationStage,
    replications: Vec<watch::Sender<ReplicationStage>>,
}

impl ReplicationTracker {
    pub fn is_empty(&self) -> bool {
        self.replications.is_empty()
    }

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

#[derive(Default, Debug)]
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

#[derive(Clone, Debug)]
pub struct TxnValue {
    pub value: Value,
    pub sequence: u32,
}

impl TxnValue {
    pub fn new(value: Option<protos::Value>, sequence: u32) -> (Self, ReplicationHandle) {
        let (value, handle) = Value::new(value);
        (Self { value, sequence }, handle)
    }

    pub fn new_replicated(value: Option<protos::Value>, sequence: u32) -> Self {
        Self { value: Value::new_replicated(value), sequence }
    }

    pub fn into_timestamped(self) -> TimestampedValue {
        let timestamp = Timestamp::txn_sequence(self.sequence);
        TimestampedValue::new(timestamp, self.value)
    }

    pub fn into_value(self) -> Option<protos::Value> {
        self.value.value
    }
}

#[derive(Clone, Debug)]
pub struct TxnRecord {
    txn: Rc<UnsafeCell<Transaction>>,
}

impl Deref for TxnRecord {
    type Target = Transaction;

    fn deref(&self) -> &Transaction {
        self.get()
    }
}

unsafe impl Send for TxnRecord {}

impl TxnRecord {
    pub fn new(txn: Transaction) -> Self {
        Self { txn: Rc::new(UnsafeCell::new(txn)) }
    }

    pub fn get(&self) -> &Transaction {
        self.get_mut()
    }

    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self) -> &mut Transaction {
        unsafe { &mut *(self.txn.get()) }
    }

    pub fn is_same(&self, other: &TxnRecord) -> bool {
        Rc::ptr_eq(&self.txn, &other.txn)
    }

    pub fn update(&self, other: &Transaction) {
        self.get_mut().update(other)
    }

    pub fn add_write_span(&self, span: KeySpan) {
        self.get_mut().write_set.push(span);
    }

    pub fn take_write_set(&self) -> Vec<KeySpan> {
        std::mem::take(&mut self.get_mut().write_set)
    }
}

#[derive(Clone, Debug)]
pub struct TxnIntent {
    pub txn: TxnRecord,
    pub value: TxnValue,
    pub history: Vec<TxnValue>,
}

impl HasTxnMeta for TxnRecord {
    fn meta(&self) -> &TxnMeta {
        &self.meta
    }
}

impl HasTxnMeta for TxnIntent {
    fn meta(&self) -> &TxnMeta {
        &self.txn.meta
    }
}

impl TxnIntent {
    pub fn new(txn: TxnRecord, value: Option<protos::Value>, sequence: u32) -> (Self, ReplicationHandle) {
        let (value, replication) = TxnValue::new(value, sequence);
        (Self { txn, value, history: Default::default() }, replication)
    }

    pub fn new_replicated(txn: TxnRecord, value: Option<protos::Value>, sequence: u32) -> Self {
        let value = TxnValue::new_replicated(value, sequence);
        Self { txn, value, history: Default::default() }
    }

    pub fn into_latest(self, txn: &Transaction) -> Option<TxnValue> {
        std::iter::once(self.value)
            .chain(self.history.into_iter().rev())
            .find(|value| !txn.is_rollbacked(value.sequence))
    }

    pub fn get_value(&self, sequence: u32) -> Option<TxnValue> {
        if sequence >= self.value.sequence {
            return Some(self.value.clone());
        }
        for value in self.history.iter() {
            if sequence >= value.sequence {
                return Some(value.clone());
            }
        }
        None
    }

    pub fn push(&mut self, value: Option<protos::Value>, sequence: u32) -> Result<ReplicationHandle> {
        let (value, replication) = TxnValue::new(value, sequence);
        self.push_value(value)?;
        Ok(replication)
    }

    pub fn push_replicated(&mut self, value: Option<protos::Value>, sequence: u32) {
        let value = TxnValue::new_replicated(value, sequence);
        self.push_value(value).unwrap();
    }

    fn push_value(&mut self, mut value: TxnValue) -> Result<()> {
        if value.sequence <= self.value.sequence {
            bail!(
                "txn {:?} write(sequence: {}) encounters higher sequence {}",
                self.txn.meta,
                value.sequence,
                self.value.sequence
            );
        }
        std::mem::swap(&mut value, &mut self.value);
        self.history.push(value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::TxnRecord;

    assert_impl_all!(TxnRecord: Send);
    assert_not_impl_any!(TxnRecord: Sync);
}
