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
use std::collections::btree_map::{BTreeMap, Entry as BTreeEntry};
use std::collections::VecDeque;
use std::ops::Bound;

use anyhow::{anyhow, bail, Result};
use hashbrown::hash_map::{Entry as HashEntry, HashMap};
use ignore_result::Ignore;
use tokio::sync::{mpsc, oneshot};
use tracing::instrument;

use super::provision::{
    ReplicationHandle,
    ReplicationTracker,
    ReplicationWatcher,
    TimestampedValue,
    TxnIntent,
    TxnRecord,
    Value,
};
use super::types::StreamingChannel;
use crate::clock::Clock;
use crate::keys::Key;
use crate::protos::{
    self,
    BatchRequest,
    BatchResponse,
    DataOperation,
    DataRequest,
    DataResponse,
    FindResponse,
    GetResponse,
    HasTxnMeta,
    HasTxnStatus,
    IncrementResponse,
    KeyRange,
    KeySpan,
    MessageId,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    PutResponse,
    RefreshReadResponse,
    ShardDescription,
    ShardDescriptor,
    ShardId,
    ShardRequest,
    ShardResponse,
    TabletId,
    TabletWatermark,
    Temporal,
    Timestamp,
    Transaction,
    TxnStatus,
    Uuid,
};
use crate::tablet::concurrency::{Request, TxnTable};
use crate::tablet::memory::{MemoryStore, MemoryTable};
use crate::tablet::TabletClient;

enum Writes {
    Write(protos::Write),
    Batch(Vec<protos::Write>),
}

impl From<Option<DataOperation>> for Writes {
    fn from(operation: Option<DataOperation>) -> Self {
        match operation {
            None => Writes::Batch(Default::default()),
            Some(DataOperation::Write(write)) => Writes::Write(write),
            Some(DataOperation::Batch(mut batch)) => {
                batch.writes.reverse();
                Writes::Batch(batch.writes)
            },
        }
    }
}

impl ExactSizeIterator for Writes {}

impl Iterator for Writes {
    type Item = protos::Write;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::replace(self, Writes::Batch(Default::default())) {
            Writes::Write(write) => Some(write),
            Writes::Batch(mut batch) => {
                let write = batch.pop();
                if !batch.is_empty() {
                    unsafe { std::ptr::write(self, Writes::Batch(batch)) };
                }
                write
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Writes::Write(_) => (1, Some(1)),
            Writes::Batch(writes) => (writes.len(), Some(writes.len())),
        }
    }
}

impl Default for Writes {
    fn default() -> Self {
        Self::Batch(Default::default())
    }
}

#[derive(Debug)]
pub enum BatchResult {
    Read {
        temporal: Temporal,
        responses: Vec<ShardResponse>,
        responser: oneshot::Sender<Result<BatchResponse>>,

        blocker: ReplicationWatcher,
    },
    Write {
        temporal: Temporal,
        responses: Vec<ShardResponse>,
        responser: oneshot::Sender<Result<BatchResponse>>,

        writes: Vec<protos::Write>,
        replication: ReplicationTracker,

        requests: Vec<Request>,
    },
    Error {
        error: anyhow::Error,
        responser: oneshot::Sender<Result<BatchResponse>>,
    },
}

#[derive(Default)]
pub struct BatchContext {
    pub cache: TabletCache,
    pub reads: ReplicationWatcher,
    pub writes: Vec<protos::Write>,
    pub replication: ReplicationTracker,
}

struct ShardStore {
    id: ShardId,
    range: KeyRange,
    store: Box<dyn Store>,
}

impl ShardStore {
    pub fn new(id: ShardId, range: KeyRange) -> Self {
        Self { id, range, store: Box::<MemoryStore>::default() }
    }

    fn put(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()> {
        self.store.put(key, ts, value)
    }

    pub fn get(&self, context: &BatchContext, ts: Timestamp, key: &[u8]) -> Result<TimestampedValue> {
        if let Some((ts, value)) = context.cache.timestamped.get(key, ts) {
            return Ok(TimestampedValue::new(ts, value));
        };
        self.store.get(key, ts)
    }

    pub fn get_timestamped(&self, context: &BatchContext, ts: Timestamp, key: &[u8]) -> Result<TimestampedValue> {
        self.get(context, ts, key).map(|value| value.into_client())
    }

    pub fn find(&self, context: &BatchContext, ts: Timestamp, key: &[u8]) -> Result<(Vec<u8>, TimestampedValue)> {
        if let Some((key, ts, value)) = context.cache.timestamped.find(key, ts) {
            return Ok((key.to_owned(), TimestampedValue::new(ts, value)));
        }
        self.store.find(key, ts)
    }

    pub fn find_timestamped(
        &self,
        context: &BatchContext,
        ts: Timestamp,
        key: &[u8],
    ) -> Result<(Vec<u8>, TimestampedValue)> {
        self.find(context, ts, key).map(|(key, value)| (key, value.into_client()))
    }

    fn check_timestamped_write(&self, context: &BatchContext, ts: Timestamp, key: &[u8]) -> Result<TimestampedValue> {
        let value = self.get(context, ts, key)?;
        // FIXME: allow equal timestamp for now to gain write-your-write.
        if ts < value.timestamp {
            return Err(anyhow!("write@{} encounters newer timestamp {}", ts, value.timestamp));
        }
        Ok(value.into_client())
    }

    fn add_timestamped_write(
        &mut self,
        context: &mut BatchContext,
        ts: Timestamp,
        key: Vec<u8>,
        value: Option<protos::Value>,
    ) {
        let write = protos::Write { key: key.clone(), value: value.clone(), sequence: 0 };
        context.writes.push(write);
        let (value, replication) = Value::new(value);
        context.cache.timestamped.put(key, ts, value);
        context.replication.batch(replication);
    }

    fn put_timestamped(
        &mut self,
        context: &mut BatchContext,
        ts: Timestamp,
        key: &[u8],
        value: Option<protos::Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        let existing_value = self.check_timestamped_write(context, ts, key)?;
        if let Some(expect_ts) = expect_ts.filter(|ts| *ts != existing_value.timestamp) {
            bail!("mismatch timestamp check: existing ts {}, expect ts {:}", existing_value.timestamp, expect_ts)
        }
        self.add_timestamped_write(context, ts, key.to_owned(), value);
        Ok(ts)
    }

    fn increment_timestamped(
        &mut self,
        context: &mut BatchContext,
        ts: Timestamp,
        key: &[u8],
        increment: i64,
    ) -> Result<i64> {
        let value = self.check_timestamped_write(context, ts, key)?;
        let i = value.value.read_int(key, "increment")?;
        let incremented = i + increment;
        let value = protos::Value::Int(incremented);
        self.add_timestamped_write(context, ts, key.to_owned(), Some(value));
        Ok(incremented)
    }

    fn refresh_read_timestamped(
        &self,
        context: &mut BatchContext,
        ts: Timestamp,
        span: KeySpan,
        from: Timestamp,
    ) -> Result<()> {
        if span.end.is_empty() {
            let found = self.get(context, ts, &span.key)?;
            if found.timestamp <= from {
                return Ok(());
            }
            bail!(
                "fail to refresh key {:?} from {} to {} due to write at timestamp {:?}",
                span.key,
                from,
                ts,
                found.timestamp
            );
        }
        let mut start = span.key;
        let end = span.end;
        loop {
            let (key, value) = self.find(context, ts, &start)?;
            if key.is_empty() || key >= end {
                break;
            }
            if value.timestamp > from {
                bail!(
                    "fail to refresh key {:?} from {:?} to {:?} due to write at timestamp {:?}",
                    key,
                    from,
                    ts,
                    value.timestamp
                );
            }
            start.clear();
            start.extend(&key);
            start.push(0);
        }
        Ok(())
    }

    fn add_transactional_write(
        &mut self,
        context: &mut BatchContext,
        txn: &TxnRecord,
        key: Vec<u8>,
        value: Option<protos::Value>,
        sequence: u32,
    ) {
        let write = protos::Write { key: key.clone(), value: value.clone(), sequence };
        context.writes.push(write);
        let (intent, replication) = TxnIntent::new(txn.clone(), value, sequence);
        context.replication.batch(replication);
        context.cache.transactional.insert(key, intent);
    }

    fn update_transactional_write(
        &mut self,
        context: &mut BatchContext,
        mut intent: TxnIntent,
        key: Vec<u8>,
        value: Option<protos::Value>,
        sequence: u32,
    ) -> Result<()> {
        let write = protos::Write { key: key.clone(), value: value.clone(), sequence };
        context.writes.push(write);
        let replication = intent.push(value, sequence)?;
        context.replication.batch(replication);
        context.cache.transactional.insert(key, intent);
        Ok(())
    }

    pub fn get_transactional(
        &mut self,
        context: &BatchContext,
        provision: &TxnProvision,
        txn: &TxnRecord,
        key: &[u8],
        sequence: u32,
    ) -> Result<TimestampedValue> {
        if let Some(intent) = provision.get_intent(context, key) {
            assert!(intent.txn.is_same(txn));
            if let Some(value) = intent.get_value(sequence) {
                return Ok(value.into_timestamped());
            }
            // Sad, the transaction is restarted or fully rollbacked, fallback to timestamped store.
        }
        self.get_timestamped(context, txn.commit_ts(), key)
    }

    pub fn find_transactional(
        &mut self,
        context: &BatchContext,
        provision: &TxnProvision,
        txn: &TxnRecord,
        key: &[u8],
        sequence: u32,
    ) -> Result<(Vec<u8>, TimestampedValue)> {
        let (found, value) = self.find_timestamped(context, txn.commit_ts(), key)?;
        let mut start = key.to_owned();
        let end = found.as_slice();
        while let Some((found, intent)) = provision.find_intent(context, &start, end) {
            if let Some(value) = intent.get_value(sequence) {
                return Ok((found.to_vec(), value.into_timestamped()));
            }
            start.clear();
            start.extend(found);
            start.push(0);
        }
        Ok((found, value))
    }

    pub fn get_txn_latest_value(
        &self,
        context: &BatchContext,
        intent: &TxnIntent,
        key: &[u8],
        sequence: u32,
    ) -> Result<TimestampedValue> {
        if let Some(value) = intent.get_value(sequence) {
            return Ok(value.into_timestamped());
        }
        self.get(context, intent.txn.commit_ts(), key)
    }

    pub fn rewrite_txn_intent<F: FnMut(Option<protos::Value>) -> Result<Option<protos::Value>>>(
        &mut self,
        context: &mut BatchContext,
        intent: TxnIntent,
        key: Vec<u8>,
        value: ValueExtractor<F>,
        sequence: u32,
        existing_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        if sequence <= intent.value.sequence {
            let Some(intent_value) = intent.get_value(sequence) else {
                bail!("txn {:?} key {:?}: can't find written value for old sequence {}", intent.txn, key, sequence);
            };
            let writting = match value {
                ValueExtractor::Plain(value) => value,
                ValueExtractor::Compute(mut compute) => {
                    let value = self.get_txn_latest_value(context, &intent, &key, sequence - 1)?;
                    compute(value.value.value)?
                },
            };
            let written = intent_value.value.value;
            if writting != written {
                bail!(
                    "txn {:?} key {:?}: non idempotent write at sequence {}, old write {:?}, new write {:?}",
                    intent.txn,
                    key,
                    sequence,
                    written,
                    writting
                );
            }
            return Ok(Timestamp::txn_sequence(sequence));
        }
        let existing_value = if existing_ts.is_some() || matches!(value, ValueExtractor::Compute(_)) {
            self.get_txn_latest_value(context, &intent, &key, sequence - 1)?
        } else {
            TimestampedValue::default()
        };
        if let Some(expected_ts) = existing_ts {
            if expected_ts != existing_value.client_ts() {
                bail!(
                    "txn {:?} key {:?}: expect value at {:?}, but got value at {:?}",
                    intent.txn.meta,
                    key,
                    expected_ts,
                    existing_value.client_ts()
                )
            }
        }
        let value = match value {
            ValueExtractor::Plain(value) => value,
            ValueExtractor::Compute(mut compute) => compute(existing_value.value.value)?,
        };
        self.update_transactional_write(context, intent, key, value, sequence)?;
        Ok(Timestamp::txn_sequence(sequence))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_transactional_with_value_extractor<F: FnMut(Option<protos::Value>) -> Result<Option<protos::Value>>>(
        &mut self,
        context: &mut BatchContext,
        provision: &mut TxnProvision,
        txn: &TxnRecord,
        key: Vec<u8>,
        value: ValueExtractor<F>,
        sequence: u32,
        existing_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        if let Some(intent) = provision.get_intent(context, &key) {
            assert!(intent.txn.is_same(txn));
            return self.rewrite_txn_intent(context, intent.clone(), key, value, sequence, existing_ts);
        }
        let commit_ts = txn.commit_ts();
        let found = self.get(context, Timestamp::MAX, &key)?;
        if commit_ts <= found.timestamp {
            return Err(anyhow!(
                "key {:?}: try to write txn at {:?}, but got value at {:?}",
                key,
                commit_ts,
                found.timestamp
            ));
        }
        if let Some(expected_ts) = existing_ts {
            let client_ts = found.client_ts();
            if expected_ts != client_ts {
                return Err(anyhow!("key {:?}: expect timestamp at {:?}, but got {:?}", key, expected_ts, client_ts));
            }
        }
        let value = match value {
            ValueExtractor::Plain(plain) => plain,
            ValueExtractor::Compute(mut compute) => compute(found.value.value)?,
        };
        self.add_transactional_write(context, txn, key, value, sequence);
        Ok(Timestamp::txn_sequence(sequence))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_transactional(
        &mut self,
        context: &mut BatchContext,
        provision: &mut TxnProvision,
        txn: &TxnRecord,
        key: Vec<u8>,
        value: Option<protos::Value>,
        sequence: u32,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        self.put_transactional_with_value_extractor(
            context,
            provision,
            txn,
            key,
            ValueExtractor::<fn(Option<protos::Value>) -> Result<Option<protos::Value>>>::Plain(value),
            sequence,
            expect_ts,
        )
    }

    pub fn increment_transactional(
        &mut self,
        context: &mut BatchContext,
        provision: &mut TxnProvision,
        txn: &TxnRecord,
        key: Vec<u8>,
        increment: i64,
        sequence: u32,
    ) -> Result<i64> {
        let mut incremented = 0;
        let value = ValueExtractor::Compute(|value| {
            let i = match value {
                None => 0,
                Some(value) => value.read_int(&key, "increment")?,
            };
            incremented = i + increment;
            Ok(Some(protos::Value::Int(incremented)))
        });
        self.put_transactional_with_value_extractor(context, provision, txn, key.clone(), value, sequence, None)?;
        Ok(incremented)
    }

    pub fn refresh_read_transactional(
        &mut self,
        context: &BatchContext,
        provision: &mut TxnProvision,
        txn: &TxnRecord,
        span: KeySpan,
        from: Timestamp,
    ) -> Result<()> {
        if span.end.is_empty() {
            if let Some(intent) = provision.get_intent(context, &span.key) {
                if intent.txn.is_same(txn) || intent.txn.commit_ts() > txn.commit_ts() {
                    return Ok(());
                }
                bail!("txn {:?} fail to refresh key {:?} due to write from txn {:?}", txn.meta, span.key, intent.txn);
            }
            let found = self.get(context, txn.commit_ts(), &span.key)?;
            if found.timestamp <= from {
                return Ok(());
            }
            bail!(
                "txn {:?} fail to refresh key {:?} due to write at timestamp {:?}",
                txn.meta,
                span.key,
                found.timestamp
            );
        }
        let mut start = span.key.clone();
        while let Some((found, intent)) = provision.find_intent(context, &start, &span.end) {
            if !intent.txn.is_same(txn) && intent.txn.commit_ts() <= txn.commit_ts() {
                bail!("txn {:?} fail to refresh key {:?} due to write from txn {:?}", txn.meta, span.key, intent.txn);
            }
            start.clear();
            start.extend(found);
            start.push(0);
        }
        start.clear();
        start.extend(&span.key);
        loop {
            let (key, value) = self.find(context, txn.commit_ts(), &start)?;
            if key.is_empty() || key >= span.end {
                break;
            }
            if value.timestamp > from {
                bail!(
                    "txn {:?} fail to refresh span {:?} due to write at timestamp {:?}",
                    txn.meta,
                    span,
                    value.timestamp
                );
            }
            start.clear();
            start.extend(&key);
            start.push(0);
        }
        Ok(())
    }
}

pub enum ValueExtractor<F: FnMut(Option<protos::Value>) -> Result<Option<protos::Value>>> {
    Plain(Option<protos::Value>),
    Compute(F),
}

#[derive(Default)]
pub struct TabletCache {
    timestamped: MemoryTable<Timestamp, Value>,
    transactional: BTreeMap<Key, TxnIntent>,
}

impl TabletCache {
    pub fn clear(&mut self) {
        self.timestamped.clear();
        self.transactional.clear();
    }
}

pub struct DataStore {
    id: TabletId,
    shards: Vec<ShardDescription>,
    stores: Vec<ShardStore>,
}

impl DataStore {
    pub fn new(id: TabletId, shards: Vec<ShardDescription>) -> Self {
        let stores = shards.iter().map(|shard| ShardStore::new(shard.id.into(), shard.range.clone())).collect();
        Self { id, shards, stores }
    }

    fn get_shard_store_mut(&mut self, id: ShardId) -> Option<&mut ShardStore> {
        self.stores.iter_mut().find(|store| store.id == id)
    }

    fn locate_shard_store_mut(&mut self, key: &[u8]) -> Option<&mut ShardStore> {
        self.stores.iter_mut().find(|shard| shard.range.contains(key))
    }

    fn find_shard_store_mut(&mut self, id: ShardId, key: &[u8]) -> Result<&mut ShardStore> {
        if let Some(store) = self.get_shard_store_mut(id) {
            return Ok(unsafe { std::mem::transmute(store) });
        }
        self.locate_shard_store_mut(key).ok_or_else(|| anyhow!("shard {id} not found for key {key:?}"))
    }

    fn put(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()> {
        let Some(store) = self.locate_shard_store_mut(&key) else {
            bail!("key {:?} does not reside in tablet {} with shards {:?}", key, self.id, self.shards)
        };
        store.put(key, ts, value)
    }

    fn put_if_located(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()> {
        let Some(store) = self.locate_shard_store_mut(&key) else { return Ok(()) };
        store.put(key, ts, value)
    }

    fn promote(&mut self, context: &mut BatchContext) -> Result<()> {
        for (key, values) in context.cache.timestamped.take().into_iter() {
            let (ts, value) = values.into_iter().next_back().unwrap();
            self.put(key, ts, value)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct TxnProvision {
    intents: BTreeMap<Key, TxnIntent>,
    transactions: HashMap<Uuid, TxnRecord>,
}

impl TxnProvision {
    fn prepare_txn(&mut self, txn: &Transaction) -> TxnRecord {
        let HashEntry::Occupied(entry) = self.transactions.entry(txn.id()) else {
            return TxnRecord::new(txn.clone());
        };
        if txn.epoch() <= entry.get().epoch() {
            return entry.get().clone();
        }
        let outdated_txn = entry.remove();
        for span in &outdated_txn.write_set {
            self.intents.remove(&span.key);
        }
        TxnRecord::new(txn.clone())
    }

    fn add_txn_writes(&mut self, txn: TxnRecord, writes: Writes) {
        for write in writes {
            match self.intents.entry(write.key) {
                BTreeEntry::Occupied(mut entry) => {
                    let intent = entry.get_mut();
                    intent.push_replicated(write.value, write.sequence);
                },
                BTreeEntry::Vacant(entry) => {
                    let intent = TxnIntent::new_replicated(txn.clone(), write.value, write.sequence);
                    entry.insert(intent);
                },
            }
        }
    }

    fn apply_txn(&mut self, store: &mut DataStore, txn: TxnRecord, writes: Writes) {
        match txn.status {
            TxnStatus::Pending => self.add_txn_writes(txn, writes),
            TxnStatus::Aborted => {
                let id = txn.id();
                let Some(existing_txn) = self.transactions.remove(&id) else {
                    return;
                };
                for span in &existing_txn.write_set {
                    self.intents.remove(&span.key);
                }
                if !txn.commit_set.is_empty() {
                    assert!(txn.write_set.is_empty());
                    self.transactions.insert(id, txn);
                }
            },
            TxnStatus::Committed => {
                self.add_txn_writes(txn.clone(), writes);
                let id = txn.id();
                let commit_ts = txn.commit_ts();
                let Some(existing_txn) = self.transactions.remove(&id) else {
                    return;
                };
                for span in &existing_txn.write_set {
                    let Some((key, intent)) = self.intents.remove_entry(&span.key) else {
                        continue;
                    };
                    let Some(latest) = intent.into_latest(&txn) else {
                        continue;
                    };
                    let value = Value::new_replicated(latest.into_value());
                    store.put(key, commit_ts, value).unwrap();
                }
                if !txn.commit_set.is_empty() {
                    assert!(txn.write_set.is_empty());
                    self.transactions.insert(id, txn);
                }
            },
        }
    }

    fn get_intent(&self, context: &BatchContext, key: &[u8]) -> Option<&TxnIntent> {
        if let Some(intent) = context.cache.transactional.get(key) {
            return Some(unsafe { std::mem::transmute(intent) });
        } else if let Some(intent) = self.intents.get(key) {
            return Some(unsafe { std::mem::transmute(intent) });
        }
        None
    }

    fn find_intent<'a>(
        &'a self,
        context: &'a BatchContext,
        key: &[u8],
        end: &[u8],
    ) -> Option<(&'a [u8], &'a TxnIntent)> {
        let bounds = if end.is_empty() {
            (Bound::Included(key.to_owned()), Bound::Unbounded)
        } else {
            (Bound::Included(key.to_owned()), Bound::Included(end.to_owned()))
        };
        let next1 = context.cache.transactional.range(bounds.clone()).next();
        let next2 = self.intents.range(bounds).next();
        match (next1, next2) {
            (None, None) => None,
            (Some((key, intent)), None) => Some((key, intent)),
            (None, Some((key, intent))) => Some((key, intent)),
            (Some((key1, intent1)), Some((key2, intent2))) => match key1 >= key2 {
                true => Some((key1, intent1)),
                false => Some((key2, intent2)),
            },
        }
    }

    pub fn resolve(
        &mut self,
        store: &mut DataStore,
        tracker: &mut ReplicationTracker,
        txn: &Transaction,
        self_update: bool,
    ) {
        if txn.status == TxnStatus::Pending {
            return;
        }
        let HashEntry::Occupied(mut entry) = self.transactions.entry(txn.id()) else {
            return;
        };

        let current = entry.get_mut();
        if !current.commit_set.is_empty() && txn.commit_set.is_empty() && !self_update {
            // It is possible for txn coordinator to issue txn resolution request to its locating
            // tablet in case of shard migration. It is importent for us to not write this empty
            // commit_set txn to log/store as it is a txn completion marker. In response, shard
            // migration should resolve txn intents.
            return;
        }

        let write_set = current.take_write_set();
        if txn.commit_set.is_empty() {
            entry.remove();
        } else {
            current.update(txn);
        }
        if txn.status == TxnStatus::Aborted {
            for span in write_set {
                self.intents.remove(&span.key);
            }
            let (replication, _replicating) = ReplicationHandle::new();
            tracker.batch(replication);
        } else {
            let commit_ts = txn.commit_ts();
            let (replication, replicating) = ReplicationHandle::new();
            for span in write_set {
                let Some((key, intent)) = self.intents.remove_entry(&span.key) else {
                    continue;
                };
                let Some(latest) = intent.into_latest(txn) else {
                    continue;
                };
                let value = Value::new_replicating(latest.into_value(), replicating.clone());
                store.put(key, commit_ts, value).unwrap();
            }
            tracker.batch(replication);
        }
    }

    fn promote(&mut self, context: &mut BatchContext) -> Result<()> {
        while let Some((key, intent)) = context.cache.transactional.pop_first() {
            let txn = match self.transactions.entry(intent.txn.id()) {
                HashEntry::Vacant(entry) => entry.insert(intent.txn.clone()),
                HashEntry::Occupied(entry) => entry.into_mut(),
            };
            txn.add_write_span(KeySpan { key: key.clone(), end: vec![] });
            self.intents.insert(key, intent);
        }
        Ok(())
    }
}

pub struct TabletStore {
    store: DataStore,
    provision: TxnProvision,

    watermark: TabletWatermark,
    watermarks: VecDeque<TabletWatermark>,
}

impl TabletStore {
    pub fn new<'a>(id: TabletId, shards: impl Into<Cow<'a, [ShardDescription]>>) -> Self {
        let shards = shards.into().into_owned();
        Self {
            store: DataStore::new(id, shards),
            provision: Default::default(),
            watermark: Default::default(),
            watermarks: Default::default(),
        }
    }

    pub fn shards(&self) -> &[ShardDescription] {
        &self.store.shards
    }

    pub fn transactions(&self) -> impl Iterator<Item = &Transaction> {
        self.provision.transactions.values().map(|txn| txn.get())
    }

    fn update_cursor(&mut self, cursor: MessageId) {
        if cursor > self.watermark.cursor {
            self.watermark.cursor = cursor;
            self.check_cursor();
        }
    }

    fn check_cursor(&mut self) {
        let Some(next_watermark) = self.watermarks.front() else {
            return;
        };
        if next_watermark.cursor > self.watermark.cursor {
            return;
        }
        let watermark = self.watermarks.pop_front().unwrap();
        if watermark.timing() > self.watermark.timing() {
            self.watermark.closed_timestamp = watermark.closed_timestamp;
            self.watermark.leader_expiration = watermark.leader_expiration;
        }
    }

    pub fn apply(&mut self, mut message: protos::DataMessage) -> Result<()> {
        let cursor = MessageId::new(message.epoch, message.sequence);
        self.update_cursor(cursor);
        if let (Some(closed_timestamp), Some(leader_expiration)) =
            (message.closed_timestamp.take(), message.leader_expiration.take())
        {
            let watermark = TabletWatermark { cursor, closed_timestamp, leader_expiration };
            self.update_watermark(watermark);
        }
        let txn = match message.temporal {
            Temporal::Timestamp(ts) => {
                let writes = Writes::from(message.operation.take());
                for write in writes {
                    self.store.put_if_located(write.key, ts, Value::from(write.value))?;
                }
                return Ok(());
            },
            Temporal::Transaction(txn) => txn,
        };

        let txn = self.prepare_txn(&txn);
        self.apply_txn(txn, message.operation.take().into());
        Ok(())
    }

    pub fn cursor(&self) -> MessageId {
        self.watermark.cursor
    }

    pub fn watermark(&self) -> &TabletWatermark {
        &self.watermark
    }

    pub fn update_watermark(&mut self, watermark: TabletWatermark) {
        if watermark.cursor <= self.watermark.cursor {
            if watermark.timing() > self.watermark.timing() {
                self.watermark.closed_timestamp = watermark.closed_timestamp;
                self.watermark.leader_expiration = watermark.leader_expiration;
            }
        } else if watermark.timing() > self.watermark.timing() {
            self.watermarks.push_back(watermark);
        }
    }

    fn prepare_txn(&mut self, txn: &Transaction) -> TxnRecord {
        self.provision.prepare_txn(txn)
    }

    fn apply_txn(&mut self, txn: TxnRecord, writes: Writes) {
        self.provision.apply_txn(&mut self.store, txn, writes)
    }

    pub fn resolve(&mut self, tracker: &mut ReplicationTracker, txn: &mut Transaction, self_update: bool) {
        self.provision.resolve(&mut self.store, tracker, txn, self_update);
        if txn.is_terminal() {
            self.shards().iter().for_each(|s| {
                txn.resolved_set.push(s.range.clone().into());
            });
        }
    }

    pub fn batch_timestamped(
        &mut self,
        context: &mut BatchContext,
        ts: Timestamp,
        requests: Vec<ShardRequest>,
    ) -> Result<Vec<ShardResponse>> {
        let mut responses = Vec::with_capacity(requests.len());
        for ShardRequest { shard_id, request } in requests.into_iter() {
            let key = request.key();
            let shard_store = self.store.find_shard_store_mut(shard_id.into(), key)?;
            let response = match request {
                DataRequest::Get(get) => {
                    let value = shard_store.get_timestamped(context, ts, &get.key)?;
                    context.reads.watch(&value.value);
                    let response = GetResponse { value: value.into() };
                    DataResponse::Get(response)
                },
                DataRequest::Find(find) => {
                    let (key, value) = shard_store.find_timestamped(context, ts, &find.key)?;
                    context.reads.watch(&value.value);
                    let response = FindResponse { key, value: value.into() };
                    DataResponse::Find(response)
                },
                DataRequest::Put(put) => {
                    let ts = shard_store.put_timestamped(context, ts, &put.key, put.value, put.expect_ts)?;
                    let response = PutResponse { write_ts: ts };
                    DataResponse::Put(response)
                },
                DataRequest::Increment(increment) => {
                    let incremented =
                        shard_store.increment_timestamped(context, ts, &increment.key, increment.increment)?;
                    let response = IncrementResponse { value: incremented };
                    DataResponse::Increment(response)
                },
                DataRequest::RefreshRead(refresh_read) => {
                    shard_store.refresh_read_timestamped(context, ts, refresh_read.span, refresh_read.from)?;
                    DataResponse::RefreshRead(RefreshReadResponse {})
                },
            };
            let shard = if shard_id == shard_store.id.into_raw() {
                None
            } else {
                Some(ShardDescriptor {
                    id: shard_id,
                    range: shard_store.range.clone(),
                    tablet_id: self.store.id.into(),
                })
            };
            responses.push(ShardResponse { response, shard });
        }
        self.store.promote(context)?;
        Ok(responses)
    }

    pub fn batch_transactional(
        &mut self,
        context: &mut BatchContext,
        txn: &Transaction,
        requests: Vec<ShardRequest>,
    ) -> Result<Vec<ShardResponse>> {
        let txn = self.provision.prepare_txn(txn);
        let mut responses = Vec::with_capacity(requests.len());
        for ShardRequest { shard_id, request } in requests.into_iter() {
            let key = request.key();
            let shard_store = self.store.find_shard_store_mut(shard_id.into(), key)?;
            let response = match request {
                DataRequest::Get(get) => {
                    let value =
                        shard_store.get_transactional(context, &self.provision, &txn, &get.key, get.sequence)?;
                    context.reads.watch(&value.value);
                    let response = GetResponse { value: value.into() };
                    DataResponse::Get(response)
                },
                DataRequest::Find(find) => {
                    let (key, value) =
                        shard_store.find_transactional(context, &self.provision, &txn, &find.key, find.sequence)?;
                    context.reads.watch(&value.value);
                    let response = FindResponse { key, value: value.into() };
                    DataResponse::Find(response)
                },
                DataRequest::Put(put) => {
                    let ts = shard_store.put_transactional(
                        context,
                        &mut self.provision,
                        &txn,
                        put.key,
                        put.value,
                        put.sequence,
                        put.expect_ts,
                    )?;
                    let response = PutResponse { write_ts: ts };
                    DataResponse::Put(response)
                },
                DataRequest::Increment(increment) => {
                    let incremented = shard_store.increment_transactional(
                        context,
                        &mut self.provision,
                        &txn,
                        increment.key,
                        increment.increment,
                        increment.sequence,
                    )?;
                    let response = IncrementResponse { value: incremented };
                    DataResponse::Increment(response)
                },
                DataRequest::RefreshRead(refresh) => {
                    shard_store.refresh_read_transactional(
                        context,
                        &mut self.provision,
                        &txn,
                        refresh.span,
                        refresh.from,
                    )?;
                    DataResponse::RefreshRead(RefreshReadResponse {})
                },
            };
            let shard = if shard_id == shard_store.id.into_raw() {
                None
            } else {
                Some(ShardDescriptor {
                    id: shard_id,
                    range: shard_store.range.clone(),
                    tablet_id: self.store.id.into(),
                })
            };
            responses.push(ShardResponse { response, shard });
        }
        self.provision.promote(context)?;
        Ok(responses)
    }

    pub fn batch(
        &mut self,
        context: &mut BatchContext,
        temporal: &Temporal,
        requests: Vec<ShardRequest>,
    ) -> Result<Vec<ShardResponse>> {
        match temporal {
            Temporal::Timestamp(ts) => self.batch_timestamped(context, *ts, requests),
            Temporal::Transaction(txn) => self.batch_transactional(context, txn, requests),
        }
    }
}

pub struct TxnTabletStore {
    txn_table: TxnTable,
    store: TabletStore,
    updated_txns: mpsc::UnboundedReceiver<Transaction>,
}

impl TxnTabletStore {
    pub fn new(store: TabletStore, clock: Clock, client: TabletClient) -> Self {
        let (txn_table, updated_txns) =
            TxnTable::new(clock, client, store.shards().to_vec(), store.transactions().cloned());
        Self { store, txn_table, updated_txns }
    }

    pub fn shards(&self) -> &[ShardDescription] {
        self.store.shards()
    }

    pub fn update_watermark(&mut self, watermark: TabletWatermark) {
        self.store.update_watermark(watermark)
    }

    pub fn step(&mut self) {
        self.store.watermark.cursor.sequence += 1;
        self.store.check_cursor();
    }

    pub fn updated_txns(&mut self) -> &mut mpsc::UnboundedReceiver<Transaction> {
        &mut self.updated_txns
    }

    pub fn update_txn(&mut self, txn: &mut Transaction) -> (Option<ReplicationTracker>, Vec<Request>) {
        let Some(requests) = self.txn_table.update_txn(txn, false) else {
            return (None, vec![]);
        };
        let mut tracker = ReplicationTracker::default();
        self.store.resolve(&mut tracker, txn, false);
        (Some(tracker), requests)
    }

    pub fn participate_txn(
        &mut self,
        request: ParticipateTxnRequest,
        channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
    ) {
        self.txn_table.participate_txn(request, channel)
    }

    #[instrument(skip(self))]
    pub fn process_request(&mut self, request: Request) -> Result<Option<BatchResult>> {
        let Some(Request { request, responser, .. }) = self.txn_table.sequence(request) else {
            return Ok(None);
        };

        let BatchRequest { mut temporal, requests, .. } = request;
        let mut context = BatchContext::default();
        let responses = match self.store.batch(&mut context, &temporal, requests) {
            Err(err) => {
                responser.send(Err(err)).ignore();
                return Ok(None);
            },
            Ok(responses) => responses,
        };
        let requests = if let Temporal::Transaction(txn) = &mut temporal {
            self.store.resolve(&mut context.replication, txn, true);
            self.txn_table.update_txn(txn, true).unwrap_or_default()
        } else {
            vec![]
        };
        let result = match context.replication.is_empty() {
            true => BatchResult::Read { temporal, responses, responser, blocker: context.reads },
            false => BatchResult::Write {
                temporal,
                responses,
                responser,
                writes: context.writes,
                replication: context.replication,
                requests,
            },
        };
        Ok(Some(result))
    }
}

pub trait Store: Send + Sync {
    fn get(&self, key: &[u8], ts: Timestamp) -> Result<TimestampedValue>;

    fn put(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()>;

    fn find(&self, key: &[u8], ts: Timestamp) -> Result<(Vec<u8>, TimestampedValue)>;
}
