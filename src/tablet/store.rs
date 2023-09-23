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

use std::collections::VecDeque;

use anyhow::{anyhow, bail, Result};

use super::types::{ReplicationTracker, ReplicationWatcher, TimestampedValue, Value};
use crate::protos::{
    self,
    DataOperation,
    DataRequest,
    DataResponse,
    FindResponse,
    GetResponse,
    IncrementResponse,
    KeyRange,
    MessageId,
    PutResponse,
    ShardDescription,
    ShardId,
    TabletId,
    TabletWatermark,
    Timestamp,
};
use crate::tablet::memory::{MemoryStore, MemoryTable};
use crate::tablet::types::Temporal;

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

#[derive(Default)]
struct BatchContext {
    writes: Vec<protos::Write>,
    replication: ReplicationTracker,
}

struct ShardStore {
    id: ShardId,
    range: KeyRange,
    store: Box<dyn DataStore>,
    provision: MemoryTable<Timestamp, Value>,
}

impl ShardStore {
    pub fn new(id: ShardId, range: KeyRange) -> Self {
        Self { id, range, store: Box::<MemoryStore>::default(), provision: MemoryTable::default() }
    }

    pub fn get(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        if let Some((ts, value)) = self.provision.get(key, temporal.timestamp()) {
            return Ok(TimestampedValue::new(ts, value).adjust_timestamp());
        };
        self.store.get(temporal, key)
    }

    pub fn find(&self, temporal: &Temporal, key: &[u8]) -> Result<(Vec<u8>, TimestampedValue)> {
        if let Some((key, ts, value)) = self.provision.find(key, temporal.timestamp()) {
            return Ok((key.to_owned(), TimestampedValue::new(ts, value).adjust_timestamp()));
        }
        self.store.find(key, temporal)
    }

    fn check_write(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        let value = self.get(temporal, key)?;
        // FIXME: allow equal timestamp for now to gain write-your-write.
        if temporal.timestamp() < value.timestamp {
            return Err(anyhow!("write@{} encounters newer timestamp {}", temporal.timestamp(), value.timestamp));
        }
        Ok(value.adjust_timestamp())
    }

    fn add_write(&mut self, context: &mut BatchContext, ts: Timestamp, key: Vec<u8>, value: Option<protos::Value>) {
        let write = protos::Write { key: key.clone(), value: value.clone(), sequence: 0 };
        context.writes.push(write);
        let (value, replication) = Value::new(value);
        self.provision.put(key, ts, value);
        context.replication.batch(replication);
    }

    fn put(
        &mut self,
        context: &mut BatchContext,
        temporal: &Temporal,
        key: &[u8],
        value: Option<protos::Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        let existing_value = self.check_write(temporal, key)?;
        if let Some(expect_ts) = expect_ts.filter(|ts| *ts != existing_value.timestamp) {
            bail!("mismatch timestamp check: existing ts {}, expect ts {:}", existing_value.timestamp, expect_ts)
        }
        self.add_write(context, temporal.timestamp(), key.to_owned(), value);
        Ok(temporal.timestamp())
    }

    fn increment(
        &mut self,
        context: &mut BatchContext,
        temporal: &Temporal,
        key: &[u8],
        increment: i64,
    ) -> Result<i64> {
        let value = self.check_write(temporal, key)?;
        let i = value.value.read_int(key, "increment")?;
        let incremented = i + increment;
        let value = protos::Value::Int(incremented);
        self.add_write(context, temporal.timestamp(), key.to_owned(), Some(value));
        Ok(incremented)
    }

    fn commit(&mut self) -> Result<()> {
        for (key, values) in self.provision.take().into_iter() {
            for (ts, value) in values.into_iter() {
                self.store.put(key.clone(), ts, value)?;
            }
        }
        Ok(())
    }

    fn abort(&mut self) {
        self.provision.take();
    }
}

pub struct TabletStore {
    id: TabletId,
    stores: Vec<ShardStore>,
    watermark: TabletWatermark,
    watermarks: VecDeque<TabletWatermark>,
}

impl TabletStore {
    pub fn new(id: TabletId, shards: &[ShardDescription]) -> Self {
        let stores = shards.iter().map(|s| ShardStore::new(s.id.into(), s.range.clone())).collect();
        Self { id, stores, watermark: Default::default(), watermarks: Default::default() }
    }

    fn step(&mut self) {
        self.watermark.cursor.sequence += 1;
        self.check_cursor();
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

    fn get_shard(&self, id: ShardId) -> Result<&ShardStore> {
        for store in self.stores.iter() {
            if store.id == id {
                return Ok(store);
            }
        }
        bail!("shard {} not found in tablet {}", id, self.id)
    }

    fn get_shard_mut(&mut self, id: ShardId) -> Result<&mut ShardStore> {
        for store in self.stores.iter_mut() {
            if store.id == id {
                return Ok(store);
            }
        }
        bail!("shard {} not found in tablet {}", id, self.id)
    }

    fn find_shard_mut(&mut self, key: &[u8]) -> Result<&mut ShardStore> {
        for store in self.stores.iter_mut() {
            if store.range.contains(key) {
                return Ok(store);
            }
        }
        bail!("shard not found in tablet {} for key: {:?}", self.id, key)
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
        let Some(temporal) = message.temporal.take() else {
            return Ok(());
        };
        let temporal = Temporal::try_from(temporal)?;
        let ts = temporal.timestamp();
        let writes = Writes::from(message.operation.take());
        for write in writes {
            let shard = self.find_shard_mut(&write.key)?;
            shard.store.put(write.key, ts, Value::from(write.value))?;
        }
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

    pub fn get(&self, shard_id: ShardId, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        let shard = self.get_shard(shard_id)?;
        shard.get(temporal, key)
    }

    fn find(&self, shard_id: ShardId, key: &[u8], temporal: &Temporal) -> Result<(Vec<u8>, TimestampedValue)> {
        let shard = self.get_shard(shard_id)?;
        shard.find(temporal, key)
    }

    fn put(
        &mut self,
        shard_id: ShardId,
        context: &mut BatchContext,
        temporal: &Temporal,
        key: &[u8],
        value: Option<protos::Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        let shard = self.get_shard_mut(shard_id)?;
        shard.put(context, temporal, key, value, expect_ts)
    }

    fn increment(
        &mut self,
        shard_id: ShardId,
        context: &mut BatchContext,
        temporal: &Temporal,
        key: &[u8],
        increment: i64,
    ) -> Result<i64> {
        let shard = self.get_shard_mut(shard_id)?;
        shard.increment(context, temporal, key, increment)
    }

    /// FIXME: commit does not allow partial succeed.
    fn commit(&mut self) -> Result<()> {
        for shard in self.stores.iter_mut() {
            shard.commit()?;
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        for shard in self.stores.iter_mut() {
            shard.abort();
        }
    }

    pub fn batch(
        &mut self,
        temporal: &mut Temporal,
        shards: Vec<u64>,
        requests: Vec<DataRequest>,
    ) -> Result<BatchResult> {
        self.cleanup();
        let shards: Vec<ShardId> = unsafe { std::mem::transmute(shards) };
        let mut context = BatchContext::default();
        let mut blocker = ReplicationWatcher::default();
        let mut responses = Vec::with_capacity(requests.len());
        for (i, request) in requests.into_iter().enumerate() {
            match request {
                DataRequest::Get(get) => {
                    let value = self.get(shards[i], temporal, &get.key)?;
                    blocker.watch(&value.value);
                    let response = GetResponse { value: value.into() };
                    responses.push(DataResponse::Get(response));
                },
                DataRequest::Find(find) => {
                    let (key, value) = self.find(shards[i], &find.key, temporal)?;
                    blocker.watch(&value.value);
                    let response = FindResponse { key, value: value.into() };
                    responses.push(DataResponse::Find(response));
                },
                DataRequest::Put(put) => {
                    let ts = self.put(shards[i], &mut context, temporal, &put.key, put.value, put.expect_ts)?;
                    let response = PutResponse { write_ts: ts };
                    responses.push(DataResponse::Put(response));
                },
                DataRequest::Increment(increment) => {
                    let incremented =
                        self.increment(shards[i], &mut context, temporal, &increment.key, increment.increment)?;
                    let response = IncrementResponse { value: incremented };
                    responses.push(DataResponse::Increment(response));
                },
            }
        }
        if !context.writes.is_empty() {
            self.step();
            self.commit()?;
        }
        Ok(BatchResult {
            ts: temporal.timestamp(),
            blocker,
            responses,
            writes: context.writes,
            replication: context.replication,
        })
    }
}

pub trait DataStore: Send + Sync {
    fn get(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue>;

    fn put(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()>;

    fn find(&self, key: &[u8], temporal: &Temporal) -> Result<(Vec<u8>, TimestampedValue)>;
}
