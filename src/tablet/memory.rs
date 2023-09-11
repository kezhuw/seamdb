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

use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::ops::RangeFrom;

use anyhow::{anyhow, bail, Result};

use super::types::{
    BatchResult,
    MessageId,
    ReplicationTracker,
    ReplicationWatcher,
    TabletStore,
    TabletWatermark,
    Temporal,
    Timestamp,
    TimestampedValue,
    Value,
};
use crate::protos::{
    self,
    DataMessage,
    DataOperation,
    DataRequest,
    DataResponse,
    FindResponse,
    GetResponse,
    IncrementResponse,
    PutResponse,
};

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

#[derive(Default)]
pub struct MemoryTabletStore {
    watermark: TabletWatermark,
    watermarks: VecDeque<TabletWatermark>,
    table: MemoryTable<Timestamp, Value>,
}

impl MemoryTabletStore {
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
}

impl TabletStore for MemoryTabletStore {
    fn cursor(&self) -> MessageId {
        self.watermark.cursor
    }

    fn watermark(&self) -> &TabletWatermark {
        &self.watermark
    }

    fn update_watermark(&mut self, watermark: TabletWatermark) {
        if watermark.cursor <= self.watermark.cursor {
            if watermark.timing() > self.watermark.timing() {
                self.watermark.closed_timestamp = watermark.closed_timestamp;
                self.watermark.leader_expiration = watermark.leader_expiration;
            }
        } else if watermark.timing() > self.watermark.timing() {
            self.watermarks.push_back(watermark);
        }
    }

    fn get(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        let Some((ts, value)) = self.table.get(key, temporal.timestamp()) else {
            return Ok(TimestampedValue::default());
        };
        Ok(TimestampedValue::new(ts, value).adjust_timestamp())
    }

    fn find(&self, key: &[u8], temporal: &Temporal) -> Result<(Vec<u8>, TimestampedValue)> {
        let Some((key, ts, value)) = self.table.find(key, temporal.timestamp()) else {
            return Ok((key.to_owned(), TimestampedValue::default()));
        };
        Ok((key.to_owned(), TimestampedValue::new(ts, value).adjust_timestamp()))
    }

    fn apply(&mut self, mut message: DataMessage) -> Result<()> {
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
            self.table.put(write.key, ts, Value::from(write.value));
        }
        Ok(())
    }

    fn batch(&mut self, temporal: &mut Temporal, requests: Vec<DataRequest>) -> Result<BatchResult> {
        let mut tablet = VirtualTablet::new(self);
        let mut blocker = ReplicationWatcher::default();
        let mut responses = Vec::with_capacity(requests.len());
        for request in requests {
            match request {
                DataRequest::Get(get) => {
                    let value = tablet.get(temporal, &get.key)?;
                    blocker.watch(&value.value);
                    let response = GetResponse { value: value.into() };
                    responses.push(DataResponse::Get(response));
                },
                DataRequest::Find(find) => {
                    let (key, value) = tablet.find(temporal, &find.key)?;
                    blocker.watch(&value.value);
                    let response = FindResponse { key, value: value.into() };
                    responses.push(DataResponse::Find(response));
                },
                DataRequest::Put(put) => {
                    let ts = tablet.put(temporal, &put.key, put.value, put.expect_ts)?;
                    let response = PutResponse { write_ts: ts };
                    responses.push(DataResponse::Put(response));
                },
                DataRequest::Increment(increment) => {
                    let incremented = tablet.increment(temporal, &increment.key, increment.increment)?;
                    let response = IncrementResponse { value: incremented };
                    responses.push(DataResponse::Increment(response));
                },
            }
        }
        let (writes, replication) = tablet.commit();
        if !writes.is_empty() {
            self.step();
        }
        Ok(BatchResult { ts: temporal.timestamp(), blocker, responses, writes, replication })
    }
}

#[derive(Default)]
struct MemoryTable<S, V> {
    map: BTreeMap<Vec<u8>, Vec<(S, V)>>,
}

impl<S: Copy + Ord + std::fmt::Debug, V: Clone + std::fmt::Debug> MemoryTable<S, V> {
    fn get(&self, key: &[u8], seq: S) -> Option<(S, V)> {
        let Some(values) = self.map.get(key) else {
            return None;
        };
        for (s, v) in values.iter().rev() {
            if *s <= seq {
                return Some((*s, v.clone()));
            }
        }
        None
    }

    fn find(&self, key: &[u8], seq: S) -> Option<(Vec<u8>, S, V)> {
        let range = self.map.range(RangeFrom { start: key.to_owned() });
        for (key, values) in range {
            for (s, v) in values.iter().rev() {
                if *s <= seq {
                    return Some((key.to_owned(), *s, v.clone()));
                }
            }
        }
        None
    }

    fn put(&mut self, key: Vec<u8>, seq: S, value: V) {
        match self.map.entry(key) {
            Entry::Vacant(vacent) => {
                vacent.insert(vec![(seq, value)]);
            },
            Entry::Occupied(occupied) => occupied.into_mut().push((seq, value)),
        }
    }
}

struct VirtualTablet<'a> {
    tablet: &'a mut MemoryTabletStore,
    provision: MemoryTable<Timestamp, Value>,

    writes: Vec<protos::Write>,
    replication: ReplicationTracker,
}

impl<'a> VirtualTablet<'a> {
    pub fn new(tablet: &'a mut MemoryTabletStore) -> Self {
        Self { tablet, provision: Default::default(), writes: Default::default(), replication: Default::default() }
    }

    pub fn get(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        if let Some((ts, value)) = self.provision.get(key, temporal.timestamp()) {
            return Ok(TimestampedValue::new(ts, value).adjust_timestamp());
        };
        self.tablet.get(temporal, key)
    }

    pub fn find(&self, temporal: &Temporal, key: &[u8]) -> Result<(Vec<u8>, TimestampedValue)> {
        if let Some((key, ts, value)) = self.provision.find(key, temporal.timestamp()) {
            return Ok((key.to_owned(), TimestampedValue::new(ts, value).adjust_timestamp()));
        }
        self.tablet.find(key, temporal)
    }

    fn check_write(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        let value = self.get(temporal, key)?;
        // FIXME: allow equal timestamp for now to gain write-your-write.
        if temporal.timestamp() < value.timestamp {
            return Err(anyhow!("write@{} encounters newer timestamp {}", temporal.timestamp(), value.timestamp));
        }
        Ok(value.adjust_timestamp())
    }

    fn add_write(&mut self, ts: Timestamp, key: Vec<u8>, value: Option<protos::Value>) {
        let write = protos::Write { key: key.clone(), value: value.clone(), sequence: 0 };
        self.writes.push(write);
        let (value, replication) = Value::new(value);
        self.provision.put(key, ts, value);
        self.replication.batch(replication);
    }

    fn put(
        &mut self,
        temporal: &Temporal,
        key: &[u8],
        value: Option<protos::Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp> {
        let existing_value = self.check_write(temporal, key)?;
        if let Some(expect_ts) = expect_ts.filter(|ts| *ts != existing_value.timestamp) {
            bail!("mismatch timestamp check: existing ts {}, expect ts {:}", existing_value.timestamp, expect_ts)
        }
        self.add_write(temporal.timestamp(), key.to_owned(), value);
        Ok(temporal.timestamp())
    }

    fn increment(&mut self, temporal: &Temporal, key: &[u8], increment: i64) -> Result<i64> {
        let value = self.check_write(temporal, key)?;
        let i = value.value.read_int(key, "increment")?;
        let incremented = i + increment;
        let value = protos::Value::Int(incremented);
        self.add_write(temporal.timestamp(), key.to_owned(), Some(value));
        Ok(incremented)
    }

    pub fn commit(self) -> (Vec<protos::Write>, ReplicationTracker) {
        for (key, values) in self.provision.map.into_iter() {
            for (ts, value) in values.into_iter() {
                self.tablet.table.put(key.clone(), ts, value);
            }
        }
        (self.writes, self.replication)
    }
}
