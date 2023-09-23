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
use std::ops::RangeFrom;

use anyhow::Result;

use super::store::DataStore;
use super::types::{Temporal, Timestamp, TimestampedValue, Value};

#[derive(Default)]
pub struct MemoryStore {
    table: MemoryTable<Timestamp, Value>,
}

impl DataStore for MemoryStore {
    fn get(&self, temporal: &Temporal, key: &[u8]) -> Result<TimestampedValue> {
        let Some((ts, value)) = self.table.get(key, temporal.timestamp()) else {
            return Ok(TimestampedValue::default());
        };
        Ok(TimestampedValue::new(ts, value).adjust_timestamp())
    }

    fn put(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()> {
        self.table.put(key, ts, value);
        Ok(())
    }

    fn find(&self, key: &[u8], temporal: &Temporal) -> Result<(Vec<u8>, TimestampedValue)> {
        let Some((key, ts, value)) = self.table.find(key, temporal.timestamp()) else {
            return Ok((key.to_owned(), TimestampedValue::default()));
        };
        Ok((key.to_owned(), TimestampedValue::new(ts, value).adjust_timestamp()))
    }
}

#[derive(Default)]
pub struct MemoryTable<S, V> {
    map: BTreeMap<Vec<u8>, Vec<(S, V)>>,
}

impl<S: Copy + Ord + std::fmt::Debug, V: Clone + std::fmt::Debug> MemoryTable<S, V> {
    pub fn take(&mut self) -> BTreeMap<Vec<u8>, Vec<(S, V)>> {
        std::mem::take(&mut self.map)
    }

    pub fn get(&self, key: &[u8], seq: S) -> Option<(S, V)> {
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

    pub fn find(&self, key: &[u8], seq: S) -> Option<(Vec<u8>, S, V)> {
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

    pub fn put(&mut self, key: Vec<u8>, seq: S, value: V) {
        match self.map.entry(key) {
            Entry::Vacant(vacent) => {
                vacent.insert(vec![(seq, value)]);
            },
            Entry::Occupied(occupied) => occupied.into_mut().push((seq, value)),
        }
    }
}
