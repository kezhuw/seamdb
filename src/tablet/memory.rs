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

use super::provision::{TimestampedValue, Value};
use super::store::Store;
use crate::protos::{PlainValue, Timestamp};

#[derive(Default, Clone)]
pub struct MemoryStore {
    table: MemoryTable<Timestamp, Value>,
}

impl MemoryStore {
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Timestamp, PlainValue)> {
        self.table
            .map
            .iter()
            .flat_map(|(key, values)| values.iter().map(|(ts, value)| (key.as_slice(), *ts, value.to_protos_value())))
    }
}

impl Store for MemoryStore {
    fn get(&self, key: &[u8], ts: Timestamp) -> Result<TimestampedValue> {
        let Some((ts, value)) = self.table.get(key, ts) else {
            return Ok(TimestampedValue::default());
        };
        Ok(TimestampedValue::new(ts, value))
    }

    fn put(&mut self, key: Vec<u8>, ts: Timestamp, value: Value) -> Result<()> {
        self.table.put(key, ts, value);
        Ok(())
    }

    fn find(&self, key: &[u8], ts: Timestamp) -> Result<(Vec<u8>, TimestampedValue)> {
        let Some((key, ts, value)) = self.table.find(key, ts) else {
            return Ok((vec![], TimestampedValue::default()));
        };
        Ok((key.to_owned(), TimestampedValue::new(ts, value)))
    }
}

#[derive(Default, Clone, Debug)]
pub struct MemoryTable<S, V> {
    map: BTreeMap<Vec<u8>, Vec<(S, V)>>,
}

impl<S: Copy + Ord + std::fmt::Debug, V: Clone + std::fmt::Debug> MemoryTable<S, V> {
    pub fn take(&mut self) -> BTreeMap<Vec<u8>, Vec<(S, V)>> {
        std::mem::take(&mut self.map)
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }

    pub fn get(&self, key: &[u8], seq: S) -> Option<(S, V)> {
        let values = self.map.get(key)?;
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
