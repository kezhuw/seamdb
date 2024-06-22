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

use std::cmp::Ordering::{self, *};
use std::time::Duration;

use super::*;

impl Temporal {
    pub fn timestamp(&self) -> Timestamp {
        match self {
            Temporal::Timestamp(ts) => *ts,
            Temporal::Transaction(txn) => txn.commit_ts(),
        }
    }

    pub fn into_transaction(self) -> Transaction {
        match self {
            Temporal::Timestamp(ts) => panic!("expect transaction, got timestamp {ts}"),
            Temporal::Transaction(txn) => txn,
        }
    }
}

impl Transaction {
    pub fn comparer() -> impl Fn(&Transaction, &Transaction) -> Ordering {
        let comparer = TxnMeta::comparer();
        move |a, b| comparer(&a.meta, &b.meta)
    }

    pub fn compare(&self, other: &Transaction) -> Ordering {
        self.meta.compare(&other.meta)
    }

    pub fn sort_spans(&mut self) {
        self.commit_set.sort_by(|left, right| left.key.cmp(&right.key))
    }

    pub fn start_ts(&self) -> Timestamp {
        self.meta.start_ts
    }

    pub fn commit_ts(&self) -> Timestamp {
        match self.commit_ts.is_zero() {
            true => self.start_ts(),
            false => self.commit_ts,
        }
    }

    pub fn is_rollbacked(&self, sequence: u32) -> bool {
        self.rollbacked_sequences.iter().any(|range| range.start <= sequence && sequence < range.end)
    }

    pub fn next_heartbeat_ts(&self) -> Timestamp {
        self.heartbeat_ts.max(self.start_ts()).into_physical() + Duration::from_millis(500)
    }

    pub fn restart(&mut self) {
        self.meta.epoch += 1;
        self.rollbacked_sequences.clear();
    }

    pub fn abort(&mut self) {
        // Bumps epoch so to accommodate server side transaction restart.
        self.restart();
        self.status = TxnStatus::Aborted;
    }

    pub fn update(&mut self, other: &Transaction) {
        assert_eq!(self.id(), other.id());
        assert_eq!(self.key(), other.key());
        assert_eq!(self.start_ts(), other.start_ts());
        self.heartbeat_ts.forward(other.heartbeat_ts);
        match self.epoch().cmp(&other.epoch()) {
            Less => {
                self.status = other.status;
                self.meta.epoch = other.epoch();
                self.rollbacked_sequences.clear();
                self.commit_ts = other.commit_ts;
                self.commit_set.clear();
                self.commit_set.extend(other.commit_set.iter().cloned());
            },
            Equal => {
                self.commit_ts.forward(other.commit_ts);
                let n = self.rollbacked_sequences.len();
                if n < other.rollbacked_sequences.len() {
                    self.rollbacked_sequences.extend(other.rollbacked_sequences[n..].iter().copied());
                } else if n == other.rollbacked_sequences.len()
                    && n != 0
                    && self.rollbacked_sequences[n - 1].end < other.rollbacked_sequences[n - 1].end
                {
                    self.rollbacked_sequences[n - 1].end = other.rollbacked_sequences[n - 1].end;
                }
                if !self.status.is_terminal() && other.status.is_terminal() {
                    self.status = other.status;
                    self.commit_set.clear();
                    self.commit_set.extend(other.commit_set.iter().cloned());
                }
            },
            Greater => {},
        }
    }

    pub fn heartbeat(&mut self, now: Timestamp) {
        self.expire(now);
        if !self.is_terminal() {
            self.heartbeat_ts = now.into_physical();
        }
    }

    pub fn expire(&mut self, now: Timestamp) -> bool {
        if self.status != TxnStatus::Pending {
            return self.status == TxnStatus::Aborted;
        }
        let next_heartbeat_ts = self.next_heartbeat_ts();
        if next_heartbeat_ts < now.into_physical() {
            self.abort();
            return true;
        }
        false
    }
}

pub trait HasTxnStatus {
    fn status(&self) -> TxnStatus;

    fn is_terminal(&self) -> bool {
        match self.status() {
            TxnStatus::Pending => false,
            TxnStatus::Aborted | TxnStatus::Committed => true,
        }
    }

    fn is_aborted(&self) -> bool {
        self.status() == TxnStatus::Aborted
    }

    fn is_committed(&self) -> bool {
        self.status() == TxnStatus::Committed
    }
}

impl HasTxnStatus for TxnStatus {
    fn status(&self) -> TxnStatus {
        *self
    }
}

impl HasTxnStatus for Transaction {
    fn status(&self) -> TxnStatus {
        self.status
    }
}

impl TxnMeta {
    pub fn is_same(&self, other: &TxnMeta) -> bool {
        self.id == other.id && self.key == other.key
    }

    pub fn comparer() -> impl Fn(&TxnMeta, &TxnMeta) -> Ordering {
        |a, b| a.compare(b)
    }

    pub fn compare(&self, other: &TxnMeta) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Equal => (self.start_ts, self.id).cmp(&(other.start_ts, other.id)),
            ordering => ordering,
        }
    }

    pub fn update(&mut self, other: &TxnMeta) {
        self.epoch = self.epoch.max(other.epoch);
    }
}

impl From<TxnMeta> for Transaction {
    fn from(meta: TxnMeta) -> Self {
        Self {
            meta,
            status: TxnStatus::Pending,
            commit_ts: Timestamp::default(),
            heartbeat_ts: Default::default(),
            write_set: Default::default(),
            commit_set: Default::default(),
            resolved_set: Default::default(),
            rollbacked_sequences: Default::default(),
        }
    }
}

impl From<Transaction> for TxnMeta {
    fn from(txn: Transaction) -> Self {
        txn.meta
    }
}

impl From<Timestamp> for Temporal {
    fn from(t: Timestamp) -> Self {
        Temporal::Timestamp(t)
    }
}

pub trait HasTxnMeta {
    fn meta(&self) -> &TxnMeta;

    fn id(&self) -> Uuid {
        self.meta().id
    }

    fn key(&self) -> &[u8] {
        &self.meta().key
    }

    fn epoch(&self) -> u32 {
        self.meta().epoch
    }
}

impl HasTxnMeta for TxnMeta {
    fn meta(&self) -> &TxnMeta {
        self
    }
}

impl HasTxnMeta for Transaction {
    fn meta(&self) -> &TxnMeta {
        &self.meta
    }
}
