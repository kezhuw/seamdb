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

mod fence;

use std::cmp::Ordering::*;
use std::collections::btree_map::BTreeMap;
use std::ops::{Range, RangeFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use asyncs::select;
use asyncs::sync::watch;
use futures::stream::{SelectAll, StreamExt};
use hashbrown::hash_map::{Entry as HashEntry, HashMap};
use ignore_result::Ignore;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tracing::{debug, instrument, trace};

use self::fence::FenceTable;
use crate::clock::Clock;
use crate::keys::Key;
use crate::protos::{
    BatchRequest,
    BatchResponse,
    HasTxnMeta,
    HasTxnStatus,
    KeySpan,
    KeySpanRef,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    ShardDescription,
    ShardRequest,
    SpanOrdering,
    Temporal,
    Timestamp,
    Transaction,
    TxnMeta,
    TxnStatus,
    Uuid,
};
use crate::tablet::client::TabletClient;
use crate::tablet::types::{StreamingChannel, StreamingResponser};

#[derive(Debug)]
pub struct Request {
    pub read_keys: Vec<KeySpan>,
    pub write_keys: Vec<KeySpan>,

    pub temporal: Temporal,
    pub requests: Vec<ShardRequest>,
    pub responser: oneshot::Sender<Result<BatchResponse>>,
}

impl Request {
    pub fn new(request: BatchRequest, responser: oneshot::Sender<Result<BatchResponse>>) -> Self {
        let mut read_keys = vec![];
        let mut write_keys = vec![];
        let BatchRequest { temporal, requests, .. } = request;
        requests.iter().for_each(|r| {
            let key = r.key();
            let span = KeySpan { key: key.to_vec(), end: vec![] };
            if r.is_read() {
                read_keys.push(span)
            } else {
                write_keys.push(span)
            }
        });
        Self { read_keys, write_keys, temporal, requests, responser }
    }

    pub fn is_readonly(&self) -> bool {
        self.requests.iter().all(|r| r.is_read())
    }

    pub fn txn(&self) -> Option<&Transaction> {
        match &self.temporal {
            Temporal::Transaction(txn) => Some(txn),
            _ => None,
        }
    }

    pub fn txn_mut(&mut self) -> Option<&mut Transaction> {
        match &mut self.temporal {
            Temporal::Transaction(txn) => Some(txn),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ResolvedSet {
    spans: Vec<KeySpan>,
}

impl ResolvedSet {
    pub fn next(&self, span: &KeySpan) -> Option<Vec<u8>> {
        let mut key = span.key.clone();
        let end = span.end.as_slice();
        let mut span = KeySpanRef::new(&key, &span.end);
        let i = self.spans.partition_point(|item| {
            if item.end.is_empty() {
                span.key > item.key.as_slice()
            } else {
                span.key >= item.end.as_slice()
            }
        });
        for item in self.spans[i..].iter() {
            if item.end.is_empty() {
                match item.key.as_slice().cmp(span.key) {
                    Less => continue,
                    Equal => match span.end.is_empty() {
                        true => return None,
                        false => {
                            key.push(0);
                            if key.as_slice() >= end {
                                return None;
                            }
                            span = KeySpanRef::new(&key, end);
                        },
                    },
                    Greater => return Some(key),
                }
                continue;
            }
            match (span.key.cmp(&item.key), span.end.is_empty()) {
                (Less, _) => break,
                (Equal | Greater, true) => return None,
                (Equal | Greater, false) => match span.end.cmp(&item.end) {
                    Less | Equal => return None,
                    Greater => {
                        key.clear();
                        key.extend(item.end.iter().copied());
                        span = KeySpanRef::new(&key, end);
                    },
                },
            }
        }
        Some(key)
    }

    pub fn push(&mut self, spans: Vec<KeySpan>) {
        if spans.is_empty() {
            return;
        }
        self.spans.extend(spans);
        self.spans.sort_by(|a, b| (&a.key, &a.end).cmp(&(&b.key, &b.end)));
        let mut i = 0;
        while i < self.spans.len() - 1 {
            let (firsts, nexts) = self.spans.split_at_mut(i + 1);
            let current = firsts.last_mut().unwrap();
            let next = nexts.first_mut().unwrap();
            if current.end.is_empty() {
                match current.key.cmp(&next.key) {
                    Less => i += 1,
                    Equal => {
                        self.spans.remove(i);
                    },
                    Greater => unreachable!("spans are sorted"),
                }
                continue;
            }
            match current.end.cmp(&next.key) {
                Less => i += 1,
                Equal => match next.end.is_empty() {
                    true => {
                        current.end.push(0);
                        self.spans.remove(i + 1);
                    },
                    false => {
                        current.end = std::mem::take(&mut next.end);
                        self.spans.remove(i + 1);
                    },
                },
                Greater => match next.end.is_empty() {
                    true => {
                        self.spans.remove(i + 1);
                    },
                    false => {
                        current.end = std::mem::take(&mut current.end).max(std::mem::take(&mut next.end));
                        self.spans.remove(i + 1);
                    },
                },
            }
        }
    }
}

enum PendingRequest {
    Timestamped(Request),
    Transactional(TxnId),
}

struct WriteLock {
    span: KeySpan,

    owner: TxnMeta,
    pendings: Vec<PendingRequest>,
    blockings: UnboundedSender<Transaction>,
}

impl WriteLock {
    pub fn new(span: KeySpan, participant: &mut TxnParticipant) -> Self {
        participant.txn.write_set.push(span.clone());
        Self::new_locked(span, participant)
    }

    pub fn new_locked(span: KeySpan, participant: &TxnParticipant) -> Self {
        Self { span, owner: participant.txn.meta.clone(), pendings: vec![], blockings: participant.updates.clone() }
    }

    pub fn block(&mut self, request: Request) {
        self.pendings.push(PendingRequest::Timestamped(request));
    }

    pub fn block_txn(&mut self, participant: &mut TxnParticipant, request: Request) {
        participant.pendings.push(request);
        let txn = &participant.txn;
        if !self.pendings.iter().any(|item| match item {
            PendingRequest::Transactional(txn_id) => *txn_id == txn.id(),
            _ => false,
        }) {
            self.pendings.push(PendingRequest::Transactional(txn.id()));
        }
        self.blockings.send(txn.clone()).ignore();
    }
}

#[derive(Default)]
pub struct LockTable {
    locks: BTreeMap<Key, WriteLock>,
}

impl LockTable {
    fn add_locked(&mut self, span: KeySpan, participant: &TxnParticipant) {
        let lock = WriteLock::new_locked(span.clone(), participant);
        self.locks.insert(span.end_key().to_vec(), lock);
    }

    fn unlock(&mut self, txn: &TxnMeta, locks: Vec<KeySpan>) -> Vec<PendingRequest> {
        let mut requests = vec![];
        let mut keys = vec![];
        for span in locks {
            let range = if span.end.is_empty() {
                self.locks.range_mut(RangeFrom { start: span.key })
            } else {
                self.locks.range_mut(Range { start: span.key, end: span.end })
            };
            for (key, lock) in range {
                if !lock.owner.is_same(txn) {
                    break;
                }
                keys.push(key.clone());
            }
            keys.drain(..).for_each(|key| {
                let lock = self.locks.remove(&key).unwrap();
                requests.extend(lock.pendings.into_iter());
            })
        }
        requests
    }

    fn sequence_txn(&mut self, participant: &mut TxnParticipant, request: Request) -> Option<Request> {
        let request = self.sequence_txn_reads(participant, request)?;
        let mut locks = BTreeMap::default();
        'next_write_span: for span in request.write_keys.iter() {
            let mut span = span.as_ref();
            let range = self.locks.range_mut(RangeFrom { start: span.key.to_owned() });
            for (_end, lock) in range {
                match (span.compare(lock.span.as_ref()), lock.owner.id() == participant.txn.id()) {
                    (SpanOrdering::LessDisjoint | SpanOrdering::LessContiguous, _) => {
                        let lock = WriteLock::new(span.into_owned(), participant);
                        locks.insert(span.end_key().to_owned(), lock);
                        continue 'next_write_span;
                    },
                    (SpanOrdering::GreaterDisjoint | SpanOrdering::GreaterContiguous, _) => continue,
                    (_, false) => {
                        lock.block_txn(participant, request);
                        return None;
                    },
                    (
                        SpanOrdering::Equal
                        | SpanOrdering::SubsetAll
                        | SpanOrdering::SubsetRight
                        | SpanOrdering::SubsetLeft,
                        true,
                    ) => continue 'next_write_span,
                    (SpanOrdering::IntersectRight | SpanOrdering::ContainRight, true) => {
                        let span = KeySpan { key: span.key.to_owned(), end: lock.span.key.to_owned() };
                        let lock = WriteLock::new(span, participant);
                        locks.insert(lock.span.end_key().to_owned(), lock);
                        continue 'next_write_span;
                    },
                    (SpanOrdering::IntersectLeft | SpanOrdering::ContainLeft, true) => {
                        span.key = lock.span.end_key();
                    },
                    (SpanOrdering::ContainAll, true) => {
                        let lock = WriteLock::new(
                            KeySpan { key: span.key.to_owned(), end: lock.span.key.to_owned() },
                            participant,
                        );
                        span.key = unsafe { std::mem::transmute(lock.span.end_key()) };
                        locks.insert(lock.span.end_key().to_owned(), lock);
                    },
                }
            }
            let lock = WriteLock::new(span.into_owned(), participant);
            locks.insert(span.end_key().to_owned(), lock);
        }
        self.locks.extend(locks);
        Some(request)
    }

    fn sequence_txn_reads(&mut self, participant: &mut TxnParticipant, request: Request) -> Option<Request> {
        'next_write_span: for span in request.read_keys.iter() {
            let span = span.as_ref();
            let range = self.locks.range_mut(RangeFrom { start: span.key.to_owned() });
            for (_end, lock) in range {
                match span.compare(lock.span.as_ref()) {
                    SpanOrdering::LessDisjoint | SpanOrdering::LessContiguous => continue 'next_write_span,
                    SpanOrdering::GreaterDisjoint | SpanOrdering::GreaterContiguous => continue,
                    SpanOrdering::Equal
                    | SpanOrdering::SubsetAll
                    | SpanOrdering::SubsetLeft
                    | SpanOrdering::SubsetRight
                    | SpanOrdering::IntersectRight
                    | SpanOrdering::ContainRight => match lock.owner.id() == participant.txn.id() {
                        true => continue 'next_write_span,
                        false => {
                            lock.block_txn(participant, request);
                            return None;
                        },
                    },
                    SpanOrdering::IntersectLeft | SpanOrdering::ContainLeft | SpanOrdering::ContainAll => {
                        match lock.owner.id() == participant.txn.id() {
                            false => {
                                lock.block_txn(participant, request);
                                return None;
                            },
                            true => continue,
                        }
                    },
                }
            }
        }
        Some(request)
    }

    fn sequence_timestamped(&mut self, request: Request) -> Option<Request> {
        'next_write_span: for span in request.write_keys.iter().chain(request.read_keys.iter()) {
            let span = span.as_ref();
            let range = self.locks.range_mut(RangeFrom { start: span.key.to_owned() });
            for (_end, lock) in range {
                match span.compare(lock.span.as_ref()) {
                    SpanOrdering::LessDisjoint | SpanOrdering::LessContiguous => continue 'next_write_span,
                    SpanOrdering::GreaterDisjoint | SpanOrdering::GreaterContiguous => continue,
                    _ => {
                        lock.block(request);
                        return None;
                    },
                }
            }
        }
        Some(request)
    }
}

struct TaskHandle {
    task: tokio::task::JoinHandle<()>,
}

impl TaskHandle {
    pub fn new(task: tokio::task::JoinHandle<()>) -> Self {
        Self { task }
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.task.abort()
    }
}

struct TxnParticipant {
    txn: Transaction,
    coordination: Option<UnboundedSender<TxnCoordination>>,
    pendings: Vec<Request>,
    updates: UnboundedSender<Transaction>,
    clock: Clock,
    #[allow(dead_code)]
    task: TaskHandle,
}

impl TxnParticipant {
    pub fn new(participation: Arc<TxnParticipation>, txn: Transaction, coordinator: bool) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let clock = participation.clock.clone();
        let (task, coordination) = if coordinator {
            participation.start_coordinator(txn.clone(), receiver)
        } else {
            participation.start_participant(txn.clone(), receiver)
        };
        Self { txn, coordination, updates: sender, pendings: vec![], task, clock }
    }

    pub fn heartbeat_txn(&mut self, txn: &mut Transaction) -> Result<()> {
        if self.coordination.is_some() {
            self.txn.heartbeat(self.clock.now());
        } else if txn.is_terminal() && !txn.commit_set.is_empty() {
            bail!("txn abortion/commitment sent to no coordinator node")
        }
        if txn.epoch() > self.txn.epoch() {
            let status = txn.status;
            let commit_set = std::mem::take(&mut txn.commit_set);
            if txn.is_terminal() {
                txn.status = TxnStatus::Pending;
            }
            self.txn.update(txn);
            txn.status = status;
            txn.commit_set = commit_set;
        }
        let commit_ts = txn.commit_ts();
        txn.update(&self.txn);
        if txn.commit_ts() != commit_ts && txn.status == TxnStatus::Committed {
            debug!("txn pushed");
            txn.status = TxnStatus::Pending;
            txn.commit_set.clear();
        }
        self.updates.send(self.txn.clone()).ignore();
        Ok(())
    }

    pub fn update_txn(&mut self, txn: &mut Transaction) -> bool {
        let epoch = self.txn.epoch();
        self.txn.update(txn);
        if self.coordination.is_some() {
            self.txn.expire(self.clock.now());
        }
        self.updates.send(Transaction { resolved_set: txn.resolved_set.clone(), ..self.txn.clone() }).ignore();
        txn.update(&self.txn);
        epoch != self.txn.epoch() || txn.status.is_terminal()
    }

    pub fn fence_ts(&mut self, ts: Timestamp) -> Timestamp {
        match (self.coordination.is_some(), self.txn.status) {
            (false, _) => self.txn.commit_ts(),
            (true, TxnStatus::Pending) => {
                if ts > self.txn.commit_ts() {
                    debug!("push txn {} commit ts from {} to {}", self.txn.id(), self.txn.commit_ts(), ts);
                    self.txn.commit_ts = ts;
                    self.updates.send(self.txn.clone()).ignore();
                }
                ts
            },
            (true, TxnStatus::Aborted | TxnStatus::Committed) => ts,
        }
    }
}

pub type TxnId = Uuid;

struct TxnParticipation {
    clock: Clock,
    client: TabletClient,
    feedback: UnboundedSender<Transaction>,
}

impl TxnParticipation {
    pub fn subscribe(&self, txn: &TxnMeta, sender: Sender<ParticipateTxnResponse>) -> watch::Sender<Transaction> {
        let client = self.client.clone();
        let txn = Transaction { meta: txn.clone(), ..Default::default() };
        let (publisher, mut watcher) = watch::channel(txn);
        tokio::spawn(async move {
            loop {
                let txn = watcher.borrow_and_update().clone();
                let (requester, mut receiver) =
                    client.open_participate_txn(ParticipateTxnRequest { txn, dependents: vec![] }, false).await;
                loop {
                    select! {
                        r = watcher.changed() => match r {
                            Err(_) => return,
                            Ok(updated) => requester.send(ParticipateTxnRequest {
                                txn: updated.clone(),
                                dependents: vec![],
                            }).await.ignore(),
                        },
                        r = receiver.message() => match r {
                            Ok(Some(response)) => sender.send(response).await.ignore(),
                            _ => break,
                        }
                    }
                }
            }
        });
        publisher
    }
}

#[derive(Default)]
struct TxnSubscribers {
    subscribers: Vec<StreamingResponser<ParticipateTxnResponse>>,
}

impl TxnSubscribers {
    pub fn push(&mut self, subscriber: StreamingResponser<ParticipateTxnResponse>) {
        self.subscribers.push(subscriber);
    }

    pub fn send(&mut self, response: ParticipateTxnResponse) {
        let mut i = 0;
        while i < self.subscribers.len() {
            match self.subscribers[i].send(response.clone()) {
                false => {
                    self.subscribers.swap_remove(i);
                },
                true => i += 1,
            }
        }
    }
}

struct TxnCoordinator<'a> {
    txn: Transaction,

    dependents: Vec<TxnMeta>,
    transitives: HashMap<Uuid, (TxnMeta, watch::Sender<Transaction>, Vec<TxnMeta>)>,
    subscribers: TxnSubscribers,
    subscriptions: Sender<ParticipateTxnResponse>,

    participation: &'a TxnParticipation,
}

impl<'a> TxnCoordinator<'a> {
    pub fn new(txn: Transaction, participation: &'a TxnParticipation) -> (Self, Receiver<ParticipateTxnResponse>) {
        let (sender, receiver) = mpsc::channel(256);
        let coordinator = TxnCoordinator {
            txn,
            dependents: vec![],
            transitives: Default::default(),
            subscribers: TxnSubscribers::default(),
            subscriptions: sender,
            participation,
        };
        (coordinator, receiver)
    }

    pub fn into_txn(self) -> Transaction {
        self.txn
    }

    pub fn participate(
        &mut self,
        participant: Transaction,
        dependents: Vec<TxnMeta>,
        subscriber: StreamingResponser<ParticipateTxnResponse>,
    ) {
        let pushed = match participant.epoch().cmp(&self.txn.epoch()) {
            Greater => true,
            Equal => participant.commit_ts > self.txn.commit_ts,
            Less => {
                subscriber.send(ParticipateTxnResponse { txn: self.txn.clone(), dependents: vec![] });
                self.subscribers.push(subscriber);
                return;
            },
        };
        self.subscribers.push(subscriber);
        if pushed {
            self.participation.feedback.send(participant).ignore();
            return;
        }
        self.extend_dependents(dependents);
    }

    pub fn update_participation(&mut self, participant: Transaction, dependents: Vec<TxnMeta>) {
        let pushed = match participant.epoch().cmp(&self.txn.epoch()) {
            Greater => true,
            Equal => participant.commit_ts > self.txn.commit_ts,
            Less => return,
        };
        if pushed {
            self.participation.feedback.send(participant).ignore();
            return;
        }
        self.extend_dependents(dependents);
    }

    pub fn update_txn(&mut self, txn: Transaction) {
        let epoch = self.txn.epoch();
        self.txn.update(&txn);
        if self.txn.epoch() > epoch || self.txn.is_terminal() {
            self.dependents.clear();
            self.transitives.clear();
        }
        self.subscribers.send(ParticipateTxnResponse { txn: self.txn.clone(), dependents: self.dependents.clone() });
        self.txn.resolved_set.clone_from(&txn.resolved_set);
    }

    pub fn add_dependent(&mut self, txn: TxnMeta) {
        match self.dependents.iter_mut().find(|item| item.id() == txn.id()) {
            Some(found) => found.update(&txn),
            None => self.dependents.push(txn.clone()),
        };
        self.subscribe_dependents(std::iter::once(txn));
        self.subscribers.send(ParticipateTxnResponse { txn: self.txn.clone(), dependents: self.dependents.clone() });
    }

    pub fn extend_dependents(&mut self, txns: Vec<TxnMeta>) {
        txns.into_iter().for_each(|txn| self.add_dependent(txn));
    }

    fn subscribe_dependents(&mut self, dependents: impl Iterator<Item = TxnMeta>) {
        for dependent in dependents {
            let HashEntry::Vacant(entry) = self.transitives.entry(dependent.id()) else {
                continue;
            };
            let publisher = self.participation.subscribe(&dependent, self.subscriptions.clone());
            entry.insert((dependent, publisher, vec![]));
        }
    }

    pub fn extend_transitive_dependents(&mut self, mut txn: Transaction, dependents: Vec<TxnMeta>) {
        let HashEntry::Occupied(mut entry) = self.transitives.entry(txn.id()) else {
            return;
        };
        if txn.epoch() < entry.get().0.epoch() {
            return;
        }
        if txn.is_terminal() || txn.epoch() > entry.get().0.epoch() {
            entry.remove();
            return;
        }
        let deadlock = dependents.iter().any(|dependent| dependent.id() == self.txn.id());
        if deadlock {
            match self.txn.compare(&txn) {
                Less => {
                    txn.restart();
                    entry.get_mut().1.publish(txn);
                },
                _ => {
                    let mut pushed_txn = self.txn.clone();
                    pushed_txn.restart();
                    self.participation.feedback.send(pushed_txn).ignore();
                },
            }
            return;
        }
        let entry = entry.into_mut();
        entry.2.clear();
        entry.2.extend(dependents.iter().cloned());
        self.subscribe_dependents(dependents.into_iter());
    }
}

#[derive(Clone)]
struct TxnParticipator {
    txn: Transaction,
    dependents: Vec<TxnMeta>,
}

impl TxnParticipator {
    pub fn new(txn: Transaction) -> Self {
        Self { txn, dependents: vec![] }
    }

    pub fn update_txn(&mut self, txn: &Transaction) {
        self.txn.update(txn);
    }

    pub fn update_dependent(&mut self, txn: TxnMeta) {
        match self.dependents.iter_mut().find(|dependent| dependent.id() == txn.id()) {
            Some(dependent) => dependent.epoch = dependent.epoch.max(txn.epoch),
            None => self.dependents.push(txn),
        }
    }

    pub fn to_request(&self) -> ParticipateTxnRequest {
        ParticipateTxnRequest { txn: self.txn.clone(), dependents: self.dependents.clone() }
    }
}

struct TxnCoordination {
    request: ParticipateTxnRequest,
    channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
}

impl TxnParticipation {
    pub fn new(clock: Clock, client: TabletClient, feedback: UnboundedSender<Transaction>) -> Self {
        Self { clock, client, feedback }
    }

    pub fn start_coordinator(
        self: Arc<TxnParticipation>,
        txn: Transaction,
        updates: UnboundedReceiver<Transaction>,
    ) -> (TaskHandle, Option<UnboundedSender<TxnCoordination>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            self.serve_coordinator(txn, updates, receiver).await;
        });
        (TaskHandle::new(task), Some(sender))
    }

    pub fn start_participant(
        self: Arc<TxnParticipation>,
        txn: Transaction,
        updates: UnboundedReceiver<Transaction>,
    ) -> (TaskHandle, Option<UnboundedSender<TxnCoordination>>) {
        let task = tokio::spawn(async move {
            self.serve_participant(txn, updates).await;
        });
        (TaskHandle::new(task), None)
    }

    #[instrument(skip_all, fields(txn.id = %txn.meta.id))]
    pub async fn serve_coordinator(
        &self,
        txn: Transaction,
        mut updates: UnboundedReceiver<Transaction>,
        mut coordinations: UnboundedReceiver<TxnCoordination>,
    ) {
        let (mut coordinator, mut subscriptions) = TxnCoordinator::new(txn, self);
        let mut participations = SelectAll::new();
        while coordinator.txn.status == TxnStatus::Pending {
            let mut sleep = tokio::time::sleep(
                Duration::from_millis(1).max(coordinator.txn.next_heartbeat_ts() - self.clock.now()),
            );
            select! {
                _ = unsafe { Pin::new_unchecked(&mut sleep) } => {
                    self.feedback.send(coordinator.txn.clone()).ignore();
                },
                Some(updated) = updates.recv() => match updated.id() == coordinator.txn.id() {
                    false => coordinator.add_dependent(updated.meta),
                    true => coordinator.update_txn(updated),
                },
                Some(TxnCoordination { request, channel }) = coordinations.recv() => {
                    let (requester, responser) = channel.into_splits();
                    coordinator.participate(request.txn, request.dependents, responser);
                    participations.push(requester);
                },
                Some(subscription) = subscriptions.recv() => coordinator.extend_transitive_dependents(subscription.txn, subscription.dependents),
                Some(participation) = participations.next() => {
                    coordinator.update_participation(participation.txn, participation.dependents);
                }
            }
        }
        drop(updates);
        let mut txn = coordinator.into_txn();
        let mut resolved_set = ResolvedSet::default();
        let mut commit_set = std::mem::take(&mut txn.commit_set);
        debug!("epoch: {}, status: {:?}, commit set: {:?}", txn.epoch(), txn.status, commit_set);
        resolved_set.push(std::mem::take(&mut txn.resolved_set));
        while let Some(last) = commit_set.last() {
            let Some(key) = resolved_set.next(last) else {
                commit_set.pop();
                continue;
            };
            match self.resolve_txn(txn.clone(), &key).await {
                Err(err) => {
                    debug!("fail to resolve txn: {err:?}");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                },
                Ok(spans) => resolved_set.push(spans),
            }
        }
        self.feedback.send(txn).ignore();
    }

    async fn resolve_txn(&self, txn: Transaction, key: &[u8]) -> Result<Vec<KeySpan>> {
        trace!("resolving txn for key: {key:?}");
        let (tablet_id, mut client) = self.client.tablet_client(key).await?;
        let request = BatchRequest {
            tablet_id: tablet_id.into(),
            uncertainty: None,
            temporal: Temporal::Transaction(txn),
            requests: vec![],
        };
        let response = client.batch(request).await?.into_inner();
        let txn = response.temporal.into_transaction();
        debug!("resolved txn spans: {:?}", txn.resolved_set);
        Ok(txn.resolved_set)
    }

    async fn participate_txn(&self, mut watcher: watch::Receiver<TxnParticipator>) {
        let mut participator = watcher.borrow_and_update().clone();
        let (mut sender, mut receiver) = self.client.open_participate_txn(participator.to_request(), false).await;
        while participator.txn.status == TxnStatus::Pending {
            select! {
                Ok(updated) = watcher.changed()  => {
                    participator.txn.update(&updated.txn);
                    participator.dependents.clear();
                    participator.dependents.extend(updated.dependents.iter().cloned());
                },
                r = receiver.message() => match r {
                    Ok(Some(response)) => {
                        participator.txn.update(&response.txn);
                        debug!("epoch: {}, status: {:?}", participator.txn.epoch(), participator.txn.status);
                        self.feedback.send(participator.txn.clone()).ignore();
                        continue;
                    },
                    _ => {
                        (sender, receiver) = self.client.open_participate_txn(participator.to_request(), false).await;
                    }
                }
            }
            sender
                .send(ParticipateTxnRequest {
                    txn: participator.txn.clone(),
                    dependents: participator.dependents.clone(),
                })
                .await
                .ignore();
        }
    }

    #[instrument(skip_all, fields(txn.id = %txn.meta.id))]
    pub async fn serve_participant(&self, txn: Transaction, mut pendings: UnboundedReceiver<Transaction>) {
        let mut participator = TxnParticipator::new(txn);
        let (publisher, mut participation) = {
            let (publisher, watcher) = watch::channel(participator.clone());
            let participation = self.participate_txn(watcher);
            (publisher, participation)
        };
        while participator.txn.status == TxnStatus::Pending {
            select! {
                Some(pending) = pendings.recv() => match pending.id() == participator.txn.id() {
                    false => participator.update_dependent(pending.meta),
                    true => participator.update_txn(&pending),
                },
                _ = unsafe { Pin::new_unchecked(&mut participation) } => break,
            }
            publisher.publish(participator.clone());
        }
    }
}

struct TabletShards {
    shards: Vec<ShardDescription>,
}

impl From<Vec<ShardDescription>> for TabletShards {
    fn from(shards: Vec<ShardDescription>) -> Self {
        Self { shards }
    }
}

impl TabletShards {
    fn find_key_shard(&self, key: &[u8]) -> Option<&ShardDescription> {
        self.shards.iter().find(|&shard| shard.range.contains(key))
    }

    fn owns_key(&self, key: &[u8]) -> bool {
        self.find_key_shard(key).is_some()
    }
}

struct TxnParticipantTable {
    shards: TabletShards,
    txns: HashMap<TxnId, TxnParticipant>,
    participation: Arc<TxnParticipation>,
}

impl TxnParticipantTable {
    pub fn new(shards: TabletShards, participation: Arc<TxnParticipation>) -> Self {
        Self { shards, txns: Default::default(), participation }
    }

    pub fn add(&mut self, participant: TxnParticipant) {
        self.txns.insert(participant.txn.id(), participant);
    }

    pub fn fence_ts(&mut self, ts: Timestamp) -> Timestamp {
        debug!("fencing ts: {ts}, txns {}", self.txns.len());
        let mut fenced_ts = ts;
        for participant in self.txns.values_mut() {
            fenced_ts = fenced_ts.min(participant.fence_ts(ts));
        }
        debug!("fenced ts: {fenced_ts}");
        fenced_ts
    }

    pub fn update_txn(&mut self, txn: &mut Transaction, self_update: bool) -> Option<Vec<KeySpan>> {
        if let HashEntry::Occupied(mut entry) = self.txns.entry(txn.id()) {
            let participant = entry.get_mut();
            if participant.update_txn(txn) {
                let locks = std::mem::take(&mut participant.txn.write_set);
                if txn.status.is_terminal()
                    && txn.commit_set.is_empty()
                    && (self_update || !self.shards.owns_key(txn.key()))
                {
                    // All done, nothing for us to resolve now.
                    entry.remove();
                }
                return Some(locks);
            }
        }
        None
    }

    pub fn participate(&mut self, txn: &mut Transaction) -> Result<Option<&mut TxnParticipant>> {
        match self.txns.entry(txn.id()) {
            HashEntry::Vacant(entry) => {
                let coordinator = self.shards.owns_key(txn.key());
                if coordinator {
                    txn.heartbeat(self.participation.clock.now());
                    if txn.is_terminal() && txn.commit_set.is_empty() {
                        return Ok(None);
                    }
                } else if txn.is_terminal() {
                    if !txn.commit_set.is_empty() {
                        bail!("");
                    }
                    return Ok(None);
                }
                let participant = TxnParticipant::new(self.participation.clone(), txn.clone(), coordinator);
                Ok(Some(entry.insert(participant)))
            },
            HashEntry::Occupied(entry) => {
                let participant = entry.into_mut();
                participant.heartbeat_txn(txn)?;
                Ok(Some(participant))
            },
        }
    }

    fn participant_mut(&mut self, txn_id: TxnId) -> Option<&mut TxnParticipant> {
        self.txns.get_mut(&txn_id)
    }

    pub fn resolve_pending_requests(&mut self, pendings: Vec<PendingRequest>) -> Vec<Request> {
        let mut requests = Vec::with_capacity(pendings.len());
        for request in pendings.into_iter() {
            match request {
                PendingRequest::Timestamped(request) => requests.push(request),
                PendingRequest::Transactional(txn_id) => {
                    if let Some(participant) = self.participant_mut(txn_id) {
                        requests.append(&mut participant.pendings);
                    }
                },
            }
        }
        requests
    }

    pub fn participate_txn(
        &mut self,
        request: ParticipateTxnRequest,
        channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
    ) {
        let Some(participant) = self.participant_mut(request.txn.id()) else {
            if self.shards.owns_key(request.txn.key()) {
                let mut txn = request.txn;
                txn.expire(self.participation.clock.now());
                let (_requester, responser) = channel.into_splits();
                responser.send(ParticipateTxnResponse { txn, dependents: vec![] });
            }
            return;
        };
        let Some(coordination) = participant.coordination.as_ref() else {
            return;
        };
        coordination.send(TxnCoordination { request, channel }).ignore();
    }
}

pub struct TxnTable {
    txns: TxnParticipantTable,
    locks: LockTable,
    fences: FenceTable,
}

impl TxnTable {
    pub fn new(
        clock: Clock,
        client: TabletClient,
        shards: Vec<ShardDescription>,
        transactions: impl Iterator<Item = Transaction>,
    ) -> (Self, UnboundedReceiver<Transaction>) {
        let shards = TabletShards::from(shards);
        let (sender, receiver) = mpsc::unbounded_channel();
        let participation = Arc::new(TxnParticipation::new(clock, client, sender));
        let mut locks = LockTable::default();
        let mut txns = TxnParticipantTable::new(shards, participation);
        for txn in transactions {
            let coordinator = txns.shards.owns_key(txn.key());
            let participant = TxnParticipant::new(txns.participation.clone(), txn, coordinator);
            for span in &participant.txn.write_set {
                locks.add_locked(span.clone(), &participant);
            }
            txns.add(participant);
        }
        (Self { txns, locks, fences: FenceTable::default() }, receiver)
    }

    pub fn sequence(&mut self, mut request: Request) -> Option<Request> {
        match &mut request.temporal {
            Temporal::Transaction(txn) => {
                if txn.status != TxnStatus::Aborted {
                    let commit_ts = txn.commit_ts();
                    let bumped_ts = self.fences.min_write_ts(txn.id(), &request.write_keys, txn.commit_ts());
                    if bumped_ts != commit_ts {
                        txn.status = TxnStatus::Pending;
                        txn.commit_ts = bumped_ts;
                        txn.commit_set.clear();
                    }
                }
                let epoch = txn.epoch();
                let participate = match self.txns.participate(txn) {
                    Ok(participate) => participate,
                    Err(err) => {
                        request.responser.send(Err(err)).ignore();
                        return None;
                    },
                };
                if epoch != txn.epoch() || txn.is_aborted() {
                    request.read_keys.clear();
                    request.write_keys.clear();
                    request.requests.clear();
                }
                let Some(participant) = participate else {
                    return Some(request);
                };
                self.locks.sequence_txn(participant, request)
            },
            Temporal::Timestamp(ts) => {
                *ts = self.fences.min_write_ts(Uuid::nil(), &request.write_keys, *ts);
                self.locks.sequence_timestamped(request)
            },
        }
    }

    #[instrument(skip(self))]
    pub fn update_txn(&mut self, txn: &mut Transaction, self_update: bool) -> Option<Vec<Request>> {
        if let Some(locks) = self.txns.update_txn(txn, self_update) {
            let requests = self.locks.unlock(&txn.meta, locks);
            let requests = self.txns.resolve_pending_requests(requests);
            return Some(requests);
        }
        None
    }

    pub fn participate_txn(
        &mut self,
        request: ParticipateTxnRequest,
        channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
    ) {
        self.txns.participate_txn(request, channel)
    }

    pub fn close_ts(&mut self, ts: Timestamp) -> Timestamp {
        let ts = self.txns.fence_ts(ts);
        self.fences.close_ts(ts);
        ts
    }

    pub fn fence_reads(&mut self, txn_id: Uuid, spans: Vec<KeySpan>, ts: Timestamp) {
        for span in spans {
            self.fences.fence(txn_id, span, ts);
        }
    }
}
