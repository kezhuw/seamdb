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

use std::cmp::Ordering::*;

use crate::protos::{KeySpan, KeySpanRef, SpanOrdering, Timestamp, Uuid};

#[derive(Debug, Clone, Copy)]
struct Barrier {
    txn_id: Uuid,
    closed_ts: Timestamp,
}

impl Barrier {
    fn new(txn_id: Uuid, closed_ts: Timestamp) -> Self {
        Self { txn_id, closed_ts }
    }

    fn write_ts(&self, txn_id: Uuid, write_ts: Timestamp) -> Timestamp {
        if write_ts > self.closed_ts {
            return write_ts;
        }
        if txn_id.is_nil() || txn_id != self.txn_id {
            self.closed_ts.next()
        } else {
            self.closed_ts
        }
    }

    fn update(&mut self, txn_id: Uuid, closed_ts: Timestamp) {
        match closed_ts.cmp(&self.closed_ts) {
            Less => {},
            Equal => self.txn_id = self.txn_id.xor(txn_id),
            Greater => {
                self.txn_id = txn_id;
                self.closed_ts = closed_ts;
            },
        }
    }

    fn merge(&mut self, barrier: Barrier) {
        self.update(barrier.txn_id, barrier.closed_ts)
    }
}

#[derive(Debug)]
struct Fence {
    span: KeySpan,
    barrier: Barrier,
}

impl Fence {
    pub fn new(span: KeySpan, txn_id: Uuid, fence_ts: Timestamp) -> Self {
        Self { span, barrier: Barrier::new(txn_id, fence_ts) }
    }

    pub fn split(&mut self, pivot: Vec<u8>) -> Self {
        let mut end = pivot;
        std::mem::swap(&mut self.span.end, &mut end);
        let next_span = KeySpan { key: self.span.end.clone(), end };
        Self { span: next_span, barrier: self.barrier }
    }
}

/// Fence table tracks maximum span read timestamp to calculate minimum span write timestamp to
/// prevent write-beneath-read.
#[derive(Default, Debug)]
pub struct FenceTable {
    fences: Vec<Fence>,
    closed_ts: Timestamp,
}

impl FenceTable {
    /// Fences all future writes at or beneath given timestamp.
    pub fn close_ts(&mut self, ts: Timestamp) {
        self.closed_ts = self.closed_ts.max(ts);
    }

    fn min_barrier(&self) -> Barrier {
        Barrier::new(Uuid::max(), self.closed_ts)
    }

    /// Fences future writes to given span at or beneath given timestamp.
    pub fn fence(&mut self, txn_id: Uuid, mut span: KeySpan, ts: Timestamp) {
        if ts <= self.closed_ts {
            return;
        }
        let mut i = self.fences.partition_point(|fence| fence.span.is_before(&span.key));
        self.clear_closed_fences(i);
        while i < self.fences.len() {
            let fence = &mut self.fences[i];
            match span.as_ref().compare(fence.span.as_ref()) {
                SpanOrdering::LessDisjoint | SpanOrdering::LessContiguous => {
                    self.fences.push(Fence::new(span, txn_id, ts));
                    return;
                },
                SpanOrdering::GreaterDisjoint | SpanOrdering::GreaterContiguous => unreachable!(),
                SpanOrdering::Equal => {
                    fence.barrier.update(txn_id, ts);
                    return;
                },
                SpanOrdering::SubsetAll | SpanOrdering::SubsetLeft | SpanOrdering::SubsetRight
                    if ts < fence.barrier.closed_ts =>
                {
                    return
                },
                SpanOrdering::SubsetAll => {
                    let mut merging_fence = fence.split(span.key);
                    let next_fence = merging_fence.split(span.end);
                    merging_fence.barrier.update(txn_id, ts);
                    self.fences.insert(i + 1, next_fence);
                    self.fences.insert(i + 1, merging_fence);
                    return;
                },
                SpanOrdering::SubsetLeft => {
                    let next_fence = fence.split(span.into_end());
                    fence.barrier.update(txn_id, ts);
                    self.fences.insert(i + 1, next_fence);
                    return;
                },
                SpanOrdering::SubsetRight => {
                    let mut merging_fence = fence.split(span.key);
                    merging_fence.barrier.update(txn_id, ts);
                    self.fences.insert(i + 1, merging_fence);
                    return;
                },
                // https://github.com/rust-lang/rust-clippy/issues/13087
                #[allow(clippy::assigning_clones)]
                SpanOrdering::IntersectRight => {
                    let pivot = span.end;
                    span.end = fence.span.key.clone();
                    if ts >= fence.barrier.closed_ts {
                        let next_fence = fence.split(pivot);
                        fence.barrier.update(txn_id, ts);
                        self.fences.insert(i + 1, next_fence);
                    }
                    self.fences.insert(i, Fence::new(span, txn_id, ts));
                    return;
                },
                #[allow(clippy::assigning_clones)]
                SpanOrdering::IntersectLeft => {
                    if ts >= fence.barrier.closed_ts {
                        let pivot = span.key;
                        span.key = fence.span.end.clone();
                        let mut merging_fence = fence.split(pivot);
                        merging_fence.barrier.update(txn_id, ts);
                        self.fences.insert(i + 1, merging_fence);
                        i += 2;
                    } else {
                        span.key.clone_from(&fence.span.end);
                        i += 1;
                    }
                },
                SpanOrdering::ContainRight => {
                    fence.barrier.update(txn_id, ts);
                    span.end.clone_from(&fence.span.key);
                    self.fences.insert(i, Fence::new(span, txn_id, ts));
                    return;
                },
                SpanOrdering::ContainLeft => {
                    fence.barrier.update(txn_id, ts);
                    span.key.clone_from(&fence.span.end);
                    i += 1;
                },
                SpanOrdering::ContainAll => {
                    let prior_fence = Fence::new(KeySpan::new_range(span.key, fence.span.key.clone()), txn_id, ts);
                    span.key = fence.span.end().into_owned();
                    fence.barrier.update(txn_id, ts);
                    self.fences.insert(i, prior_fence);
                    i += 2;
                },
            }
            self.clear_closed_fences(i);
        }
        if i == self.fences.len() {
            self.fences.push(Fence::new(span, txn_id, ts));
        }
    }

    pub fn min_write_ts(&mut self, txn_id: Uuid, writes: &[KeySpan], mut ts: Timestamp) -> Timestamp {
        for write in writes {
            let barrier = self.max_barrier(write.as_ref());
            ts = barrier.write_ts(txn_id, ts);
        }
        ts
    }

    fn max_barrier(&mut self, span: KeySpanRef<'_>) -> Barrier {
        let mut i = self.fences.partition_point(|fence| fence.span.is_before(span.key));
        self.clear_closed_fences(i);
        let mut max_barrier = self.min_barrier();
        while i < self.fences.len() {
            let fence = &self.fences[i];
            match span.compare(fence.span.as_ref()) {
                SpanOrdering::Equal
                | SpanOrdering::SubsetAll
                | SpanOrdering::SubsetLeft
                | SpanOrdering::SubsetRight
                | SpanOrdering::IntersectRight
                | SpanOrdering::ContainRight => {
                    max_barrier.merge(fence.barrier);
                    break;
                },
                SpanOrdering::LessDisjoint | SpanOrdering::LessContiguous => break,
                SpanOrdering::GreaterDisjoint | SpanOrdering::GreaterContiguous => unreachable!(),
                SpanOrdering::IntersectLeft | SpanOrdering::ContainLeft | SpanOrdering::ContainAll => {
                    max_barrier.merge(fence.barrier);
                    i += 1;
                },
            }
            self.clear_closed_fences(i);
        }
        max_barrier
    }

    fn clear_closed_fences(&mut self, i: usize) {
        while i < self.fences.len() {
            if self.fences[i].barrier.closed_ts > self.closed_ts {
                break;
            }
            self.fences.remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assertor::*;

    use super::FenceTable;
    use crate::protos::{KeySpan, Timestamp, Uuid};

    #[test]
    fn default() {
        let ts10 = Timestamp::ZERO + Duration::from_secs(10);
        let ts50 = Timestamp::ZERO + Duration::from_secs(50);
        let ts55 = Timestamp::ZERO + Duration::from_secs(55);
        let ts60 = Timestamp::ZERO + Duration::from_secs(60);
        let ts65 = Timestamp::ZERO + Duration::from_secs(65);
        let ts70 = Timestamp::ZERO + Duration::from_secs(70);
        let txn1 = Uuid::new_random();
        let txn2 = Uuid::new_random();
        let txn3 = Uuid::new_random();

        let mut fences = FenceTable::default();
        assert_that!(fences.min_write_ts(Uuid::nil(), &vec![KeySpan::new_key(b"k1")], Timestamp::ZERO))
            .is_equal_to(Timestamp::ZERO.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &vec![KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts10);
        assert_that!(fences.min_write_ts(Uuid::new_random(), &vec![KeySpan::new_key(b"k1")], Timestamp::ZERO))
            .is_equal_to(Timestamp::ZERO.next());
        assert_that!(fences.min_write_ts(Uuid::new_random(), &vec![KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts10);

        // given: close ts50
        fences.close_ts(ts50);

        // then: min write ts advances to ts50.next()
        assert_that!(fences.min_write_ts(Uuid::nil(), &vec![KeySpan::new_key(b"k1")], Timestamp::ZERO))
            .is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &vec![KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(Uuid::new_random(), &vec![KeySpan::new_key(b"k1")], Timestamp::ZERO))
            .is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(Uuid::new_random(), &vec![KeySpan::new_key(b"k1")], ts10))
            .is_equal_to(ts50.next());

        // given:
        //   * close ts50
        //   * fence txn1 [k1, k2)    => ts60
        fences.fence(txn1, KeySpan::new_range("k1", "k2"), ts60);
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k1", b"k12")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k12", b"k15")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k12", b"k3")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k1", b"k12")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k12", b"k15")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k12", b"k3")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k2", b"k3")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k1", b"k12")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k12", b"k15")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k12", b"k3")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k2", b"k3")], ts10)).is_equal_to(ts50.next());

        // given:
        //   * close ts50
        //   * fence txn1 [k1, k2)    => ts60
        //   * fence txn2 [k12, k19)  => ts55
        // then: nothing changed
        fences.fence(txn2, KeySpan::new_range("k12", "k17"), ts55);
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k1", b"k12")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k12", b"k15")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k12", b"k3")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k1", b"k12")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k12", b"k15")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k12", b"k3")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k2", b"k3")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k1", b"k12")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k12", b"k15")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k12", b"k3")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k2", b"k3")], ts10)).is_equal_to(ts50.next());

        // given:
        //   * close ts50
        //   * fence txn1 [k1, k2)    => ts60
        //   * fence txn2 [k12, k19)  => ts55
        //   * fence txn2 [k12, k19)  => ts70
        // then: nothing changed
        fences.fence(txn2, KeySpan::new_range("k12", "k17"), ts70);
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k1", b"k12")], ts10))
            .is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k12", b"k15")], ts10))
            .is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k12", b"k3")], ts10))
            .is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k1", b"k12")], ts10)).is_equal_to(ts60);
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k12", b"k15")], ts10)).is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k12", b"k3")], ts10)).is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k2", b"k3")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_key(b"k0")], ts10)).is_equal_to(ts50.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_key(b"k1")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k1", b"k12")], ts10)).is_equal_to(ts60.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k12", b"k15")], ts10)).is_equal_to(ts70);
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k12", b"k3")], ts10)).is_equal_to(ts70);
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k2", b"k3")], ts10)).is_equal_to(ts50.next());

        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k0", b"k3")], ts65))
            .is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k0", b"k3")], ts65)).is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k0", b"k3")], ts65)).is_equal_to(ts70);
        assert_that!(fences.min_write_ts(txn3, &[KeySpan::new_range(b"k0", b"k3")], ts65)).is_equal_to(ts70.next());

        fences.fence(txn3, KeySpan::new_range("k20", "k4"), ts70);
        assert_that!(fences.min_write_ts(Uuid::nil(), &[KeySpan::new_range(b"k16", b"k21")], ts70))
            .is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn1, &[KeySpan::new_range(b"k16", b"k21")], ts70)).is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn2, &[KeySpan::new_range(b"k16", b"k21")], ts70)).is_equal_to(ts70.next());
        assert_that!(fences.min_write_ts(txn3, &[KeySpan::new_range(b"k16", b"k21")], ts70)).is_equal_to(ts70.next());
    }
}
