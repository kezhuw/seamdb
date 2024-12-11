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
use std::cmp::Ordering::{self, *};

use super::*;

impl KeyRange {
    pub fn is_intersect_with(&self, other: &KeyRange) -> bool {
        !(self.end <= other.start || other.end <= self.start)
    }

    pub fn compare(&self, key: &[u8]) -> Ordering {
        if self.end.as_slice() <= key {
            Less
        } else if key < self.start.as_slice() {
            Greater
        } else {
            Equal
        }
    }

    pub fn resume_from(&self, mut end: Vec<u8>) -> Vec<u8> {
        match self.end < end {
            true => {
                end.clone_from(&self.end);
                end
            },
            false => Vec::default(),
        }
    }
}

impl From<KeyRange> for KeySpan {
    fn from(range: KeyRange) -> Self {
        Self { key: range.start, end: range.end }
    }
}

impl KeySpan {
    pub fn new_key(key: impl Into<Vec<u8>>) -> Self {
        Self { key: key.into(), end: vec![] }
    }

    pub fn new_range(key: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self { key: key.into(), end: end.into() }
    }

    pub fn end_key(&self) -> &[u8] {
        self.as_ref().end_key()
    }

    pub fn is_before(&self, key: &[u8]) -> bool {
        if self.end.is_empty() {
            key > self.key.as_slice()
        } else {
            key >= self.end.as_slice()
        }
    }

    pub fn extend_start(&mut self, start: &[u8]) -> bool {
        if start < self.key.as_slice() {
            self.key = start.to_vec();
            true
        } else {
            false
        }
    }

    pub fn as_ref(&self) -> KeySpanRef<'_> {
        KeySpanRef { key: self.key.as_slice(), end: self.end.as_slice() }
    }

    pub fn end(&self) -> Cow<'_, [u8]> {
        if self.end.is_empty() {
            let mut end = Vec::with_capacity(self.key.len() + 1);
            end.extend(&self.key);
            end.push(0);
            Cow::Owned(end)
        } else {
            Cow::Borrowed(&self.end)
        }
    }

    pub fn append_end(&self, end: &mut Vec<u8>) {
        if self.end.is_empty() {
            end.extend(&self.key);
            end.push(0);
        } else {
            end.extend_from_slice(self.end.as_slice());
        }
    }

    pub fn into_end(mut self) -> Vec<u8> {
        match self.end.is_empty() {
            true => {
                self.key.push(0);
                self.key
            },
            false => self.end,
        }
    }
}

#[derive(Clone, Copy)]
pub struct KeySpanRef<'a> {
    pub key: &'a [u8],
    pub end: &'a [u8],
}

#[derive(Debug, PartialEq)]
pub enum SpanOrdering {
    LessDisjoint,
    LessContiguous,

    GreaterDisjoint,
    GreaterContiguous,

    Equal,

    IntersectLeft,
    IntersectRight,

    ContainRight,
    ContainLeft,
    ContainAll,

    SubsetAll,
    SubsetRight,
    SubsetLeft,
}

impl SpanOrdering {
    fn from(ordering: Ordering) -> Self {
        match ordering {
            Ordering::Less => Self::LessDisjoint,
            Ordering::Equal => Self::Equal,
            Ordering::Greater => Self::GreaterDisjoint,
        }
    }

    pub fn reverse(self) -> Self {
        match self {
            SpanOrdering::LessDisjoint => SpanOrdering::GreaterDisjoint,
            SpanOrdering::LessContiguous => SpanOrdering::GreaterContiguous,
            SpanOrdering::GreaterDisjoint => SpanOrdering::LessDisjoint,
            SpanOrdering::GreaterContiguous => SpanOrdering::LessContiguous,
            SpanOrdering::Equal => SpanOrdering::Equal,
            SpanOrdering::IntersectLeft => SpanOrdering::IntersectRight,
            SpanOrdering::IntersectRight => SpanOrdering::IntersectLeft,
            SpanOrdering::ContainRight => SpanOrdering::SubsetRight,
            SpanOrdering::ContainLeft => SpanOrdering::SubsetLeft,
            SpanOrdering::ContainAll => SpanOrdering::SubsetAll,
            SpanOrdering::SubsetRight => SpanOrdering::ContainRight,
            SpanOrdering::SubsetLeft => SpanOrdering::ContainLeft,
            SpanOrdering::SubsetAll => SpanOrdering::ContainAll,
        }
    }
}

impl<'a> KeySpanRef<'a> {
    pub fn new(key: &'a [u8], end: &'a [u8]) -> Self {
        Self { key, end }
    }

    pub fn is_single(&self) -> bool {
        self.end.is_empty()
    }

    pub fn compare(&self, other: KeySpanRef<'_>) -> SpanOrdering {
        match (self.is_single(), other.is_single()) {
            (true, true) => SpanOrdering::from(self.key.cmp(other.key)),
            (true, false) => match self.key.cmp(other.key) {
                Less => SpanOrdering::LessDisjoint,
                Equal => SpanOrdering::SubsetLeft,
                Greater => match self.key.cmp(other.end) {
                    Less => SpanOrdering::SubsetAll,
                    Equal => SpanOrdering::GreaterContiguous,
                    Greater => SpanOrdering::GreaterDisjoint,
                },
            },
            (false, true) => other.compare(*self).reverse(),
            (false, false) => match self.end.cmp(other.key) {
                Less => SpanOrdering::LessDisjoint,
                Equal => SpanOrdering::LessContiguous,
                Greater => match self.key.cmp(other.end) {
                    Equal => SpanOrdering::GreaterContiguous,
                    Greater => SpanOrdering::GreaterDisjoint,
                    Less => match (self.key.cmp(other.key), self.end.cmp(other.end)) {
                        (Less, Less) => SpanOrdering::IntersectRight,
                        (Less, Equal) => SpanOrdering::ContainRight,
                        (Less, Greater) => SpanOrdering::ContainAll,
                        (Equal, Equal) => SpanOrdering::Equal,
                        (Equal, Less) => SpanOrdering::SubsetLeft,
                        (Equal, Greater) => SpanOrdering::ContainLeft,
                        (Greater, Greater) => SpanOrdering::IntersectLeft,
                        (Greater, Equal) => SpanOrdering::SubsetRight,
                        (Greater, Less) => SpanOrdering::SubsetAll,
                    },
                },
            },
        }
    }

    pub fn into_owned(self) -> KeySpan {
        KeySpan { key: self.key.to_vec(), end: self.end.to_vec() }
    }

    pub fn end_key(&self) -> &'a [u8] {
        if self.end.is_empty() {
            self.key
        } else {
            self.end
        }
    }

    pub fn end(&self) -> Cow<'a, [u8]> {
        if self.end.is_empty() {
            let mut end = Vec::with_capacity(self.key.len() + 1);
            end.extend(self.key);
            end.push(0);
            Cow::Owned(end)
        } else {
            Cow::Borrowed(self.end)
        }
    }
}

#[cfg(test)]
mod tests {
    use assertor::*;

    use super::{KeySpanRef, SpanOrdering};

    #[test]
    fn compare() {
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k1", b""))).is_equal_to(SpanOrdering::Equal);
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k2", b"")))
            .is_equal_to(SpanOrdering::LessDisjoint);
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k0", b"")))
            .is_equal_to(SpanOrdering::GreaterDisjoint);

        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k2", b"k20")))
            .is_equal_to(SpanOrdering::LessDisjoint);
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k1", b"k10")))
            .is_equal_to(SpanOrdering::SubsetLeft);
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k0", b"k10")))
            .is_equal_to(SpanOrdering::SubsetAll);
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k0", b"k1")))
            .is_equal_to(SpanOrdering::GreaterContiguous);
        assert_that!(KeySpanRef::new(b"k1", b"").compare(KeySpanRef::new(b"k0", b"k01")))
            .is_equal_to(SpanOrdering::GreaterDisjoint);

        assert_that!(KeySpanRef::new(b"k2", b"k20").compare(KeySpanRef::new(b"k1", b"")))
            .is_equal_to(SpanOrdering::GreaterDisjoint);
        assert_that!(KeySpanRef::new(b"k1", b"k10").compare(KeySpanRef::new(b"k1", b"")))
            .is_equal_to(SpanOrdering::ContainLeft);
        assert_that!(KeySpanRef::new(b"k0", b"k10").compare(KeySpanRef::new(b"k1", b"")))
            .is_equal_to(SpanOrdering::ContainAll);
        assert_that!(KeySpanRef::new(b"k0", b"k1").compare(KeySpanRef::new(b"k1", b"")))
            .is_equal_to(SpanOrdering::LessContiguous);
        assert_that!(KeySpanRef::new(b"k0", b"k01").compare(KeySpanRef::new(b"k1", b"")))
            .is_equal_to(SpanOrdering::LessDisjoint);

        assert_that!(KeySpanRef::new(b"k1", b"k10").compare(KeySpanRef::new(b"k2", b"k20")))
            .is_equal_to(SpanOrdering::LessDisjoint);
        assert_that!(KeySpanRef::new(b"k1", b"k10").compare(KeySpanRef::new(b"k10", b"k20")))
            .is_equal_to(SpanOrdering::LessContiguous);
        assert_that!(KeySpanRef::new(b"k1", b"k10").compare(KeySpanRef::new(b"k0", b"k1")))
            .is_equal_to(SpanOrdering::GreaterContiguous);
        assert_that!(KeySpanRef::new(b"k1", b"k10").compare(KeySpanRef::new(b"k0", b"k00")))
            .is_equal_to(SpanOrdering::GreaterDisjoint);
        assert_that!(KeySpanRef::new(b"k1", b"k11").compare(KeySpanRef::new(b"k10", b"k12")))
            .is_equal_to(SpanOrdering::IntersectRight);
        assert_that!(KeySpanRef::new(b"k1", b"k11").compare(KeySpanRef::new(b"k10", b"k11")))
            .is_equal_to(SpanOrdering::ContainRight);
        assert_that!(KeySpanRef::new(b"k1", b"k12").compare(KeySpanRef::new(b"k10", b"k11")))
            .is_equal_to(SpanOrdering::ContainAll);
        assert_that!(KeySpanRef::new(b"k1", b"k12").compare(KeySpanRef::new(b"k1", b"k12")))
            .is_equal_to(SpanOrdering::Equal);
        assert_that!(KeySpanRef::new(b"k1", b"k12").compare(KeySpanRef::new(b"k1", b"k13")))
            .is_equal_to(SpanOrdering::SubsetLeft);
        assert_that!(KeySpanRef::new(b"k1", b"k12").compare(KeySpanRef::new(b"k1", b"k11")))
            .is_equal_to(SpanOrdering::ContainLeft);
        assert_that!(KeySpanRef::new(b"k10", b"k12").compare(KeySpanRef::new(b"k1", b"k11")))
            .is_equal_to(SpanOrdering::IntersectLeft);
        assert_that!(KeySpanRef::new(b"k10", b"k11").compare(KeySpanRef::new(b"k1", b"k11")))
            .is_equal_to(SpanOrdering::SubsetRight);
        assert_that!(KeySpanRef::new(b"k10", b"k11").compare(KeySpanRef::new(b"k1", b"k12")))
            .is_equal_to(SpanOrdering::SubsetAll);
    }
}
