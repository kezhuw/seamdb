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
}

impl From<KeyRange> for KeySpan {
    fn from(range: KeyRange) -> Self {
        Self { key: range.start, end: range.end }
    }
}

impl KeySpan {
    pub fn end_key(&self) -> &[u8] {
        self.as_ref().end_key()
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
}

#[derive(Clone, Copy)]
pub struct KeySpanRef<'a> {
    pub key: &'a [u8],
    pub end: &'a [u8],
}

impl<'a> KeySpanRef<'a> {
    pub fn new(key: &'a [u8], end: &'a [u8]) -> Self {
        Self { key, end }
    }

    pub fn compare(&self, other: KeySpanRef<'_>) -> Ordering {
        match (self.end.is_empty(), other.end.is_empty()) {
            (true, true) => self.key.cmp(other.key),
            (false, true) => match other.key.cmp(self.key) {
                Less => Less,
                Equal => Equal,
                Greater => match other.key.cmp(self.end) {
                    Less => Equal,
                    Equal | Greater => Greater,
                },
            },
            (true, false) => other.compare(*self).reverse(),
            (false, false) => match self.compare(KeySpanRef::new(other.key, Default::default())) {
                Greater => Greater,
                _ => match self.compare(KeySpanRef::new(other.end, Default::default())) {
                    Less => Less,
                    _ => Equal,
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
}
