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

use std::io::Write;

use anyhow::{bail, Result};

use crate::protos::{KeyRange, TabletId};

pub const TABLET_DESCRIPTOR_KEY_PREFIX: &[u8] = &[b'T', b'D'];
pub const TABLET_DEPLOYMENT_KEY_PREFIX: &[u8] = &[b'T', b'T'];
pub const ROOT_KEY_PREFIX: &[u8] = &[b'a', b'1'];
pub const RANGE_KEY_PREFIX: &[u8] = &[b'a', b'2'];
pub const DATA_KEY_PREFIX: &[u8] = &[b'd'];
pub const SYSTEM_KEY_PREFIX: &[u8] = &[b'd', b's'];
pub const USER_KEY_PREFIX: &[u8] = &[b'd', b'u'];
pub const MAX_KEY: &[u8] = &[0xffu8];

pub type Key = Vec<u8>;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum KeyKind {
    Range { root: bool },
    Data { system: bool },
}

impl KeyKind {
    pub fn is_root(&self) -> bool {
        *self == Self::Range { root: true }
    }

    pub fn is_range(&self) -> bool {
        *self == Self::Range { root: false }
    }
}

pub fn identify_key(key: &[u8]) -> Result<(KeyKind, &[u8])> {
    let n = key.len();
    match n {
        0 => bail!("invalid key: no key"),
        1 => match key[0] {
            b'd' => Ok((KeyKind::Data { system: false }, b"")),
            _ => bail!("invalid key: {:?}", key),
        },
        _ => Ok(match (key[0], key[1], &key[2..]) {
            (b'a', b'1', key) => (KeyKind::Range { root: true }, key),
            (b'a', b'2', key) => (KeyKind::Range { root: false }, key),
            (b'd', b'u', key) => (KeyKind::Data { system: false }, key),
            (b'd', b's', key) => (KeyKind::Data { system: true }, key),
            _ => bail!("invalid key: {:?}", key),
        }),
    }
}

pub fn root_key(key: &[u8]) -> Vec<u8> {
    let mut rooted_key = Vec::with_capacity(ROOT_KEY_PREFIX.len() + key.len());
    rooted_key.extend(ROOT_KEY_PREFIX.iter());
    rooted_key.extend(key.iter());
    rooted_key
}

pub fn descriptor_key(id: TabletId) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(TABLET_DESCRIPTOR_KEY_PREFIX);
    write!(&mut buf, "{}", id).unwrap();
    buf
}

pub fn deployment_key(id: TabletId) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(TABLET_DEPLOYMENT_KEY_PREFIX);
    write!(&mut buf, "{}", id).unwrap();
    buf
}

pub fn descriptor_range() -> KeyRange {
    let start = TABLET_DESCRIPTOR_KEY_PREFIX.to_owned();
    let mut end = Vec::with_capacity(TABLET_DESCRIPTOR_KEY_PREFIX.len() + 1);
    end.extend_from_slice(TABLET_DESCRIPTOR_KEY_PREFIX);
    end.push(b'z');
    KeyRange { start, end }
}

pub fn deployment_range() -> KeyRange {
    let start = TABLET_DEPLOYMENT_KEY_PREFIX.to_owned();
    let mut end = Vec::with_capacity(TABLET_DEPLOYMENT_KEY_PREFIX.len() + 1);
    end.extend_from_slice(TABLET_DEPLOYMENT_KEY_PREFIX);
    end.push(b'z');
    KeyRange { start, end }
}

pub fn range_key(key: &[u8]) -> Vec<u8> {
    let mut ranged_key = Vec::with_capacity(RANGE_KEY_PREFIX.len() + key.len());
    ranged_key.extend(RANGE_KEY_PREFIX.iter());
    ranged_key.extend(key.iter());
    ranged_key
}

pub fn user_key(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(USER_KEY_PREFIX.len() + key.len());
    buf.extend(USER_KEY_PREFIX.iter());
    buf.extend(key.iter());
    buf
}

pub fn system_key(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SYSTEM_KEY_PREFIX.len() + key.len());
    buf.extend(SYSTEM_KEY_PREFIX.iter());
    buf.extend(key.iter());
    buf
}

#[cfg(test)]
mod tests {
    use assertor::*;

    use crate::keys::*;

    #[test]
    fn test_keys_invariants() {
        assert_that!(ROOT_KEY_PREFIX).is_less_than(RANGE_KEY_PREFIX);
        assert_that!(RANGE_KEY_PREFIX).is_less_than(DATA_KEY_PREFIX);
        assert_that!(DATA_KEY_PREFIX).is_less_than(SYSTEM_KEY_PREFIX);
        assert_that!(SYSTEM_KEY_PREFIX).is_less_than(USER_KEY_PREFIX);
        assert_that!(USER_KEY_PREFIX).is_less_than(MAX_KEY);

        assert_that!(ROOT_KEY_PREFIX.len()).is_equal_to(RANGE_KEY_PREFIX.len());
        assert_that!(USER_KEY_PREFIX.len()).is_equal_to(SYSTEM_KEY_PREFIX.len());

        assert_that!(identify_key(ROOT_KEY_PREFIX).unwrap())
            .is_equal_to((KeyKind::Range { root: true }, b"".as_slice()));
        assert_that!(identify_key(RANGE_KEY_PREFIX).unwrap())
            .is_equal_to((KeyKind::Range { root: false }, b"".as_slice()));
        assert_that!(identify_key(DATA_KEY_PREFIX).unwrap())
            .is_equal_to((KeyKind::Data { system: false }, b"".as_slice()));
        assert_that!(identify_key(USER_KEY_PREFIX).unwrap())
            .is_equal_to((KeyKind::Data { system: false }, b"".as_slice()));
        assert_that!(identify_key(SYSTEM_KEY_PREFIX).unwrap())
            .is_equal_to((KeyKind::Data { system: true }, b"".as_slice()));
    }

    #[test]
    fn test_keys_root_key() {
        let key = root_key(b"x");
        assert_that!(key.as_slice()).is_greater_than(ROOT_KEY_PREFIX);
        assert_that!(key.as_slice()).is_less_than(RANGE_KEY_PREFIX);
        assert_that!(identify_key(&key).unwrap()).is_equal_to((KeyKind::Range { root: true }, b"x".as_slice()));
    }

    #[test]
    fn test_keys_range_key() {
        let key = range_key(b"x");
        assert_that!(key.as_slice()).is_greater_than(RANGE_KEY_PREFIX);
        assert_that!(key.as_slice()).is_less_than(DATA_KEY_PREFIX);
        assert_that!(identify_key(&key).unwrap()).is_equal_to((KeyKind::Range { root: false }, b"x".as_slice()));
    }

    #[test]
    fn test_keys_system_key() {
        let key = system_key(b"x");
        assert_that!(key.as_slice()).is_greater_than(SYSTEM_KEY_PREFIX);
        assert_that!(key.as_slice()).is_less_than(USER_KEY_PREFIX);
        assert_that!(identify_key(&key).unwrap()).is_equal_to((KeyKind::Data { system: true }, b"x".as_slice()));
    }

    #[test]
    fn test_keys_user_key() {
        let key = user_key(b"x");
        assert_that!(key.as_slice()).is_greater_than(USER_KEY_PREFIX);
        assert_that!(key.as_slice()).is_less_than(MAX_KEY);
        assert_that!(identify_key(&key).unwrap()).is_equal_to((KeyKind::Data { system: false }, b"x".as_slice()));
    }
}
