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

use anyhow::{bail, Result};

pub const ROOT_KEY_PREFIX: &[u8] = &[b'r', b'1'];
pub const RANGE_KEY_PREFIX: &[u8] = &[b'r', b'2'];
pub const DATA_KEY_PREFIX: &[u8] = &[b'd'];
pub const USER_KEY_PREFIX: &[u8] = &[b'd', b'u'];
pub const SYSTEM_KEY_PREFIX: &[u8] = &[b'd', b's'];
pub const MAX_KEY: &[u8] = &[0xffu8];

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum KeyKind {
    Range { root: bool },
    Data { system: bool },
}

pub fn identify_key(key: &[u8]) -> Result<(KeyKind, &[u8])> {
    let n = key.len();
    if n < 3 {
        bail!("invalid key: {:?}", key)
    }
    Ok(match (key[0], key[1], &key[2..]) {
        (b'r', b'1', key) => (KeyKind::Range { root: true }, key),
        (b'r', b'2', key) => (KeyKind::Range { root: false }, key),
        (b'd', b'u', key) => (KeyKind::Data { system: false }, key),
        (b'd', b's', key) => (KeyKind::Data { system: true }, key),
        _ => bail!("invalid key: {:?}", key),
    })
}

pub fn root_key(key: &[u8]) -> Vec<u8> {
    let mut rooted_key = Vec::with_capacity(ROOT_KEY_PREFIX.len() + key.len());
    rooted_key.extend(ROOT_KEY_PREFIX.iter());
    rooted_key.extend(key.iter());
    rooted_key
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
