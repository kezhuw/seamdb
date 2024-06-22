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

//! Generated code for protobuf message and rpc.

#[rustfmt::skip]
mod generated;
mod span;
mod temporal;
mod uuid;

use std::cmp;
use std::fmt::{Display, Error, Formatter};

use anyhow::{anyhow, bail, Result};
use hashbrown::Equivalent;
pub use temporal::{HasTxnMeta, HasTxnStatus};

pub use self::data_message::Operation as DataOperation;
pub use self::generated::*;
pub use self::span::*;
pub use self::tablet_service_client::TabletServiceClient;
pub use self::tablet_service_server::{TabletService, TabletServiceServer};
pub use crate::keys;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TabletId(u64);

impl From<u64> for TabletId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<TabletId> for u64 {
    fn from(id: TabletId) -> Self {
        id.0
    }
}

impl Display for TabletId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_fmt(format_args!("{:#x}", self.0))
    }
}

impl Equivalent<TabletId> for u64 {
    fn equivalent(&self, key: &TabletId) -> bool {
        *self == key.0
    }
}

impl TabletId {
    pub const ROOT: Self = Self(1);

    pub fn into_raw(self) -> u64 {
        self.0
    }

    pub const fn from_raw(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ShardId(pub(crate) u64);

impl Display for ShardId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_fmt(format_args!("{:#x}", self.0))
    }
}

impl From<u64> for ShardId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<ShardId> for u64 {
    fn from(id: ShardId) -> Self {
        id.0
    }
}

impl ShardId {
    pub const DEPLOYMENT: Self = Self(3);
    pub const DESCRIPTOR: Self = Self(2);
    pub const ROOT: Self = Self(1);

    pub const fn from_raw(id: u64) -> Self {
        Self(id)
    }

    pub fn into_raw(self) -> u64 {
        self.0
    }
}

impl ShardDescriptor {
    pub fn root() -> Self {
        Self { id: ShardId::ROOT.into(), range: KeyRange::root(), tablet_id: TabletId::ROOT.into() }
    }

    pub fn descriptor() -> Self {
        Self { id: ShardId::DESCRIPTOR.into(), range: keys::descriptor_range(), tablet_id: TabletId::ROOT.into() }
    }

    pub fn deployment() -> Self {
        Self { id: ShardId::DEPLOYMENT.into(), range: keys::deployment_range(), tablet_id: TabletId::ROOT.into() }
    }

    pub fn is_predecessor_of(&self, other: &ShardDescriptor) -> bool {
        self.id < other.id && self.range.is_intersect_with(&other.range)
    }
}

impl MessageId {
    pub fn new(epoch: u64, sequence: u64) -> Self {
        Self { epoch, sequence }
    }

    pub fn new_fenced(epoch: u64) -> Self {
        Self::new(epoch, 0)
    }

    pub fn advance(&mut self, next: MessageId) -> Result<bool> {
        match next.epoch.cmp(&self.epoch) {
            cmp::Ordering::Less => Ok(false),
            cmp::Ordering::Greater => {
                self.epoch = next.epoch;
                self.sequence = next.sequence;
                Ok(true)
            },
            cmp::Ordering::Equal => {
                if next.sequence <= self.sequence {
                    Ok(false)
                } else if next.sequence != self.sequence + 1 {
                    Err(anyhow!(
                        "can not advance epoch sequence from {}:{} to {}:{}",
                        self.epoch,
                        self.sequence,
                        next.epoch,
                        next.sequence
                    ))
                } else {
                    self.epoch = next.epoch;
                    self.sequence = next.sequence;
                    Ok(true)
                }
            },
        }
    }
}

impl TabletWatermark {
    pub fn new(cursor: MessageId, closed_timestamp: Timestamp, leader_expiration: Timestamp) -> Self {
        Self { cursor, closed_timestamp, leader_expiration }
    }

    pub fn timing(&self) -> (Timestamp, Timestamp) {
        (self.closed_timestamp, self.leader_expiration)
    }
}

impl BatchRequest {
    pub fn is_readonly(&self) -> bool {
        self.requests.iter().all(|r| r.is_read())
    }
}

impl KeyRange {
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self { start: start.into(), end: end.into() }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    pub fn root() -> Self {
        let end = keys::range_key(keys::MAX_KEY);
        let end = keys::root_key(&end);
        Self::new(keys::ROOT_KEY_PREFIX, end)
    }
}

impl TabletDeployment {
    pub fn update(&mut self, epoch: u64, generation: u64, servers: Vec<String>) -> bool {
        if self.epoch < epoch || (self.epoch == epoch && self.generation < generation) {
            self.epoch = epoch;
            self.generation = generation;
            self.servers = servers;
            true
        } else {
            false
        }
    }

    pub fn index(&self, node: &str) -> Option<usize> {
        self.servers.iter().position(|x| x == node)
    }

    pub fn order(&self, other: &Self) -> std::cmp::Ordering {
        (self.epoch, self.generation).cmp(&(other.epoch, other.generation))
    }

    pub fn generation(&self) -> (u64, u64) {
        (self.epoch, self.generation)
    }
}

impl ShardRequest {
    pub fn is_read(&self) -> bool {
        self.request.is_read()
    }

    pub fn key(&self) -> &[u8] {
        self.request.key()
    }
}

impl DataRequest {
    pub fn is_read(&self) -> bool {
        !self.is_write()
    }

    pub fn is_write(&self) -> bool {
        matches!(self, DataRequest::Put(_) | DataRequest::Increment(_))
    }

    pub fn write_key(&self) -> Option<&[u8]> {
        match self {
            Self::Put(request) => Some(&request.key),
            Self::Increment(request) => Some(&request.key),
            _ => None,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get(request) => &request.key,
            Self::Find(request) => &request.key,
            Self::Put(request) => &request.key,
            Self::Increment(request) => &request.key,
            Self::RefreshRead(request) => &request.span.key,
        }
    }

    pub fn set_key(&mut self, key: Vec<u8>) {
        match self {
            Self::Get(request) => request.key = key,
            Self::Find(request) => request.key = key,
            Self::Put(request) => request.key = key,
            Self::Increment(request) => request.key = key,
            Self::RefreshRead(request) => request.span.key = key,
        }
    }
}

impl TabletManifest {
    pub fn update(&mut self, manifest: Option<TabletManifest>) {
        let Some(manifest) = manifest else {
            return;
        };
        *self = manifest;
    }

    pub fn rotate(&mut self) {
        self.obsoleted_logs.append(&mut self.dirty_logs);
        self.obsoleted_files.append(&mut self.dirty_files);
    }

    pub fn update_watermark(&mut self, watermark: TabletWatermark) {
        self.watermark = watermark;
    }
}

impl ManifestMessage {
    pub fn new(epoch: u64, sequence: u64, manifest: TabletManifest) -> Self {
        Self { epoch, sequence, manifest: Some(manifest) }
    }

    pub fn new_fenced(epoch: u64) -> Self {
        Self { epoch, sequence: 0, manifest: None }
    }
}

impl DataMessage {
    pub fn new_fenced(epoch: u64) -> Self {
        Self {
            epoch,
            sequence: 0,
            temporal: Temporal::default(),
            closed_timestamp: None,
            leader_expiration: None,
            operation: None,
        }
    }

    pub fn take_writes(&mut self) -> Vec<Write> {
        match self.operation.take() {
            None => vec![],
            Some(DataOperation::Write(write)) => vec![write],
            Some(DataOperation::Batch(batch)) => batch.writes,
        }
    }
}

impl Value {
    pub fn read_int(&self, key: &[u8], operation: &str) -> Result<i64> {
        match self {
            Value::Int(i) => Ok(*i),
            value => bail!("key {:?}: {} expect int, but got {:?}", key, operation, value),
        }
    }

    pub fn read_bytes(&self, key: &[u8], operation: &str) -> Result<&[u8]> {
        match self {
            Value::Bytes(bytes) => Ok(bytes),
            value => bail!("key {:?}: {} expect bytes, but got {:?}", key, operation, value),
        }
    }

    pub fn from_message(message: &impl prost::Message) -> Value {
        let bytes = message.encode_to_vec();
        Self::Bytes(bytes)
    }

    pub fn into_int(self) -> Result<i64, Self> {
        match self {
            Self::Int(i) => Ok(i),
            _ => Err(self),
        }
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Bytes(bytes) => Ok(bytes),
            _ => Err(self),
        }
    }
}

impl TimestampedValue {
    pub fn read_int(&self, key: &[u8], operation: &str) -> Result<i64> {
        self.value.read_int(key, operation)
    }

    pub fn read_bytes(&self, key: &[u8], operation: &str) -> Result<&[u8]> {
        self.value.read_bytes(key, operation)
    }
}

impl BatchResponse {
    #[allow(clippy::result_large_err)]
    pub fn into_one(mut self) -> Result<ShardResponse, Self> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        Ok(self.responses.remove(0))
    }

    #[allow(clippy::result_large_err)]
    pub fn into_get(mut self) -> Result<GetResponse, Self> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        let ShardResponse { shard, response } = self.responses.remove(0);
        match response.into_get() {
            Ok(get) => Ok(get),
            Err(response) => {
                self.responses.push(ShardResponse { shard, response });
                Err(self)
            },
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn into_find(mut self) -> Result<FindResponse, Self> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        match self.responses.remove(0) {
            ShardResponse { response: DataResponse::Find(find), .. } => Ok(find),
            response => {
                self.responses.push(response);
                Err(self)
            },
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn into_put(mut self) -> Result<PutResponse, Self> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        match self.responses.remove(0) {
            ShardResponse { response: DataResponse::Put(put), .. } => Ok(put),
            response => {
                self.responses.push(response);
                Err(self)
            },
        }
    }
}

impl DataResponse {
    pub fn into_get(self) -> Result<GetResponse, Self> {
        match self {
            Self::Get(get) => Ok(get),
            _ => Err(self),
        }
    }

    pub fn into_find(self) -> Result<FindResponse, Self> {
        match self {
            Self::Find(find) => Ok(find),
            _ => Err(self),
        }
    }

    pub fn into_put(self) -> Result<PutResponse, Self> {
        match self {
            Self::Put(put) => Ok(put),
            _ => Err(self),
        }
    }

    pub fn into_increment(self) -> Result<IncrementResponse, Self> {
        match self {
            Self::Increment(increment) => Ok(increment),
            _ => Err(self),
        }
    }

    pub fn as_find_mut(&mut self) -> Option<&mut FindResponse> {
        match self {
            Self::Find(find) => Some(find),
            _ => None,
        }
    }
}

impl ClusterDescriptor {
    pub fn to_tablet(&self) -> TabletDescriptor {
        TabletDescriptor {
            id: TabletId::ROOT.into(),
            generation: self.generation,
            manifest_log: self.manifest_log.clone(),
        }
    }
}
