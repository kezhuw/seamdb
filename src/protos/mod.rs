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
use std::cmp;

use anyhow::{anyhow, bail, Result};

pub use self::data_message::Operation as DataOperation;
pub use self::generated::*;
pub use self::tablet_service_client::TabletServiceClient;
pub use self::tablet_service_server::{TabletService, TabletServiceServer};
pub use crate::keys;

impl From<ClusterMeta> for TabletDeployment {
    fn from(meta: ClusterMeta) -> Self {
        let tablet = TabletDescriptor {
            id: 1,
            generation: 0,
            range: TabletRange { start: keys::ROOT_KEY_PREFIX.to_owned(), end: keys::RANGE_KEY_PREFIX.to_owned() },
            log: meta.log,
            merge_bounds: TabletMergeBounds::None,
        };
        Self { tablet, epoch: meta.epoch, generation: meta.generation, servers: meta.servers }
    }
}

impl From<&ClusterMeta> for TabletDeployment {
    fn from(meta: &ClusterMeta) -> Self {
        let tablet = TabletDescriptor {
            id: 1,
            generation: 0,
            range: TabletRange { start: keys::ROOT_KEY_PREFIX.to_owned(), end: keys::RANGE_KEY_PREFIX.to_owned() },
            log: meta.log.clone(),
            merge_bounds: TabletMergeBounds::None,
        };
        TabletDeployment { tablet, epoch: meta.epoch, generation: meta.generation, servers: meta.servers.clone() }
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
}

impl DataRequest {
    pub fn is_read(&self) -> bool {
        matches!(self, DataRequest::Get(_) | DataRequest::Find(_))
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
        Self { epoch, sequence: 0, temporal: None, closed_timestamp: None, leader_expiration: None, operation: None }
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
    pub fn into_one(mut self) -> Result<DataResponse, BatchResponse> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        Ok(self.responses.remove(0))
    }

    pub fn into_find(mut self) -> Result<FindResponse, BatchResponse> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        match self.responses.remove(0) {
            DataResponse::Find(find) => Ok(find),
            response => {
                self.responses.push(response);
                Err(self)
            },
        }
    }

    pub fn into_put(mut self) -> Result<PutResponse, BatchResponse> {
        if self.responses.len() != 1 {
            return Err(self);
        }
        match self.responses.remove(0) {
            DataResponse::Put(put) => Ok(put),
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
}
