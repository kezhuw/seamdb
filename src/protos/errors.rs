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

use std::error::Error;
use std::fmt::{Display, Formatter, Result};

use super::*;

impl Display for ConflictWriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match &self.transient {
            Transient::Timestamp(ts) => write!(f, "conflict with write to key {:?} at {ts}", self.key),
            Transient::Transaction(txn) => {
                write!(f, "conflict with write to key {:?} from {}", self.key, txn)
            },
        }
    }
}

impl Error for ConflictWriteError {}

impl Display for DataTypeMismatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "expect key {:?} has type {:?}, but get {:?}", self.key, self.expect, self.actual)
    }
}

impl Error for DataTypeMismatchError {}

impl Display for ShardNotFoundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self.shard_id {
            0 => write!(f, "find no shard for key {:?}", self.key),
            shard_id => write!(f, "can not find shard {} for key {:?}", shard_id, self.key),
        }
    }
}

impl Error for ShardNotFoundError {}

impl Display for TimestampMismatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "key {:?} get overwritten at timestamp {}", self.key, self.actual)
    }
}

impl Error for TimestampMismatchError {}

impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(&self.message)
    }
}

impl Error for StoreError {}

impl Display for SimpleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(&self.message)
    }
}

impl Error for SimpleError {}

impl Display for DataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::ConflictWrite(err) => err.fmt(f),
            Self::MismatchDataType(err) => err.fmt(f),
            Self::ShardNotFound(err) => err.fmt(f),
            Self::TimestampMismatch(err) => err.fmt(f),
            Self::Store(err) => err.fmt(f),
            Self::Internal(err) => err.fmt(f),
        }
    }
}

impl Error for DataError {}

impl DataError {
    pub fn shard_not_found(key: impl Into<Vec<u8>>, shard_id: ShardId) -> Self {
        Self::ShardNotFound(ShardNotFoundError { key: key.into(), shard_id: shard_id.into() })
    }

    pub fn conflict_write(key: impl Into<Vec<u8>>, transient: impl Into<Transient>) -> Self {
        Self::ConflictWrite(ConflictWriteError { key: key.into(), transient: transient.into() })
    }

    pub fn timestamp_mismatch(key: impl Into<Vec<u8>>, actual: Timestamp) -> Self {
        Self::TimestampMismatch(TimestampMismatchError { key: key.into(), actual })
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(SimpleError { message: message.into() })
    }
}

impl Display for BatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self.request_index {
            None => write!(f, "batch request failed due to {}", self.error),
            Some(i) => {
                write!(f, "{}th request in batch failed due to {}", i, self.error)
            },
        }
    }
}

impl Error for BatchError {}

impl BatchError {
    pub fn new(error: DataError) -> Self {
        Self { request_index: None, error }
    }

    pub fn with_index(request_index: usize, error: DataError) -> Self {
        Self { request_index: Some(request_index as u32), error }
    }

    pub fn with_message(msg: impl Into<String>) -> Self {
        Self { request_index: None, error: DataError::Internal(SimpleError { message: msg.into() }) }
    }
}
