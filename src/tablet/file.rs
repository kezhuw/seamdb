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

use std::pin::Pin;

use bytes::{Buf, BufMut};
use futures::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use thiserror::Error;

use crate::fs::{FileError, FileReader, FileWriter};
use crate::protos::{PlainValue, Timestamp, Transaction, TxnIntent, Uuid};

const MAGIC_NUMBER: &str = "b112dc2cf3358be76f047b5703f60d3554a3290e118677c1c2bd002dae1dadcd";
const HEADER_MAGIC_KEY: &str = "seamdb_header_magic_number";
const FOOTER_MAGIC_KEY: &str = "seamdb_footer_magic_number";

#[derive(Debug, Error)]
pub enum CompactedFileError {
    #[error("corrupted: {0}")]
    Corrupted(String),
    #[error("{0}")]
    FileError(#[from] FileError),
    #[error("{0}")]
    Anyhow(anyhow::Error),
}

impl From<std::io::Error> for CompactedFileError {
    fn from(err: std::io::Error) -> Self {
        Self::FileError(err.into())
    }
}

impl From<anyhow::Error> for CompactedFileError {
    fn from(err: anyhow::Error) -> Self {
        Self::Anyhow(err)
    }
}

pub struct CompactedFileWriter {
    buf: Vec<u8>,
    file: Pin<Box<dyn FileWriter>>,
}

unsafe impl Send for CompactedFileWriter {}

impl CompactedFileWriter {
    pub async fn new(file: Box<dyn FileWriter>) -> Result<Self, CompactedFileError> {
        let mut writer = Self { buf: Default::default(), file: file.into() };
        writer.append(HEADER_MAGIC_KEY.as_bytes(), MAGIC_NUMBER.as_bytes()).await?;
        Ok(writer)
    }

    pub async fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), CompactedFileError> {
        let n = key.len() + value.len() + 4;
        self.buf.clear();
        self.buf.reserve(n + 4);
        self.buf.put_u32(n as u32);
        self.buf.put_u32(key.len() as u32);
        self.buf.put_slice(key);
        self.buf.put_slice(value);
        self.file.write_all(self.buf.as_slice()).await?;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), CompactedFileError> {
        self.append(FOOTER_MAGIC_KEY.as_bytes(), MAGIC_NUMBER.as_bytes()).await
    }
}

#[derive(Debug)]
pub enum CompactedFileEntry {
    Transaction(Transaction),
    WriteIntent { key: Vec<u8>, intent: TxnIntent },
    KeyValue { key: Vec<u8>, ts: Timestamp, value: PlainValue },
}

pub struct CompactedFileIterator {
    buf: Vec<u8>,
    end: bool,
    file: Pin<Box<dyn FileReader>>,
}

unsafe impl Send for CompactedFileIterator {}

impl CompactedFileIterator {
    pub async fn read_next(&mut self) -> Result<Option<CompactedFileEntry>, CompactedFileError> {
        match self.read().await? {
            Some((key, value)) => {
                let entry = Self::parse_entry(key, value)?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    fn parse_entry(key: Vec<u8>, value: Vec<u8>) -> Result<CompactedFileEntry, CompactedFileError> {
        if let Some(suffix) = key.strip_prefix(b"0_txns/") {
            let txn_id = match uuid::Uuid::try_parse_ascii(suffix) {
                Ok(id) => Uuid::from(id),
                Err(err) => return Err(CompactedFileError::Corrupted(format!("invalid txn id {:?}: {}", suffix, err))),
            };
            let txn = match Transaction::decode(value.as_slice()) {
                Ok(txn) => txn,
                Err(err) => {
                    return Err(CompactedFileError::Corrupted(format!(
                        "fail to decode txn {} {:?}: {}",
                        txn_id, value, err
                    )))
                },
            };
            return Ok(CompactedFileEntry::Transaction(txn));
        }
        if let Some(key) = key.strip_prefix(b"1_intents/") {
            let Ok(key) = hex_simd::decode_to_vec(key) else {
                return Err(CompactedFileError::Corrupted(format!("fail to decode txn intent key: {:?}", key)));
            };
            let intent = TxnIntent::decode(value.as_slice()).map_err(|err| {
                CompactedFileError::Corrupted(format!("fail to decode txn intent key {:?} {:?}: {}", key, value, err))
            })?;
            return Ok(CompactedFileEntry::WriteIntent { key: key.to_owned(), intent });
        }
        if let Some(key) = key.strip_prefix(b"2_values/") {
            let (key, ts) = Self::decode_value_key(key)?;
            let value = PlainValue::decode(value.as_slice()).map_err(|err| {
                CompactedFileError::Corrupted(format!(
                    "fail to decode timestamped key {:?}/{} {:?}: {}",
                    key, ts, value, err
                ))
            })?;
            return Ok(CompactedFileEntry::KeyValue { key, ts, value });
        }
        Err(CompactedFileError::Corrupted(format!("unknown entry key {:?}", key)))
    }

    fn decode_value_key(key: &[u8]) -> Result<(Vec<u8>, Timestamp), CompactedFileError> {
        if let Ok(key) = std::str::from_utf8(key) {
            if let Some((key, ts)) = key.split_once('/') {
                if let Ok(ts) = ts.parse() {
                    if let Ok(key) = hex_simd::decode_to_vec(key) {
                        return Ok((key, ts));
                    }
                }
            }
        };
        Err(CompactedFileError::Corrupted(format!("fail to decode timestamped key: {:?}", key)))
    }

    pub async fn new(file: Box<dyn FileReader>) -> Result<Self, CompactedFileError> {
        let mut iter = CompactedFileIterator { buf: Default::default(), end: false, file: file.into() };
        let Some((key, value)) = iter.next().await? else {
            return Err(FileError::EmptyFile.into());
        };
        if key != HEADER_MAGIC_KEY.as_bytes() || value != MAGIC_NUMBER.as_bytes() {
            return Err(CompactedFileError::Corrupted(format!(
                "expect header magic ({}, {}), get ({:?}, {:?})",
                HEADER_MAGIC_KEY, MAGIC_NUMBER, key, value
            )));
        }
        Ok(iter)
    }

    pub async fn read(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, CompactedFileError> {
        if self.end {
            return Ok(None);
        }
        let Some((key, value)) = self.next().await? else {
            return Err(CompactedFileError::Corrupted("unexpected eof, no footer magic".to_string()));
        };
        if key == FOOTER_MAGIC_KEY.as_bytes() {
            if value == MAGIC_NUMBER.as_bytes() {
                self.end = true;
                return Ok(None);
            }
            return Err(CompactedFileError::Corrupted(format!(
                "expect footer magic ({}, {}), get ({:?})",
                FOOTER_MAGIC_KEY, MAGIC_NUMBER, key
            )));
        }
        Ok(Some((key, value)))
    }

    fn parse(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, CompactedFileError> {
        if self.buf.len() < 8 {
            return Ok(None);
        }
        let mut buf = self.buf.as_slice();
        let len = buf.get_u32() as usize;
        if len > buf.remaining() {
            return Ok(None);
        }
        let key_len = buf.get_u32() as usize;
        let value_len = len - key_len - 4;
        let mut key = Vec::with_capacity(key_len);
        let mut value = Vec::with_capacity(value_len);
        key.extend_from_slice(&buf.chunk()[..key_len]);
        buf.advance(key_len);
        value.extend_from_slice(&buf.chunk()[..value_len]);
        drop(self.buf.drain(..len + 4));
        Ok(Some((key, value)))
    }

    async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, CompactedFileError> {
        loop {
            if let Some((key, value)) = self.parse()? {
                return Ok(Some((key, value)));
            }
            self.buf.reserve(4096);
            let n = self.file.read(unsafe { std::mem::transmute(self.buf.spare_capacity_mut()) }).await?;
            if n == 0 && !self.buf.is_empty() {
                return Err(CompactedFileError::Corrupted("unexpected eof".to_string()));
            }
            unsafe {
                self.buf.set_len(self.buf.len() + n);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CompactedFileIterator, CompactedFileWriter};
    use crate::endpoint::ResourceUri;
    use crate::fs::{FileSystemManager, MemoryFileSystemFactory};

    #[asyncs::test]
    async fn read_write() {
        let manager = FileSystemManager::from(MemoryFileSystemFactory);
        let uri = ResourceUri::parse("memory://files/a").unwrap();

        let writer = manager.create(&uri).await.unwrap();
        let mut writer = CompactedFileWriter::new(writer).await.unwrap();
        writer.append(b"key_1", b"a").await.unwrap();
        writer.append(b"key_2", b"b").await.unwrap();
        writer.append(b"key_3", b"c").await.unwrap();
        writer.finish().await.unwrap();
        drop(writer);

        let reader = manager.open(&uri).await.unwrap();
        let mut reader = CompactedFileIterator::new(reader).await.unwrap();
        let (key1, value1) = reader.read().await.unwrap().unwrap();
        assert_eq!(key1, b"key_1");
        assert_eq!(value1, b"a");

        let (key2, value2) = reader.read().await.unwrap().unwrap();
        assert_eq!(key2, b"key_2");
        assert_eq!(value2, b"b");

        let (key3, value3) = reader.read().await.unwrap().unwrap();
        assert_eq!(key3, b"key_3");
        assert_eq!(value3, b"c");
    }
}
