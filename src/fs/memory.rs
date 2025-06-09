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

use std::collections::hash_map::{Entry, HashMap};
use std::io::Error;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::SystemTime;

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::BufMut;
use futures::{AsyncRead, AsyncWrite};

use super::{FileMeta, FileReader, FileSystem, FileSystemFactory, FileWriter};
use crate::endpoint::{OwnedServiceUri, ResourceUri};

pub struct MemoryFileSystemFactory;

impl MemoryFileSystemFactory {
    pub fn open(uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>> {
        if uri.scheme() != "memory" {
            bail!("unsupported fs scheme: {}", uri.scheme())
        }
        Ok(MemoryFileSystem::new(uri))
    }
}

#[async_trait]
impl FileSystemFactory for MemoryFileSystemFactory {
    fn schemes(&self) -> &[&'static str] {
        &["memory"]
    }

    async fn open(&self, uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>> {
        MemoryFileSystemFactory::open(uri)
    }
}

struct MemoryFile {
    content: Vec<u8>,
    last_modified: SystemTime,
}

impl MemoryFile {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { content: Default::default(), last_modified: SystemTime::now() }))
    }
}

struct MemoryFileReader {
    file: Arc<Mutex<MemoryFile>>,
    offset: usize,
}

impl MemoryFileReader {
    pub fn new(file: Arc<Mutex<MemoryFile>>) -> Self {
        Self { file, offset: 0 }
    }
}

impl AsyncRead for MemoryFileReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<Result<usize, Error>> {
        let file = self.file.lock().unwrap();
        let remainings = file.content.len() - self.offset;
        let n = remainings.min(buf.len());
        if n == 0 {
            return Poll::Ready(Ok(0));
        }
        let end = self.offset + n;
        buf.put_slice(&file.content[self.offset..end]);
        drop(file);
        self.offset = end;
        Poll::Ready(Ok(n))
    }
}

impl FileReader for MemoryFileReader {}

struct MemoryFileWriter {
    file: Arc<Mutex<MemoryFile>>,
    closed: bool,
}

impl MemoryFileWriter {
    fn new(file: Arc<Mutex<MemoryFile>>) -> Self {
        Self { file, closed: false }
    }
}

impl AsyncWrite for MemoryFileWriter {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        let mut file = self.file.lock().unwrap();
        file.content.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.closed = true;
        Poll::Ready(Ok(()))
    }
}

impl FileWriter for MemoryFileWriter {}

pub struct MemoryFileSystem {
    uri: OwnedServiceUri,
    files: Mutex<HashMap<String, Arc<Mutex<MemoryFile>>>>,
}

impl MemoryFileSystem {
    fn new(uri: OwnedServiceUri) -> Arc<Self> {
        Arc::new(Self { uri, files: Mutex::new(HashMap::new()) })
    }
}

#[async_trait]
impl FileSystem for MemoryFileSystem {
    fn location(&self) -> ResourceUri<'_> {
        self.uri.resource()
    }

    async fn open(&self, path: &str) -> Result<Box<dyn FileReader>> {
        let files = self.files.lock().unwrap();
        let file = files.get(path).cloned();
        drop(files);
        match file {
            None => bail!("file not found: {}", path),
            Some(file) => Ok(Box::new(MemoryFileReader::new(file))),
        }
    }

    async fn stat(&self, path: &str) -> Result<FileMeta> {
        let files = self.files.lock().unwrap();
        let file = files.get(path).cloned();
        drop(files);
        match file {
            None => bail!("file not found: {}", path),
            Some(file) => {
                let locked = file.lock().unwrap();
                Ok(FileMeta { size: locked.content.len() as u64, last_modified: locked.last_modified })
            },
        }
    }

    async fn create(&self, path: &str) -> Result<Box<dyn FileWriter>> {
        let mut files = self.files.lock().unwrap();
        match files.entry(path.to_owned()) {
            Entry::Occupied(_) => bail!("file already existed: {}", path),
            Entry::Vacant(entry) => {
                let file = entry.insert(MemoryFile::new()).clone();
                drop(files);
                Ok(Box::new(MemoryFileWriter::new(file)))
            },
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        let _file = files.remove(path);
        drop(files);
        Ok(())
    }
}
