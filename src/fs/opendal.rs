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

use std::io::Error;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite, SinkExt};
use opendal::services::HdfsNative;
use opendal::{FuturesAsyncReader, FuturesBytesSink, Metadata, Operator};

use super::{FileMeta, FileReader, FileSystem, FileSystemFactory, FileWriter};
use crate::endpoint::{OwnedServiceUri, ResourceUri};

impl From<Metadata> for FileMeta {
    fn from(metadata: Metadata) -> Self {
        Self { last_modified: metadata.last_modified().unwrap().into(), size: metadata.content_length() }
    }
}

struct OpenDALFileSystem {
    uri: OwnedServiceUri,
    operator: Operator,
    shared: Weak<OpenDALFileSystem>,
}

impl OpenDALFileSystem {
    fn new(uri: OwnedServiceUri, operator: Operator) -> Arc<OpenDALFileSystem> {
        let mut fs = Arc::new(OpenDALFileSystem { uri, operator, shared: Weak::new() });
        Arc::get_mut(&mut fs).unwrap().shared = Arc::downgrade(&fs);
        fs
    }
}

#[async_trait]
impl FileSystem for OpenDALFileSystem {
    fn location(&self) -> ResourceUri<'_> {
        self.uri.resource()
    }

    async fn open(&self, path: &str) -> Result<Box<dyn FileReader>> {
        let reader = self.operator.reader(path).await?;
        let reader = reader.into_futures_async_read(..).await?;
        Ok(Box::new(OpenDALFileReader { reader }))
    }

    async fn stat(&self, path: &str) -> Result<FileMeta> {
        let stat = self.operator.stat(path).await?;
        Ok(stat.into())
    }

    async fn create(&self, path: &str) -> Result<Box<dyn FileWriter>> {
        let writer = self.operator.writer_with(path).if_not_exists(true).await?;
        Ok(Box::new(OpenDALFileWriter { sink: writer.into_bytes_sink() }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.operator.delete(path).await?;
        Ok(())
    }
}

pub struct OpenDALFileSystemFactory;

#[async_trait]
impl FileSystemFactory for OpenDALFileSystemFactory {
    fn schemes(&self) -> &[&'static str] {
        &["hdfs"]
    }

    async fn open(&self, uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>> {
        match uri.scheme() {
            "hdfs" => {
                let operator =
                    Operator::new(HdfsNative::default().root(uri.path()).name_node(&uri.endpoint().to_string()))?
                        .finish();
                Ok(OpenDALFileSystem::new(uri, operator))
            },
            _ => bail!("unsupported filesystem {}", uri),
        }
    }
}

pub struct OpenDALFileReader {
    reader: FuturesAsyncReader,
}

impl AsyncRead for OpenDALFileReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, Error>> {
        let reader = unsafe { Pin::new_unchecked(&mut self.reader) };
        reader.poll_read(cx, buf)
    }
}

impl FileReader for OpenDALFileReader {}

pub struct OpenDALFileWriter {
    sink: FuturesBytesSink,
}

impl AsyncWrite for OpenDALFileWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        match self.sink.poll_ready_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(_)) => {
                self.sink.start_send_unpin(buf.to_vec().into())?;
                Poll::Ready(Ok(buf.len()))
            },
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.sink.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.sink.poll_close_unpin(cx)
    }
}

impl FileWriter for OpenDALFileWriter {}
