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

use std::future::Future;
use std::io::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::{Buf, BufMut};
use bytesize::ByteSize;
use futures::{AsyncRead, AsyncWrite, FutureExt};

use super::{ByteLogProducer, ByteLogSubscriber, LogClient, LogManager, LogOffset, LogPosition};
use crate::endpoint::{OwnedServiceUri, ResourceUri};
use crate::fs::{FileMeta, FileReader, FileSystem, FileSystemFactory, FileWriter};

pub struct LogFileSystemFactory {
    manager: Arc<LogManager>,
    schemes: Vec<&'static str>,
}

impl LogFileSystemFactory {
    pub fn new(manager: Arc<LogManager>) -> Self {
        let schemes = manager.registry().factories().keys().copied().collect();
        Self { manager, schemes }
    }
}

#[async_trait]
impl FileSystemFactory for LogFileSystemFactory {
    fn schemes(&self) -> &[&'static str] {
        &self.schemes
    }

    async fn open(&self, uri: OwnedServiceUri) -> Result<Arc<dyn FileSystem>> {
        let client = self.manager.registry().new_client(&uri).await?;
        Ok(Arc::new(LogFileSystem::new(client)))
    }
}

pub struct LogFileSystem {
    client: Arc<dyn LogClient>,
}

impl LogFileSystem {
    pub fn new(client: Arc<dyn LogClient>) -> Self {
        Self { client }
    }

    fn decode_file_meta(path: &str, payload: &[u8]) -> Result<FileMeta> {
        if let Some(mut payload) = payload.strip_prefix([0xff, 0xff].as_slice()) {
            let n = payload.get_u16() as usize;
            let mut buf = &payload[..n];
            let last = buf.get_u8() == 1;
            if last {
                let size = buf.get_u64();
                let last_modified = SystemTime::UNIX_EPOCH + Duration::from_millis(buf.get_u64());
                return Ok(FileMeta { size, last_modified });
            }
        }
        bail!("file {} not completed", path)
    }
}

#[async_trait]
impl FileSystem for LogFileSystem {
    fn location(&self) -> ResourceUri<'_> {
        self.client.location()
    }

    async fn open(&self, path: &str) -> Result<Box<dyn FileReader>> {
        let subscriber = self.client.subscribe_log(path, LogOffset::Earliest).await?;
        Ok(Box::new(LogFileReader::new(subscriber)))
    }

    async fn stat(&self, path: &str) -> Result<FileMeta> {
        let mut subscriber = self.client.subscribe_log(path, LogOffset::Latest).await?;
        let position = subscriber.latest().await?;
        subscriber.seek(LogOffset::Position(position)).await?;
        let (_, payload) = subscriber.read().await?;
        Self::decode_file_meta(path, payload)
    }

    async fn create(&self, path: &str) -> Result<Box<dyn FileWriter>> {
        self.client.create_log(path, ByteSize::default()).await.unwrap();
        let producer = self.client.produce_log(path).await.unwrap();
        Ok(Box::new(LogFileWriter::new(producer)))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.client.delete_log(path).await
    }
}

type Subscription = Pin<Box<dyn Future<Output = Result<(LogPosition, &'static [u8])>> + Send>>;

pub struct LogFileReader {
    end: bool,
    payload: Option<&'static [u8]>,
    subscription: Option<Subscription>,
    subscriber: Box<dyn ByteLogSubscriber>,
}

impl LogFileReader {
    fn new(subscriber: Box<dyn ByteLogSubscriber>) -> Self {
        Self { end: false, payload: None, subscription: None, subscriber }
    }

    fn decode_payload(payload: &[u8]) -> Result<(bool, &[u8]), Error> {
        match payload.strip_prefix([0xff, 0xff].as_slice()) {
            None => Ok((false, payload)),
            Some(mut payload) => {
                let n = payload.get_u16() as usize;
                let mut buf = &payload[..n];
                let payload = &payload[n..];
                let last = buf.get_u8() == 1;
                Ok((last, payload))
            },
        }
    }
}

impl AsyncRead for LogFileReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<Result<usize, Error>> {
        if buf.is_empty() || (self.end && self.payload.is_none()) {
            return Poll::Ready(Ok(0));
        }
        loop {
            if let Some(mut payload) = self.payload.take() {
                let n = payload.len().min(buf.len());
                buf.put_slice(&payload[..n]);
                payload = &payload[n..];
                if !payload.is_empty() {
                    self.payload = Some(payload);
                }
                return Poll::Ready(Ok(n));
            }
            if let Some(subscription) = self.subscription.as_mut().map(Pin::as_mut) {
                match subscription.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => {
                        self.subscription = None;
                        return Poll::Ready(Err(Error::other(err)));
                    },
                    Poll::Ready(Ok((_position, payload))) => {
                        self.subscription = None;
                        let (last, payload) = Self::decode_payload(payload)?;
                        self.payload = Some(payload);
                        self.end = last;
                        continue;
                    },
                }
            }
            self.subscription = Some(unsafe { std::mem::transmute(self.subscriber.read().boxed()) });
        }
    }
}

impl FileReader for LogFileReader {}

type Produce = Pin<Box<dyn Future<Output = Result<usize, Error>> + Send>>;

pub struct LogFileWriter {
    buf: Vec<u8>,
    size: u64,
    produce: Option<Produce>,
    producer: Option<Box<dyn ByteLogProducer>>,
}

impl LogFileWriter {
    fn new(producer: Box<dyn ByteLogProducer>) -> Self {
        Self { buf: Vec::default(), size: 0, produce: None, producer: Some(producer) }
    }

    fn prepare_head(&mut self, last: bool) {
        self.buf.clear();
        self.buf.put_u16(0xffff);
        match last {
            false => {
                self.buf.put_u16(1);
                self.buf.put_u8(if last { 1 } else { 0 });
            },
            true => {
                self.buf.put_u16(17);
                self.buf.put_u8(if last { 1 } else { 0 });
                self.buf.put_u64(self.size);

                let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
                self.buf.put_u64(now.as_millis() as u64);
            },
        }
    }

    fn write_log(mut self: Pin<&mut Self>, cx: &mut Context<'_>, last: bool, buf: &[u8]) -> Poll<Result<usize, Error>> {
        loop {
            let n = buf.len();
            if n == 0 && !last {
                return Poll::Ready(Ok(0));
            }
            if let Some(produce) = self.produce.as_mut().map(Pin::as_mut) {
                return match produce.poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        self.produce = None;
                        Poll::Ready(result)
                    },
                };
            }
            // Take it out to circumvent multiple mutable borrows.
            let Some(mut producer) = self.producer.take() else {
                return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
            };
            self.size += buf.len() as u64;
            let payload = if last || buf.strip_prefix([0xff, 0xff].as_slice()).is_some() {
                self.prepare_head(last);
                self.buf.extend_from_slice(buf);
                self.buf.as_slice()
            } else {
                buf
            };
            if let Err(err) = producer.queue(payload) {
                return Poll::Ready(Err(Error::other(err)));
            }
            let produce = async {
                match producer.wait().await {
                    Err(err) => Err(Error::other(err)),
                    Ok(_position) => Ok(n),
                }
            };
            self.produce = Some(unsafe { std::mem::transmute(produce.boxed()) });
            self.producer = Some(producer);
        }
    }
}

impl AsyncWrite for LogFileWriter {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        self.write_log(cx, false, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        ready!(self.as_mut().write_log(cx, true, Default::default()))?;
        self.producer.take();
        Poll::Ready(Ok(()))
    }
}

impl FileWriter for LogFileWriter {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{AsyncReadExt, AsyncWriteExt};

    use super::{FileSystem, FileSystemFactory, LogFileSystemFactory};
    use crate::endpoint::{OwnedServiceUri, ServiceUri};
    use crate::log::kafka::tests::KafkaContainer;
    use crate::log::{KafkaLogFactory, LogRegistry, MemoryLogFactory};

    async fn check_log_file(fs: Arc<dyn FileSystem>) {
        let first_line = "first line\n";
        let second_line = "second line\n";

        fs.location();

        let writer = fs.create("a.txt").await.unwrap();
        let mut writer = Box::into_pin(writer);
        writer.write_all(first_line.as_bytes()).await.unwrap();
        writer.write_all(second_line.as_bytes()).await.unwrap();
        writer.close().await.unwrap();

        let meta = fs.stat("a.txt").await.unwrap();
        assert_eq!(meta.size as usize, first_line.len() + second_line.len());

        let mut reader = Box::into_pin(fs.open("a.txt").await.unwrap());
        let mut buf = Vec::default();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf.as_slice(), format!("{first_line}{second_line}").as_bytes());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn log_based_file() {
        let kafka = KafkaContainer::new();
        let server = format!("kafka://127.0.0.1:{}/files", kafka.get_host_port());
        let kafka_uri: OwnedServiceUri = server.try_into().unwrap();

        let mut registry = LogRegistry::default();
        registry.register(KafkaLogFactory::default()).unwrap();
        registry.register(MemoryLogFactory).unwrap();

        let manager = registry.into_manager(&kafka_uri).await.unwrap();
        let fs_factory = LogFileSystemFactory::new(manager.into());

        check_log_file(fs_factory.open(kafka_uri).await.unwrap()).await;
        check_log_file(fs_factory.open(ServiceUri::parse("memory://memory").unwrap()).await.unwrap()).await;
    }
}
