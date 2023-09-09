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

//! Persistent log trait and its implementations.

mod kafka;
mod manager;
mod memory;

use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytesize::ByteSize;
use compact_str::CompactString;
use derivative::Derivative;

pub use self::kafka::KafkaLogFactory;
pub use self::manager::{LogManager, LogRegistry};
pub use self::memory::MemoryLogFactory;
use super::endpoint::{Endpoint, Params, ResourceId};

pub type OwnedLogAddress = LogAddress<'static>;

/// Address to a log.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct LogAddress<'a> {
    str: Cow<'a, str>,
    #[derivative(Debug = "ignore")]
    uri: ResourceId<'a>,
}

impl<'a> TryFrom<&'a str> for LogAddress<'a> {
    type Error = anyhow::Error;

    fn try_from(str: &'a str) -> Result<Self> {
        Self::new(str)
    }
}

impl std::ops::Deref for LogAddress<'_> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.str
    }
}

impl std::fmt::Display for LogAddress<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.str)
    }
}

impl PartialEq<&str> for LogAddress<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.str.eq(*other)
    }
}

impl PartialEq<Self> for LogAddress<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.str.eq(&other.str)
    }
}

impl From<LogAddress<'_>> for String {
    fn from(address: LogAddress<'_>) -> Self {
        address.str.into_owned()
    }
}

impl Hash for LogAddress<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.str.as_ref().hash(state)
    }
}

impl Clone for LogAddress<'_> {
    fn clone(&self) -> Self {
        let str = self.str.clone();
        if matches!(str, Cow::Borrowed(_)) {
            return Self { str, uri: self.uri };
        }
        let uri = unsafe { self.uri.relocate(std::mem::transmute(str.as_ref())) };
        Self { str, uri }
    }
}

impl<'a> LogAddress<'a> {
    pub fn new(str: impl Into<Cow<'a, str>>) -> Result<Self> {
        let str = str.into();
        let uri = ResourceId::parse_named("log address", str.as_ref())?;
        if unsafe { uri.path().get_unchecked(1..) }.find('/').is_some() {
            bail!("log address invalid log name: {str}")
        }
        let uri: ResourceId<'a> = unsafe { std::mem::transmute(uri) };
        Ok(Self { str, uri })
    }

    pub fn uri(&self) -> ResourceId<'_> {
        self.uri
    }

    pub fn as_str(&self) -> &str {
        &self.str
    }

    pub fn to_owned(&self) -> LogAddress<'static> {
        self.clone().into_owned()
    }

    pub fn into_owned(self) -> LogAddress<'static> {
        let str = match &self.str {
            Cow::Owned(_) => return unsafe { std::mem::transmute(self) },
            Cow::Borrowed(str) => str.to_string(),
        };
        let str: Cow<'static, str> = Cow::Owned(str);
        let uri = unsafe { self.uri.relocate(std::mem::transmute(str.as_ref())) };
        LogAddress { str, uri }
    }
}

/// Absolute position in log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogPosition {
    Offset(u128),
    Cursor(CompactString),
}

impl LogPosition {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Offset(offset) if *offset <= u64::MAX as u128 => Some(*offset as u64),
            Self::Cursor(str) => match str.parse() {
                Ok(offset) => Some(offset),
                _ => None,
            },
            _ => None,
        }
    }
}

/// Relative position in log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogOffset {
    Earliest,
    Latest,
    Position(LogPosition),
}

impl From<LogPosition> for LogOffset {
    fn from(position: LogPosition) -> Self {
        Self::Position(position)
    }
}

/// Producer to write byte message to log.
#[async_trait]
pub trait ByteLogProducer: Send + std::fmt::Debug + 'static {
    fn queue(&mut self, payload: &[u8]) -> Result<()>;

    async fn wait(&mut self) -> Result<LogPosition>;

    async fn send(&mut self, payload: &[u8]) -> Result<LogPosition> {
        self.queue(payload)?;
        self.wait().await
    }
}

/// Subscriber to read byte message from log.
#[async_trait]
pub trait ByteLogSubscriber: Send + std::fmt::Debug + 'static {
    async fn read(&mut self) -> Result<(LogPosition, &[u8])>;

    async fn seek(&mut self, offset: LogOffset) -> Result<()>;

    async fn latest(&self) -> Result<LogPosition>;
}

/// Client to a log cluster.
#[async_trait]
pub trait LogClient: Send + Sync + std::fmt::Debug + 'static {
    async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>>;

    async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>>;

    async fn create_log(&self, name: &str, retention: ByteSize) -> Result<()>;

    async fn delete_log(&self, name: &str) -> Result<()>;
}

/// Factory to open [LogClient].
#[async_trait]
pub trait LogFactory: Send + Sync + std::fmt::Debug + 'static {
    fn scheme(&self) -> &'static str;

    async fn open_client(&self, endpoint: &Endpoint, params: &Params) -> Result<Arc<dyn LogClient>>;
}

#[cfg(test)]
pub mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use anyhow::{bail, Result};
    use async_trait::async_trait;
    use bytesize::ByteSize;
    use hashbrown::hash_map::HashMap;
    use speculoos::*;
    use test_case::test_case;
    use tokio::sync::watch;

    use crate::endpoint::{Endpoint, OwnedEndpoint, Params};
    use crate::log::{ByteLogProducer, ByteLogSubscriber, LogAddress, LogClient, LogFactory, LogOffset, LogPosition};

    #[derive(Clone, Debug)]
    pub struct TestLog {
        name: String,
        retention: ByteSize,
        messages: Arc<Mutex<Vec<Vec<u8>>>>,
        len: Arc<watch::Sender<usize>>,
    }

    impl TestLog {
        fn new(name: &str, retention: ByteSize) -> Self {
            Self { name: name.to_string(), retention, messages: Default::default(), len: Arc::new(watch::channel(0).0) }
        }

        fn append(&self, message: Vec<u8>) -> Result<LogPosition> {
            let mut messages = self.messages.lock().unwrap();
            let position = messages.len();
            messages.push(message);
            self.len.send_replace(position + 1);
            Ok(LogPosition::Offset(position as u128))
        }

        fn read(&self, i: usize) -> Vec<u8> {
            self.messages.lock().unwrap()[i].clone()
        }

        fn len(&self) -> usize {
            self.messages.lock().unwrap().len()
        }

        fn resolve_offset(&self, offset: LogOffset) -> Result<usize> {
            match offset {
                LogOffset::Earliest => Ok(0),
                LogOffset::Latest => Ok(self.latest()),
                LogOffset::Position(position) => {
                    let offset = position.as_u64().unwrap() as usize;
                    let len = self.len();
                    if offset > len {
                        bail!("seek offset ahead len")
                    }
                    Ok(offset)
                },
            }
        }

        pub fn name(&self) -> &str {
            &self.name
        }

        pub fn retention(&self) -> ByteSize {
            self.retention
        }

        pub fn latest(&self) -> usize {
            let n = self.len();
            if n > 0 {
                n - 1
            } else {
                0
            }
        }
    }

    #[derive(Debug)]
    pub struct TestLogProducer {
        log: TestLog,
        queue: VecDeque<Vec<u8>>,
    }

    impl TestLogProducer {
        fn new(log: TestLog) -> Self {
            Self { log, queue: Default::default() }
        }
    }

    #[async_trait]
    impl ByteLogProducer for TestLogProducer {
        fn queue(&mut self, payload: &[u8]) -> Result<()> {
            self.queue.push_back(payload.to_owned());
            Ok(())
        }

        async fn wait(&mut self) -> Result<LogPosition> {
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.log.append(self.queue.pop_front().unwrap())
        }
    }

    #[derive(Debug)]
    pub struct TestLogSubscriber {
        log: TestLog,
        payload: Vec<u8>,
        offset: usize,
    }

    impl TestLogSubscriber {
        fn new(log: TestLog, offset: LogOffset) -> Self {
            let offset = log.resolve_offset(offset).unwrap();
            Self { log, offset, payload: vec![] }
        }
    }

    #[async_trait]
    impl ByteLogSubscriber for TestLogSubscriber {
        async fn read(&mut self) -> Result<(LogPosition, &[u8])> {
            let mut receiver = self.log.len.subscribe();
            receiver.wait_for(|l| *l > self.offset).await.unwrap();
            let position = self.offset;
            self.payload = self.log.read(position);
            self.offset += 1;
            Ok((LogPosition::Offset(position as u128), &self.payload))
        }

        async fn seek(&mut self, offset: LogOffset) -> Result<()> {
            self.offset = self.log.resolve_offset(offset)?;
            Ok(())
        }

        async fn latest(&self) -> Result<LogPosition> {
            let offset = self.log.latest();
            Ok(LogPosition::Offset(offset as u128))
        }
    }

    #[derive(Debug)]
    pub struct TestLogClient {
        endpoint: OwnedEndpoint,
        params: Params,
        logs: Arc<Mutex<HashMap<String, TestLog>>>,
    }

    impl TestLogClient {
        fn new(endpoint: &Endpoint, params: &Params) -> Self {
            Self { endpoint: endpoint.to_owned(), params: params.to_owned(), logs: Default::default() }
        }

        pub fn endpoint(&self) -> Endpoint<'_> {
            self.endpoint.as_ref()
        }

        pub fn params(&self) -> &Params {
            &self.params
        }

        pub fn get_log(&self, name: &str) -> Option<TestLog> {
            self.logs.lock().unwrap().get(name).cloned()
        }
    }

    #[async_trait]
    impl LogClient for TestLogClient {
        async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>> {
            let Some(log) = self.logs.lock().unwrap().get(name).cloned() else { bail!("log {name} not found") };
            Ok(Box::new(TestLogProducer::new(log)))
        }

        async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>> {
            let Some(log) = self.logs.lock().unwrap().get(name).cloned() else { bail!("log {name} not found") };
            Ok(Box::new(TestLogSubscriber::new(log, offset)))
        }

        async fn create_log(&self, name: &str, retention: ByteSize) -> Result<()> {
            let log = TestLog::new(name, retention);
            if self.logs.lock().unwrap().insert(name.to_string(), log).is_some() {
                bail!("log already exists")
            }
            Ok(())
        }

        async fn delete_log(&self, name: &str) -> Result<()> {
            self.logs.lock().unwrap().remove(name);
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    pub struct TestLogFactory {
        scheme: &'static str,
        clients: Arc<Mutex<HashMap<OwnedEndpoint, Arc<TestLogClient>>>>,
    }

    impl TestLogFactory {
        pub fn new(scheme: &'static str) -> Self {
            Self { scheme, clients: Arc::new(Mutex::new(HashMap::default())) }
        }

        fn add_client(&self, endpoint: &Endpoint, client: Arc<TestLogClient>) {
            self.clients.lock().unwrap().insert(endpoint.to_owned(), client);
        }

        pub fn get_client(&self, endpoint: &Endpoint) -> Option<Arc<TestLogClient>> {
            self.clients.lock().unwrap().get(endpoint).cloned()
        }
    }

    #[async_trait]
    impl LogFactory for TestLogFactory {
        fn scheme(&self) -> &'static str {
            self.scheme
        }

        async fn open_client(&self, endpoint: &Endpoint, params: &Params) -> Result<Arc<dyn LogClient>> {
            let client = Arc::new(TestLogClient::new(endpoint, params));
            self.add_client(endpoint, client.clone());
            Ok(client)
        }
    }

    #[test]
    #[should_panic(expected = "log address expect path")]
    fn test_log_address_no_name() {
        LogAddress::new("kafka://localhost:9092").unwrap();
    }

    #[test]
    #[should_panic(expected = "log address invalid log name")]
    fn test_log_address_invalid_path() {
        LogAddress::new("kafka://localhost:9092/a/b").unwrap();
    }

    #[test]
    fn test_log_address_display() {
        let address = "kafka://localhost:9092/xyz";
        assert_that!(LogAddress::new(address).unwrap().to_string()).is_equal_to(address.to_string());
    }

    #[test_case("kafka://localhost/abc")]
    fn test_log_address_str(uri: &str) {
        let address = LogAddress::new(uri).unwrap();
        assert_that!(address.as_str()).is_equal_to(uri);
        assert_that!(address.uri().to_string().as_ref()).is_equal_to(uri);
    }

    #[test_case("kafka://localhost/abc")]
    fn test_log_address_equal(uri: &str) {
        let address = LogAddress::new(uri).unwrap();
        assert_eq!(address, uri);
        assert_that!(address.as_str()).is_equal_to(uri);
        assert_that!(address.uri().to_string().as_ref()).is_equal_to(uri);
    }

    #[test_case("kafka://localhost/abc")]
    fn test_log_address_owned(uri: &str) {
        let address = LogAddress::new(uri).unwrap();
        assert_that!(address).is_equal_to(LogAddress::new(uri.to_string()).unwrap());
        assert_that!(address).is_equal_to(address.to_owned());
        assert_that!(address.into_owned().as_str()).is_equal_to(uri);
    }
}
