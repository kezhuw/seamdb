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

pub mod fs;
mod kafka;
mod manager;
mod memory;

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytesize::ByteSize;
use compact_str::CompactString;

pub use self::kafka::KafkaLogFactory;
pub use self::manager::{LogManager, LogRegistry};
pub use self::memory::MemoryLogFactory;
use super::endpoint::{Params, ResourceUri, ServiceUri};

pub type OwnedLogAddress = LogAddress<'static>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogUri<'a> {
    uri: ServiceUri<'a>,
}

impl<'a> TryFrom<&'a str> for LogUri<'a> {
    type Error = anyhow::Error;

    fn try_from(str: &'a str) -> Result<Self> {
        let uri = ServiceUri::parse_named("log uri", str)?;
        LogAddress::try_from(uri.resource())?;
        Ok(Self { uri })
    }
}

impl Display for LogUri<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(self.uri.as_str())
    }
}

impl LogUri<'_> {
    pub fn address(&self) -> LogAddress<'_> {
        LogAddress { uri: self.uri.resource() }
    }

    pub fn params(&self) -> &Params<'_> {
        self.uri.params()
    }

    pub fn offset(&self) -> LogOffset {
        if let Some(offset) = self.params().query("offset") {
            return LogOffset::from(offset);
        }
        LogOffset::Earliest
    }
}

/// Address to a log.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogAddress<'a> {
    uri: ResourceUri<'a>,
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
        &self.uri
    }
}

impl PartialEq<&str> for LogAddress<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Display for LogAddress<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.uri)
    }
}

impl From<LogAddress<'_>> for String {
    fn from(address: LogAddress<'_>) -> String {
        address.uri.into()
    }
}

impl<'a> From<LogAddress<'a>> for ResourceUri<'a> {
    fn from(address: LogAddress<'a>) -> Self {
        address.uri
    }
}

impl<'a> TryFrom<ResourceUri<'a>> for LogAddress<'a> {
    type Error = anyhow::Error;

    fn try_from(uri: ResourceUri<'a>) -> Result<Self> {
        if uri.path().is_empty() {
            bail!("log address expect path: {uri}")
        }
        Ok(Self { uri })
    }
}

impl<'a> LogAddress<'a> {
    pub fn new(str: impl Into<Cow<'a, str>>) -> Result<Self> {
        let uri = ResourceUri::parse_named("log address", str.into())?;
        Self::try_from(uri)
    }

    pub fn uri(&self) -> &ResourceUri<'_> {
        &self.uri
    }

    pub fn as_str(&self) -> &str {
        &self.uri
    }

    pub fn to_owned(&self) -> LogAddress<'static> {
        self.clone().into_owned()
    }

    pub fn into_owned(self) -> LogAddress<'static> {
        LogAddress { uri: self.uri.into_owned() }
    }
}

/// Absolute position in log.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogPosition {
    Offset(u128),
    Cursor(CompactString),
}

impl Default for LogPosition {
    fn default() -> Self {
        Self::Offset(0)
    }
}

impl LogPosition {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Offset(offset) if *offset <= u64::MAX as u128 => Some(*offset as u64),
            Self::Cursor(str) => str.parse().ok(),
            _ => None,
        }
    }

    pub fn is_next_of(&self, previous: &LogPosition) -> bool {
        let Some(current) = self.as_u64() else {
            return false;
        };
        let Some(previous) = previous.as_u64() else {
            return false;
        };
        previous + 1 == current
    }
}

impl Display for LogPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LogPosition::Offset(offset) => f.write_fmt(format_args!("{offset}")),
            LogPosition::Cursor(cursor) => f.write_str(cursor),
        }
    }
}

impl From<&str> for LogPosition {
    fn from(s: &str) -> Self {
        match s.parse::<u128>() {
            Ok(offset) => Self::Offset(offset),
            Err(_) => Self::Cursor(CompactString::from(s)),
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

impl Display for LogOffset {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Earliest => f.write_str("earliest"),
            Self::Latest => f.write_str("latest"),
            Self::Position(position) => write!(f, "{position}"),
        }
    }
}

impl From<&str> for LogOffset {
    fn from(s: &str) -> Self {
        match s {
            "earliest" => Self::Earliest,
            "latest" => Self::Latest,
            _ => Self::Position(LogPosition::from(s)),
        }
    }
}

/// Producer to write byte message to log.
#[async_trait]
pub trait ByteLogProducer: Send + fmt::Debug + 'static {
    fn exclusive(&self) -> bool {
        false
    }

    fn queue(&mut self, payload: &[u8]) -> Result<()>;

    async fn wait(&mut self) -> Result<LogPosition>;

    async fn send(&mut self, payload: &[u8]) -> Result<LogPosition> {
        self.queue(payload)?;
        self.wait().await
    }
}

/// Subscriber to read byte message from log.
#[async_trait]
pub trait ByteLogSubscriber: Send + Sync + fmt::Debug {
    async fn read<'a>(&'a mut self) -> Result<(LogPosition, &'a [u8])>;

    async fn seek(&mut self, offset: LogOffset) -> Result<()>;

    async fn latest(&self) -> Result<LogPosition>;
}

/// Client to a log cluster.
#[async_trait]
pub trait LogClient: Send + Sync + fmt::Debug + 'static {
    fn location(&self) -> ResourceUri<'_>;

    async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>>;

    async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>>;

    async fn create_log(&self, name: &str, retention: ByteSize) -> Result<()>;

    async fn delete_log(&self, name: &str) -> Result<()>;
}

/// Factory to open [LogClient].
#[async_trait]
pub trait LogFactory: Send + Sync + fmt::Debug + 'static {
    fn scheme(&self) -> &'static str;

    async fn open_client(&self, uri: &ServiceUri) -> Result<Arc<dyn LogClient>>;
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

    use crate::endpoint::{OwnedResourceUri, OwnedServiceUri, Params, ResourceUri, ServiceUri};
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
        async fn read<'a>(&'a mut self) -> Result<(LogPosition, &'a [u8])> {
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
        uri: OwnedServiceUri,
        logs: Arc<Mutex<HashMap<String, TestLog>>>,
    }

    impl TestLogClient {
        fn new(uri: &ServiceUri) -> Self {
            Self { uri: uri.to_owned(), logs: Default::default() }
        }

        pub fn resource_uri(&self) -> ResourceUri<'_> {
            self.uri.resource()
        }

        pub fn params(&self) -> &Params {
            self.uri.params()
        }

        pub fn get_log(&self, name: &str) -> Option<TestLog> {
            self.logs.lock().unwrap().get(name).cloned()
        }
    }

    #[async_trait]
    impl LogClient for TestLogClient {
        fn location(&self) -> ResourceUri<'_> {
            self.uri.resource()
        }

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
        clients: Arc<Mutex<HashMap<OwnedResourceUri, Arc<TestLogClient>>>>,
    }

    impl TestLogFactory {
        pub fn new(scheme: &'static str) -> Self {
            Self { scheme, clients: Arc::new(Mutex::new(HashMap::default())) }
        }

        fn add_client(&self, uri: ResourceUri<'_>, client: Arc<TestLogClient>) {
            self.clients.lock().unwrap().insert(uri.into_owned(), client);
        }

        pub fn get_client(&self, uri: &ResourceUri) -> Option<Arc<TestLogClient>> {
            self.clients.lock().unwrap().get(uri).cloned()
        }
    }

    #[async_trait]
    impl LogFactory for TestLogFactory {
        fn scheme(&self) -> &'static str {
            self.scheme
        }

        async fn open_client(&self, uri: &ServiceUri) -> Result<Arc<dyn LogClient>> {
            let client = Arc::new(TestLogClient::new(uri));
            self.add_client(uri.resource(), client.clone());
            Ok(client)
        }
    }

    #[test]
    #[should_panic(expected = "log address expect path")]
    fn test_log_address_no_name() {
        LogAddress::new("kafka://localhost:9092").unwrap();
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

    #[test]
    fn test_log_address_namespaced() {
        LogAddress::new("kafka://localhost:9092/a/b").unwrap();
    }
}
