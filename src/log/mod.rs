pub mod kafka;
pub mod manager;

use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytesize::ByteSize;
use compact_str::CompactString;

use super::endpoint::{Endpoint, Params, ResourceId};

/// Address to a log.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct LogAddress(String);

impl std::ops::Deref for LogAddress {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LogAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<&str> for LogAddress {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq(other)
    }
}

impl TryFrom<String> for LogAddress {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self> {
        let id = ResourceId::parse_named("log address", &s)?;
        if unsafe { id.path().get_unchecked(1..) }.find('/').is_some() {
            bail!("log address invalid log name: {s}")
        }
        Ok(LogAddress(s))
    }
}

impl LogAddress {
    pub fn resource_id(&self) -> ResourceId<'_> {
        unsafe { ResourceId::new_unchecked(&self.0) }
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
pub trait LogFactory: std::fmt::Debug + 'static {
    fn scheme(&self) -> &'static str;

    async fn open_client(&self, endpoint: &Endpoint, params: &Params) -> Result<Arc<dyn LogClient>>;
}

#[cfg(test)]
mod tests {
    use speculoos::*;

    use super::*;

    #[test]
    fn test_log_address_display() {
        let address = "kafka://localhost:9092/xyz".to_string();
        assert_that!(LogAddress(address.clone()).to_string()).is_equal_to(address);
    }
}
