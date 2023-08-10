pub mod kafka;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytesize::ByteSize;
use compact_str::CompactString;

use super::service_uri::ServiceUri;

/// Address to a log.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogAddress(pub String);

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
pub trait ByteLogProducer: Send + std::fmt::Debug {
    fn queue(&mut self, payload: &[u8]) -> Result<()>;

    async fn wait(&mut self) -> Result<LogPosition>;

    async fn send(&mut self, payload: &[u8]) -> Result<LogPosition> {
        self.queue(payload)?;
        self.wait().await
    }
}

/// Subscriber to read byte message from log.
#[async_trait]
pub trait ByteLogSubscriber: Send + Sync + std::fmt::Debug {
    async fn read(&mut self) -> Result<(LogPosition, &[u8])>;

    async fn seek(&mut self, offset: LogOffset) -> Result<()>;

    async fn latest(&self) -> Result<LogPosition>;
}

/// Client to a log cluster.
#[async_trait]
pub trait LogClient: Send + Sync + std::fmt::Debug {
    /// uri to reconstruct this client.
    fn uri(&self) -> &ServiceUri;

    /// Connection address part from uri with no params for Client customization.
    fn address(&self) -> &str;

    fn log_address(&self, mut name: String) -> LogAddress {
        let address = self.address();
        let i = match address.rfind('?') {
            None => address.len(),
            Some(i) => i,
        };
        name.reserve(address.len() + 1);
        name.insert(0, '/');
        name.insert_str(0, &address[..i]);
        name.push_str(&address[i..]);
        LogAddress(name)
    }

    async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>>;

    async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>>;

    async fn create_log(&self, name: &str, retention: ByteSize) -> Result<LogAddress>;

    async fn delete_log(&self, name: &str) -> Result<()>;
}

/// Factory to open [LogClient].
#[async_trait]
pub trait LogFactory {
    async fn new_client(uri: ServiceUri) -> Result<Arc<dyn LogClient>>;
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
