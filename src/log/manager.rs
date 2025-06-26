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

//! [LogManager] to route log to its client.

use std::sync::Arc;

use anyhow::{bail, Result};
use bytesize::ByteSize;
use either::Either;
use hashbrown::hash_map::{Entry, HashMap};

use super::{ByteLogProducer, ByteLogSubscriber, LogAddress, LogClient, LogFactory, LogOffset, OwnedLogAddress};
use crate::endpoint::{OwnedResourceUri, ResourceUri, ServiceUri};

#[derive(Debug)]
pub struct LogManager {
    registry: LogRegistry,
    active_client: Arc<dyn LogClient>,
    clients: HashMap<OwnedResourceUri, Arc<dyn LogClient>>,
}

#[derive(Clone, Debug, Default)]
pub struct LogRegistry {
    factories: HashMap<&'static str, Arc<dyn LogFactory>>,
}

impl LogRegistry {
    pub fn register<T: LogFactory + 'static>(&mut self, factory: T) -> Result<(), T> {
        let entry = self.factories.entry(factory.scheme());
        match entry {
            Entry::Vacant(vacant) => {
                vacant.insert(Arc::new(factory));
                Ok(())
            },
            _ => Err(factory),
        }
    }

    pub(super) fn factories(&self) -> &HashMap<&'static str, Arc<dyn LogFactory>> {
        &self.factories
    }

    fn find_factory(&self, scheme: &str) -> Option<&dyn LogFactory> {
        self.factories.get(scheme).map(|f| f.as_ref())
    }

    pub(super) async fn new_client(&self, uri: &ServiceUri<'_>) -> Result<Arc<dyn LogClient>> {
        let Some(factory) = self.find_factory(uri.scheme()) else {
            bail!("no log factory for scheme of endpoint {}", uri.endpoint())
        };
        factory.open_client(uri).await
    }

    pub async fn into_manager(self, uri: &ServiceUri<'_>) -> Result<LogManager> {
        let client = self.new_client(uri).await?;
        let mut manager = LogManager { registry: self, active_client: client, clients: Default::default() };
        manager.add_client(uri.resource(), manager.active_client.clone());
        Ok(manager)
    }
}

impl LogManager {
    pub async fn new(factory: impl LogFactory, uri: &ServiceUri<'_>) -> Result<LogManager> {
        let mut registry = LogRegistry::default();
        registry.register(factory).unwrap();
        registry.into_manager(uri).await
    }

    pub fn registry(&self) -> &LogRegistry {
        &self.registry
    }

    pub fn clients(&self) -> impl Iterator<Item = &Arc<dyn LogClient>> {
        self.clients.values()
    }

    pub fn default_client(&self) -> &Arc<dyn LogClient> {
        &self.active_client
    }

    /// Open a client to produce, subscribe and delete existing logs.
    ///
    /// This will create a new client if the endpoint is new to any existing clients, otherwise it
    /// will return client opened for same endpoint.
    #[allow(invalid_reference_casting)]
    pub async fn open_client(&mut self, uri: &ServiceUri<'_>) -> Result<Arc<dyn LogClient>> {
        if let Some(client) = self.get_direct_client(&uri.resource()) {
            return Ok(client.clone());
        }
        let client = self.registry.new_client(uri).await?;
        self.add_client(uri.resource(), client.clone());
        Ok(client)
    }

    fn add_client(&mut self, uri: ResourceUri<'_>, client: Arc<dyn LogClient>) {
        match uri.split() {
            Either::Right(iter) => {
                let cluster_uri = iter.uri().to_owned();
                for uri in iter {
                    self.clients.insert(uri, client.clone());
                }
                self.clients.insert(cluster_uri, client);
            },
            Either::Left(uri) => {
                self.clients.insert(uri.into_owned(), client);
            },
        }
    }

    fn get_direct_client<'a>(&'a self, uri: &ResourceUri<'_>) -> Option<&'a Arc<dyn LogClient>> {
        if self.active_client.location() == *uri {
            return Some(&self.active_client);
        }
        self.clients.get(uri.as_str())
    }

    fn get_balanced_client(&self, uri: &ResourceUri) -> Option<&Arc<dyn LogClient>> {
        if let Either::Right(iter) = uri.clone().split() {
            for uri in iter {
                if let Some(client) = self.clients.get(uri.as_str()) {
                    return Some(client);
                }
            }
        }
        None
    }

    fn get_client<'a>(&'a self, uri: &ResourceUri<'_>) -> Option<&'a Arc<dyn LogClient>> {
        self.get_direct_client(uri).or_else(|| self.get_balanced_client(uri))
    }

    fn find_client<'a, 'b>(&'a self, address: &'b LogAddress) -> Result<(&'a dyn LogClient, &'b str)> {
        let uri = address.uri();
        let parent = uri.parent().unwrap();
        if let Some(client) = self.get_client(&parent) {
            return Ok((client.as_ref(), &uri.path()[parent.path().len() + 1..]));
        };
        bail!("no client for log address: {address}")
    }

    pub fn locate_log(&self, name: &str) -> OwnedLogAddress {
        let address = format!("{}/{}", self.active_client.location(), name);
        OwnedLogAddress::new(address).unwrap()
    }

    pub async fn create_log(&self, name: &str, retention: ByteSize) -> Result<OwnedLogAddress> {
        self.active_client.create_log(name, retention).await?;
        let address = format!("{}/{}", self.active_client.location(), name);
        Ok(OwnedLogAddress::new(address).unwrap())
    }

    pub async fn delete_log(&self, address: &LogAddress<'_>) -> Result<()> {
        let (client, name) = self.find_client(address)?;
        client.delete_log(name).await
    }

    pub async fn produce_log(&self, address: &LogAddress<'_>) -> Result<Box<dyn ByteLogProducer>> {
        let (client, name) = self.find_client(address)?;
        client.produce_log(name).await
    }

    pub async fn subscribe_log(
        &self,
        address: &LogAddress<'_>,
        offset: LogOffset,
    ) -> Result<Box<dyn ByteLogSubscriber>> {
        let (client, name) = self.find_client(address)?;
        client.subscribe_log(name, offset).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assertor::*;
    use bytesize::ByteSize;

    use crate::endpoint::{ResourceUri, ServiceUri};
    use crate::log::tests::*;
    use crate::log::{LogAddress, LogClient, LogOffset, LogRegistry};

    #[test]
    fn test_log_registry_find_factory() {
        let factory1 = TestLogFactory::new("scheme1");
        let factory2 = TestLogFactory::new("scheme2");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        registry.register(TestLogFactory::new("scheme1")).unwrap_err();
        registry.register(factory2.clone()).unwrap();

        assert!(registry.find_factory("scheme1").is_some());
        assert!(registry.find_factory("scheme2").is_some());
        assert!(registry.find_factory("scheme3").is_none());
    }

    #[tokio::test]
    async fn test_log_registry_new_client() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let uri = ServiceUri::parse("scheme1://address?param1=value1").unwrap();
        let client = registry.new_client(&uri).await.unwrap();
        let created_client = factory1.get_client(&uri.resource()).unwrap();
        assert_that!(created_client.resource_uri()).is_equal_to(uri.resource());
        assert_that!(created_client.params()).is_equal_to(uri.params());
        assert!(Arc::ptr_eq(&client, &(created_client as Arc<dyn LogClient>)));
    }

    #[tokio::test]
    #[should_panic(expected = "no log factory for scheme")]
    async fn test_log_registry_no_factory() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1).unwrap();
        registry.new_client(&ServiceUri::try_from("scheme2://address").unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn test_log_registry_into_manager() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let uri = ServiceUri::parse("scheme1://server1,server2:2222?param1=value1").unwrap();
        let manager = registry.into_manager(&uri).await.unwrap();
        let created_client: Arc<dyn LogClient> = factory1.get_client(&uri.resource()).unwrap();
        assert_that!(manager.active_client.location()).is_equal_to(uri.resource());
        assert!(Arc::ptr_eq(&manager.active_client, &created_client));

        // https://github.com/rust-lang/rust/issues/106447
        assert!(Arc::ptr_eq(
            manager.get_client(&ResourceUri::try_from("scheme1://server1").unwrap()).unwrap(),
            &created_client
        ));
        assert!(Arc::ptr_eq(
            manager.get_client(&ResourceUri::try_from("scheme1://server1,server3").unwrap()).unwrap(),
            &created_client
        ));
        assert!(Arc::ptr_eq(
            manager.get_client(&ResourceUri::try_from("scheme1://server2:2222").unwrap()).unwrap(),
            &created_client
        ));
        assert!(Arc::ptr_eq(
            manager.get_client(&ResourceUri::try_from("scheme1://server1,server2:2222").unwrap()).unwrap(),
            &created_client
        ));
        assert!(Arc::ptr_eq(
            manager.get_client(&ResourceUri::try_from("scheme1://server4,server2:2222").unwrap()).unwrap(),
            &created_client
        ));
    }

    #[tokio::test]
    async fn test_log_manager_open_client() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let uri1 = ServiceUri::try_from("scheme1://server1").unwrap();
        let mut manager = registry.into_manager(&uri1).await.unwrap();
        let client1: Arc<dyn LogClient> = factory1.get_client(&uri1.resource()).unwrap().clone();
        assert!(Arc::ptr_eq(&manager.open_client(&uri1).await.unwrap(), &client1));

        let uri2 = ServiceUri::try_from("scheme1://server2").unwrap();
        assert!(!Arc::ptr_eq(&manager.open_client(&uri2).await.unwrap(), &client1));
        let uri12 = ServiceUri::try_from("scheme1://server1,server2").unwrap();
        assert!(!Arc::ptr_eq(&manager.open_client(&uri12).await.unwrap(), &client1));
    }

    #[tokio::test]
    async fn test_log_manager_logging() {
        let factory = TestLogFactory::new("scheme");
        let mut registry = LogRegistry::default();
        registry.register(factory.clone()).unwrap();
        let active_uri = ServiceUri::try_from("scheme://server1,server2").unwrap();
        let mut manager = registry.into_manager(&active_uri).await.unwrap();

        // given: create a log through log manager
        let log1_address = manager.create_log("log1", ByteSize::mib(512)).await.unwrap();
        assert_eq!(log1_address, "scheme://server1,server2/log1");

        // then: log created in active client
        let active_client = factory.get_client(&active_uri.resource()).unwrap();
        let log1 = active_client.get_log("log1").unwrap();
        assert_eq!(log1.name(), "log1");
        assert_eq!(log1.retention(), ByteSize::mib(512));

        // given: producer/subscriber to same log
        let mut log1_producer = manager.produce_log(&log1_address).await.unwrap();
        let mut log1_subscriber = manager.subscribe_log(&log1_address, LogOffset::Latest).await.unwrap();

        // then: subscriber will read what producer send
        let payload1_position = log1_producer.send(b"payload1").await.unwrap();
        assert_that!(log1_subscriber.read().await.unwrap()).is_equal_to((payload1_position, b"payload1".as_slice()));

        // given: log created through client
        active_client.create_log("log2", ByteSize::default()).await.unwrap();
        let log2_address = LogAddress::try_from("scheme://server2/log2").unwrap();

        // then: producer and subscriber are connected through above client
        let mut log2_producer = manager.produce_log(&log2_address).await.unwrap();
        let mut log2_subscriber = manager.subscribe_log(&log2_address, LogOffset::Latest).await.unwrap();
        let subscriber_handle = tokio::spawn(async move {
            let (position, payload) = log2_subscriber.read().await.unwrap();
            (position, payload.to_owned())
        });
        let payload2_position = log2_producer.send(b"payload2").await.unwrap();
        assert_that!(subscriber_handle.await.unwrap())
            .is_equal_to((payload2_position, b"payload2".as_slice().to_owned()));

        // given: open client for "server2"
        let fallback_uri2 = ServiceUri::try_from("scheme://server2").unwrap();
        let client2 = manager.open_client(&fallback_uri2).await.unwrap();

        // then: it is a nop
        let client1 = active_client.clone() as Arc<dyn LogClient>;
        assert!(Arc::ptr_eq(&client1, &client2));
        manager.produce_log(&LogAddress::try_from("scheme://server2/log2").unwrap()).await.unwrap();

        // then: both "server1", "server2" are connected through the same client
        let log1_address = LogAddress::try_from("scheme://server1/log1").unwrap();
        let mut log1_producer = manager.produce_log(&log1_address).await.unwrap();
        let payload3_position = log1_producer.send(b"payload3").await.unwrap();
        assert_that!(log1_subscriber.read().await.unwrap()).is_equal_to((payload3_position, b"payload3".as_slice()));

        manager.delete_log(&log1_address).await.unwrap();
        assert!(active_client.get_log("log1").is_none());
    }
}
