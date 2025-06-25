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
use rand::prelude::*;
use smallvec::SmallVec;

use super::{ByteLogProducer, ByteLogSubscriber, LogAddress, LogClient, LogFactory, LogOffset, OwnedLogAddress};
use crate::endpoint::{OwnedResourceUri, ResourceUri, ServiceUri};

#[derive(Debug)]
pub struct LogManager {
    registry: LogRegistry,
    active_client: Arc<dyn LogClient>,
    cluster_clients: HashMap<OwnedResourceUri, Arc<dyn LogClient>>,
    balanced_clients: HashMap<OwnedResourceUri, Vec<Arc<dyn LogClient>>>,
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

    fn find_factory(&self, scheme: &str) -> Option<&dyn LogFactory> {
        self.factories.get(scheme).map(|f| f.as_ref())
    }

    async fn new_client(&self, uri: &ServiceUri<'_>) -> Result<Arc<dyn LogClient>> {
        let Some(factory) = self.find_factory(uri.scheme()) else {
            bail!("no log factory for scheme of endpoint {}", uri.endpoint())
        };
        factory.open_client(uri).await
    }

    pub async fn into_manager(self, uri: &ServiceUri<'_>) -> Result<LogManager> {
        let client = self.new_client(uri).await?;
        let mut manager = LogManager {
            registry: self,
            active_client: client,
            cluster_clients: Default::default(),
            balanced_clients: Default::default(),
        };
        manager.add_balanced_client(uri.resource(), unsafe { std::mem::transmute(&manager.active_client) });
        Ok(manager)
    }
}

impl LogManager {
    pub async fn new(factory: impl LogFactory, uri: &ServiceUri<'_>) -> Result<LogManager> {
        let mut registry = LogRegistry::default();
        registry.register(factory).unwrap();
        registry.into_manager(uri).await
    }

    /// Open a client to produce, subscribe and delete existing logs.
    ///
    /// This will create a new client if the endpoint is new to any existing clients, otherwise it
    /// will return client opened for same endpoint.
    #[allow(invalid_reference_casting)]
    pub async fn open_client<'a>(&'a mut self, uri: &ServiceUri<'_>) -> Result<&'a Arc<dyn LogClient>> {
        if let Some(client) = self.get_cluster_client(&uri.resource()) {
            return Ok(client);
        }
        let client = self.registry.new_client(uri).await?;
        // https://github.com/rust-lang/rust/issues/74068
        // `last_or_push` of https://blog.rust-lang.org/2022/08/05/nll-by-default.html
        let manager = unsafe { &mut *(self as *const Self as *mut Self) };
        Ok(manager.add_client(uri.resource(), client))
    }

    fn add_client<'a>(&'a mut self, uri: ResourceUri<'_>, client: Arc<dyn LogClient>) -> &'a Arc<dyn LogClient> {
        self.add_balanced_client(uri.clone(), &client);
        self.cluster_clients.entry(uri.into_owned()).insert(client).into_mut()
    }

    fn add_balanced_client(&mut self, uri: ResourceUri<'_>, client: &Arc<dyn LogClient>) {
        if let Either::Right(iter) = uri.split() {
            for uri in iter {
                self.add_single_balanced_client(uri, client.clone());
            }
        }
    }

    fn add_single_balanced_client(&mut self, uri: OwnedResourceUri, client: Arc<dyn LogClient>) {
        self.balanced_clients.entry(uri).or_default().push(client.clone());
    }

    fn get_cluster_client<'a>(&'a self, uri: &ResourceUri<'_>) -> Option<&'a Arc<dyn LogClient>> {
        if self.active_client.location() == *uri {
            return Some(&self.active_client);
        }
        self.cluster_clients.get(uri.as_str())
    }

    fn get_balanced_client(&self, uri: &ResourceUri) -> Option<&dyn LogClient> {
        let mut servers: SmallVec<[_; 10]> = match uri.clone().split() {
            Either::Left(uri) => std::iter::once(uri.into_owned()).collect(),
            Either::Right(iter) => iter.collect(),
        };
        let rng = &mut thread_rng();
        servers.shuffle(rng);
        for server in servers {
            if let Some(clients) = self.balanced_clients.get(&server) {
                return Some(clients.choose(rng).unwrap().as_ref());
            };
        }
        None
    }

    fn get_client(&self, uri: &ResourceUri<'_>) -> Option<&dyn LogClient> {
        if let Some(client) = self.get_cluster_client(uri) {
            return Some(client.as_ref());
        }
        self.get_balanced_client(uri)
    }

    fn find_client<'a, 'b>(&'a self, address: &'b LogAddress) -> Result<(&'a dyn LogClient, &'b str)> {
        let uri = address.uri();
        let parent = uri.parent().unwrap();
        if let Some(client) = self.get_client(&parent) {
            return Ok((client, &uri.path()[parent.path().len() + 1..]));
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
        assert_eq!(
            manager.get_client(&ResourceUri::try_from("scheme1://server1").unwrap()).unwrap() as *const dyn LogClient
                as *const (),
            created_client.as_ref() as *const dyn LogClient as *const (),
        );
        assert_eq!(
            manager.get_client(&ResourceUri::try_from("scheme1://server1,server3").unwrap()).unwrap()
                as *const dyn LogClient as *const (),
            created_client.as_ref() as *const dyn LogClient as *const ()
        );
        assert_eq!(
            manager.get_client(&ResourceUri::try_from("scheme1://server2:2222").unwrap()).unwrap()
                as *const dyn LogClient as *const (),
            created_client.as_ref() as *const dyn LogClient as *const ()
        );
        assert_eq!(
            manager.get_client(&ResourceUri::try_from("scheme1://server1,server2:2222").unwrap()).unwrap()
                as *const dyn LogClient as *const (),
            created_client.as_ref() as *const dyn LogClient as *const ()
        );
        assert_eq!(
            manager.get_client(&ResourceUri::try_from("scheme1://server4,server2:2222").unwrap()).unwrap()
                as *const dyn LogClient as *const (),
            created_client.as_ref() as *const dyn LogClient as *const ()
        );
    }

    #[tokio::test]
    async fn test_log_manager_open_client() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let uri1 = ServiceUri::try_from("scheme1://server1").unwrap();
        let mut manager = registry.into_manager(&uri1).await.unwrap();
        let client1: Arc<dyn LogClient> = factory1.get_client(&uri1.resource()).unwrap().clone();
        assert!(Arc::ptr_eq(manager.open_client(&uri1).await.unwrap(), &client1));

        let uri2 = ServiceUri::try_from("scheme1://server2").unwrap();
        assert!(!Arc::ptr_eq(manager.open_client(&uri2).await.unwrap(), &client1));
        let uri12 = ServiceUri::try_from("scheme1://server1,server2").unwrap();
        assert!(!Arc::ptr_eq(manager.open_client(&uri12).await.unwrap(), &client1));
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

        // given: log created in balanced client
        let balanced_client = active_client.clone();
        balanced_client.create_log("log2", ByteSize::default()).await.unwrap();
        let log2_address = LogAddress::try_from("scheme://server2/log2").unwrap();

        // then: producer and subscriber are connected through balanced client
        let mut log2_producer = manager.produce_log(&log2_address).await.unwrap();
        let mut log2_subscriber = manager.subscribe_log(&log2_address, LogOffset::Latest).await.unwrap();
        let subscriber_handle = tokio::spawn(async move {
            let (position, payload) = log2_subscriber.read().await.unwrap();
            (position, payload.to_owned())
        });
        let payload2_position = log2_producer.send(b"payload2").await.unwrap();
        assert_that!(subscriber_handle.await.unwrap())
            .is_equal_to((payload2_position, b"payload2".as_slice().to_owned()));

        // given: cluster endpoint for "server2"
        let fallback_uri2 = ServiceUri::try_from("scheme://server2").unwrap();
        manager.open_client(&fallback_uri2).await.unwrap();

        // then: balanced client for "server2" will be shadowed
        assert_that!(manager
            .produce_log(&LogAddress::try_from("scheme://server2/log2").unwrap())
            .await
            .unwrap_err()
            .to_string())
        .contains("log log2 not found");

        // then: balanced client for "server1" should work as normal
        let log1_address = LogAddress::try_from("scheme://server1/log1").unwrap();
        let mut log1_producer = manager.produce_log(&log1_address).await.unwrap();
        let payload3_position = log1_producer.send(b"payload3").await.unwrap();
        assert_that!(log1_subscriber.read().await.unwrap()).is_equal_to((payload3_position, b"payload3".as_slice()));

        manager.delete_log(&log1_address).await.unwrap();
        assert!(active_client.get_log("log1").is_none());
    }
}
