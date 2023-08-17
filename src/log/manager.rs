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
use hashbrown::hash_map::{Entry, HashMap};
use rand::prelude::*;
use smallvec::SmallVec;

use super::{ByteLogProducer, ByteLogSubscriber, LogAddress, LogClient, LogFactory, LogOffset};
use crate::endpoint::{Endpoint, OwnedEndpoint, Params};

#[derive(Debug)]
pub struct LogManager {
    registry: LogRegistry,
    active_client: Arc<dyn LogClient>,
    active_address: OwnedEndpoint,
    cluster_clients: HashMap<OwnedEndpoint, Arc<dyn LogClient>>,
    balanced_clients: HashMap<OwnedEndpoint, Vec<Arc<dyn LogClient>>>,
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

    async fn new_client(&self, endpoint: &Endpoint<'_>, params: &Params) -> Result<Arc<dyn LogClient>> {
        let scheme = endpoint.scheme();
        let Some(factory) = self.find_factory(scheme) else {
            bail!("no log factory for scheme of endpoint {}", endpoint)
        };
        factory.open_client(endpoint, params).await
    }

    pub async fn into_manager(self, endpoint: &Endpoint<'_>, params: &Params) -> Result<LogManager> {
        let client = self.new_client(endpoint, params).await?;
        let mut manager = LogManager {
            registry: self,
            active_client: client,
            active_address: endpoint.to_owned(),
            cluster_clients: Default::default(),
            balanced_clients: Default::default(),
        };
        manager.add_balanced_client(endpoint, unsafe { std::mem::transmute(&manager.active_client) });
        Ok(manager)
    }
}

impl LogManager {
    /// Open a client to produce, subscribe and delete existing logs.
    ///
    /// This will create a new client if the endpoint is new to any existing clients, otherwise it
    /// will return client opened for same endpoint.
    #[allow(clippy::cast_ref_to_mut)]
    pub async fn open_client<'a>(
        &'a mut self,
        endpoint: &Endpoint<'_>,
        params: &Params,
    ) -> Result<&'a Arc<dyn LogClient>> {
        if let Some(client) = self.get_cluster_client(endpoint) {
            return Ok(client);
        }
        let client = self.registry.new_client(endpoint, params).await?;
        // https://github.com/rust-lang/rust/issues/74068
        // `last_or_push` of https://blog.rust-lang.org/2022/08/05/nll-by-default.html
        let manager = unsafe { &mut *(self as *const Self as *mut Self) };
        Ok(manager.add_client(endpoint, client))
    }

    fn add_client<'a>(&'a mut self, endpoint: &Endpoint, client: Arc<dyn LogClient>) -> &'a Arc<dyn LogClient> {
        self.add_balanced_client(endpoint, &client);
        self.cluster_clients.entry(endpoint.to_owned()).insert(client).into_mut()
    }

    fn add_balanced_client(&mut self, endpoint: &Endpoint, client: &Arc<dyn LogClient>) {
        // We don't use `split` here to avoid add single-server cluster endpoint.
        let Some((server, remainings)) = endpoint.address().split_once(',') else {
            return;
        };
        self.add_single_balanced_client(&endpoint.with_address(server), client.clone());
        for server in remainings.split(',') {
            self.add_single_balanced_client(&endpoint.with_address(server), client.clone());
        }
    }

    fn add_single_balanced_client(&mut self, server: &Endpoint, client: Arc<dyn LogClient>) {
        self.balanced_clients.entry(server.to_owned()).or_default().push(client.clone());
    }

    fn get_cluster_client<'a>(&'a self, endpoint: &Endpoint) -> Option<&'a Arc<dyn LogClient>> {
        if self.active_address == *endpoint {
            return Some(&self.active_client);
        }
        self.cluster_clients.get(endpoint)
    }

    fn get_balanced_client(&self, endpoint: &Endpoint) -> Option<&dyn LogClient> {
        let mut servers: SmallVec<[&str; 10]> = endpoint.address().split(',').collect();
        let rng = &mut thread_rng();
        servers.shuffle(rng);
        for server in servers {
            if let Some(clients) = self.balanced_clients.get(&endpoint.with_address(server)) {
                return Some(clients.choose(rng).unwrap().as_ref());
            };
        }
        None
    }

    fn get_client(&self, endpoint: &Endpoint) -> Option<&dyn LogClient> {
        if let Some(client) = self.get_cluster_client(endpoint) {
            return Some(client.as_ref());
        }
        self.get_balanced_client(endpoint)
    }

    fn find_client<'a, 'b>(&'a self, address: &'b LogAddress) -> Result<(&'a dyn LogClient, &'b str)> {
        let resource_id = address.resource_id();
        if let Some(client) = self.get_client(&resource_id.endpoint()) {
            return Ok((client, &resource_id.path()[1..]));
        };
        bail!("no client for log address: {address}")
    }

    pub async fn create_log(&self, name: &str, retention: ByteSize) -> Result<LogAddress> {
        self.active_client.create_log(name, retention).await?;
        let address = format!("{}/{}", self.active_address, name);
        Ok(LogAddress(address))
    }

    pub async fn delete_log(&self, address: &LogAddress) -> Result<()> {
        let (client, name) = self.find_client(address)?;
        client.delete_log(name).await
    }

    pub async fn produce_log(&self, address: &LogAddress) -> Result<Box<dyn ByteLogProducer>> {
        let (client, name) = self.find_client(address)?;
        client.produce_log(name).await
    }

    pub async fn subscribe_log(&self, address: &LogAddress, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>> {
        let (client, name) = self.find_client(address)?;
        client.subscribe_log(name, offset).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use speculoos::*;

    use super::super::*;
    use super::*;
    use crate::endpoint::ServiceUri;

    #[derive(Default, Debug)]
    struct TestLogProducer {}

    #[async_trait]
    impl ByteLogProducer for TestLogProducer {
        fn queue(&mut self, _payload: &[u8]) -> Result<()> {
            unreachable!()
        }

        async fn wait(&mut self) -> Result<LogPosition> {
            unreachable!()
        }
    }

    #[derive(Debug)]
    struct TestLogSubscriber {
        offset: LogOffset,
    }

    #[async_trait]
    impl ByteLogSubscriber for TestLogSubscriber {
        async fn read(&mut self) -> Result<(LogPosition, &[u8])> {
            unreachable!()
        }

        async fn seek(&mut self, _offset: LogOffset) -> Result<()> {
            unreachable!()
        }

        async fn latest(&self) -> Result<LogPosition> {
            unreachable!()
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct LogProducerPointer(*const dyn ByteLogProducer);

    impl<T: ByteLogProducer> From<&T> for LogProducerPointer {
        fn from(r: &T) -> Self {
            Self(r as *const dyn ByteLogProducer)
        }
    }

    impl From<&dyn ByteLogProducer> for LogProducerPointer {
        fn from(r: &dyn ByteLogProducer) -> Self {
            Self(r as *const dyn ByteLogProducer)
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct LogSubscriberPointer(*const dyn ByteLogSubscriber);

    impl<T: ByteLogSubscriber> From<&T> for LogSubscriberPointer {
        fn from(r: &T) -> Self {
            Self(r as *const dyn ByteLogSubscriber)
        }
    }

    impl From<&dyn ByteLogSubscriber> for LogSubscriberPointer {
        fn from(r: &dyn ByteLogSubscriber) -> Self {
            Self(r as *const dyn ByteLogSubscriber)
        }
    }

    unsafe impl Send for LogProducerPointer {}
    unsafe impl Send for LogSubscriberPointer {}

    #[derive(Debug)]
    struct TestLogClient {
        endpoint: OwnedEndpoint,
        params: Params,
        created_logs: Arc<Mutex<HashMap<String, ByteSize>>>,
        producing_logs: Arc<Mutex<HashMap<String, LogProducerPointer>>>,
        subscribing_logs: Arc<Mutex<HashMap<String, LogSubscriberPointer>>>,
    }

    impl TestLogClient {
        fn new(endpoint: &Endpoint, params: &Params) -> Self {
            Self {
                endpoint: endpoint.to_owned(),
                params: params.to_owned(),
                created_logs: Default::default(),
                producing_logs: Default::default(),
                subscribing_logs: Default::default(),
            }
        }

        pub fn endpoint(&self) -> Endpoint<'_> {
            self.endpoint.as_ref()
        }

        pub fn params(&self) -> &Params {
            &self.params
        }

        pub fn get_log(&self, name: &str) -> Option<ByteSize> {
            self.created_logs.lock().unwrap().get(name).copied()
        }

        pub fn get_producer(&self, name: &str) -> Option<&TestLogProducer> {
            match self.producing_logs.lock().unwrap().get(name).copied() {
                None => None,
                Some(pointer) => unsafe { Some(&*(pointer.0 as *const TestLogProducer)) },
            }
        }

        pub fn get_subscriber(&self, name: &str) -> Option<&TestLogSubscriber> {
            match self.subscribing_logs.lock().unwrap().get(name).copied() {
                None => None,
                Some(pointer) => unsafe { Some(&*(pointer.0 as *const TestLogSubscriber)) },
            }
        }
    }

    #[async_trait]
    impl LogClient for TestLogClient {
        async fn produce_log(&self, name: &str) -> Result<Box<dyn ByteLogProducer>> {
            if self.created_logs.lock().unwrap().get(name).is_none() {
                bail!("log not found")
            }
            let producer = Box::new(TestLogProducer::default());
            self.producing_logs.lock().unwrap().insert(name.to_string(), LogProducerPointer::from(producer.as_ref()));
            Ok(producer)
        }

        async fn subscribe_log(&self, name: &str, offset: LogOffset) -> Result<Box<dyn ByteLogSubscriber>> {
            if self.created_logs.lock().unwrap().get(name).is_none() {
                bail!("log not found")
            }
            let subscriber = Box::new(TestLogSubscriber { offset });
            self.subscribing_logs
                .lock()
                .unwrap()
                .insert(name.to_string(), LogSubscriberPointer::from(subscriber.as_ref()));
            Ok(subscriber)
        }

        async fn create_log(&self, name: &str, retention: ByteSize) -> Result<()> {
            if self.created_logs.lock().unwrap().insert(name.to_string(), retention).is_some() {
                bail!("log already exists")
            }
            Ok(())
        }

        async fn delete_log(&self, name: &str) -> Result<()> {
            self.created_logs.lock().unwrap().remove(name);
            Ok(())
        }
    }

    #[derive(Default, Clone, Debug)]
    struct TestLogFactory {
        scheme: &'static str,
        clients: Arc<Mutex<HashMap<OwnedEndpoint, Arc<TestLogClient>>>>,
    }

    impl TestLogFactory {
        pub fn new(scheme: &'static str) -> Self {
            Self { scheme, ..Default::default() }
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
    fn test_registry_find_factory() {
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
    async fn test_registry_new_client() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let (resource_id, params) = ServiceUri::parse("scheme1://address?param1=value1").unwrap();
        let endpoint = resource_id.endpoint();
        let client = registry.new_client(&endpoint, &params).await.unwrap();
        let created_client = factory1.get_client(&endpoint).unwrap();
        assert_that!(created_client.endpoint()).is_equal_to(endpoint);
        assert_that!(created_client.params()).is_equal_to(&params);
        assert!(Arc::ptr_eq(&client, &(created_client as Arc<dyn LogClient>)));
    }

    #[tokio::test]
    #[should_panic(expected = "no log factory for scheme")]
    async fn test_registry_no_factory() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1).unwrap();
        registry.new_client(&Endpoint::try_from("scheme2://address").unwrap(), &Params::default()).await.unwrap();
    }

    #[tokio::test]
    async fn test_registry_into_manager() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let (resource_id, params) = ServiceUri::parse("scheme1://server1,server2:2222?param1=value1").unwrap();
        let endpoint = resource_id.endpoint();
        let manager = registry.into_manager(&endpoint, &params).await.unwrap();
        let created_client: Arc<dyn LogClient> = factory1.get_client(&endpoint).unwrap();
        assert_that!(manager.active_address.as_ref()).is_equal_to(endpoint);
        assert!(Arc::ptr_eq(&manager.active_client, &created_client));
        assert!(std::ptr::eq(
            manager.get_client(&Endpoint::try_from("scheme1://server1").unwrap()).unwrap(),
            created_client.as_ref()
        ));
        assert!(std::ptr::eq(
            manager.get_client(&Endpoint::try_from("scheme1://server1,server3").unwrap()).unwrap(),
            created_client.as_ref()
        ));
        assert!(std::ptr::eq(
            manager.get_client(&Endpoint::try_from("scheme1://server2:2222").unwrap()).unwrap(),
            created_client.as_ref()
        ));
        assert!(std::ptr::eq(
            manager.get_client(&Endpoint::try_from("scheme1://server1,server2:2222").unwrap()).unwrap(),
            created_client.as_ref()
        ));
        assert!(std::ptr::eq(
            manager.get_client(&Endpoint::try_from("scheme1://server4,server2:2222").unwrap()).unwrap(),
            created_client.as_ref()
        ));
    }

    #[tokio::test]
    async fn test_manager_open_client() {
        let factory1 = TestLogFactory::new("scheme1");
        let mut registry = LogRegistry::default();
        registry.register(factory1.clone()).unwrap();
        let endpoint1 = Endpoint::try_from("scheme1://server1").unwrap();
        let mut manager = registry.into_manager(&endpoint1, &Params::default()).await.unwrap();
        let client1: Arc<dyn LogClient> = factory1.get_client(&endpoint1).unwrap().clone();
        assert!(Arc::ptr_eq(manager.open_client(&endpoint1, &Params::default()).await.unwrap(), &client1));

        let endpoint2 = Endpoint::try_from("scheme1://server2").unwrap();
        assert!(!Arc::ptr_eq(manager.open_client(&endpoint2, &Params::default()).await.unwrap(), &client1));
        let endpoint12 = Endpoint::try_from("scheme1://server1,server2").unwrap();
        assert!(!Arc::ptr_eq(manager.open_client(&endpoint12, &Params::default()).await.unwrap(), &client1));
    }

    #[tokio::test]
    async fn test_manager_log() {
        let factory = TestLogFactory::new("scheme");
        let mut registry = LogRegistry::default();
        registry.register(factory.clone()).unwrap();
        let active_endpoint = Endpoint::try_from("scheme://server1,server2").unwrap();
        let mut manager = registry.into_manager(&active_endpoint, &Params::default()).await.unwrap();

        let log1 = manager.create_log("log1", ByteSize::mib(512)).await.unwrap();
        assert!(log1 == "scheme://server1,server2/log1");

        let active_client = factory.get_client(&active_endpoint).unwrap();
        assert_eq!(active_client.get_log("log1").unwrap(), ByteSize::mib(512));

        let log1_subscriber = manager.subscribe_log(&log1, LogOffset::Latest).await.unwrap();
        let cached_log1_subscriber = active_client.get_subscriber("log1").unwrap();
        assert_eq!(cached_log1_subscriber.offset, LogOffset::Latest);
        assert!(std::ptr::eq(log1_subscriber.as_ref(), cached_log1_subscriber as &dyn ByteLogSubscriber));

        active_client.create_log("log2", ByteSize::default()).await.unwrap();
        let log2_producer =
            manager.produce_log(&LogAddress::try_from("scheme://server2/log2".to_string()).unwrap()).await.unwrap();
        assert!(std::ptr::eq(
            log2_producer.as_ref(),
            active_client.get_producer("log2").unwrap() as &dyn ByteLogProducer
        ));

        let fallback_endpoint2 = Endpoint::try_from("scheme://server2").unwrap();
        manager.open_client(&fallback_endpoint2, &Params::default()).await.unwrap();
        assert!(manager
            .produce_log(&LogAddress::try_from("scheme://server2/log2".to_string()).unwrap())
            .await
            .unwrap_err()
            .to_string()
            .contains("log not found"));

        let log1_address = LogAddress::try_from("scheme://server1/log1".to_string()).unwrap();
        let log1_producer = manager.produce_log(&log1_address).await.unwrap();
        assert!(std::ptr::eq(
            log1_producer.as_ref(),
            active_client.get_producer("log1").unwrap() as &dyn ByteLogProducer
        ));

        manager.delete_log(&log1_address).await.unwrap();
        assert!(active_client.get_log("log1").is_none());
    }
}
