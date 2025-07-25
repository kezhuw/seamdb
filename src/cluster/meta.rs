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

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use bytesize::ByteSize;
use compact_str::{CompactString, ToCompactString};
use etcd_client::{
    Client,
    Compare,
    CompareOp,
    EventType,
    Txn,
    TxnOp,
    WatchOptions,
    WatchResponse,
    WatchStream,
    Watcher,
};
use prost::Message as _;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::etcd::EtcdHelper;
use super::node::NodeRegistry;
use super::ClusterEnv;
use crate::endpoint::ServiceUri;
use crate::keys;
use crate::log::OwnedLogAddress;
use crate::protos::{
    self,
    ClusterDescriptor,
    DataMessage,
    DataOperation,
    KeyRange,
    ManifestMessage,
    ShardDescription,
    ShardDescriptor,
    ShardId,
    ShardMergeBounds,
    TabletDeployment,
    TabletDescription,
    TabletDescriptor,
    TabletId,
    TabletManifest,
    Temporal,
    Timestamp,
};
use crate::tablet::TabletDeployer;
use crate::utils::{self, DropOwner, DropWatcher, WatchConsumer as _};

type Revision = i64;

const CLUSTER_BOOT_TIMEOUT: Duration = Duration::from_secs(5);

const BOOTSTRAP_LOGS_NUMBER: usize = 8;

const RANGE_TABLET_INITIAL_ID: TabletId = TabletId::from_raw(0x010000);
const SYSTEM_TABLET_INITIAL_ID: TabletId = TabletId::from_raw(0x020000);
const DATA_TABLET_INITIAL_ID: TabletId = TabletId::from_raw(0x030000);

const RANGE_SHARD_INITAL_ID: ShardId = ShardId::from_raw(0x010000);
const SYSTEM_SHARD_INITAL_ID: ShardId = ShardId::from_raw(0x020000);
const DATA_SHARD_INITAL_ID: ShardId = ShardId::from_raw(0x030000);

fn parse_deployment(response: WatchResponse) -> Result<TabletDeployment> {
    let event = response.events().last().ok_or_else(|| anyhow!("cluster meta watch receives no event"))?;
    match event.event_type() {
        EventType::Delete => bail!("cluster meta got deleted"),
        EventType::Put => {
            let kv = event.kv().ok_or_else(|| anyhow!("cluster meta watch receives no kv"))?;
            let deployment = TabletDeployment::decode(kv.value())?;
            Ok(deployment)
        },
    }
}

#[derive(Clone)]
struct EtcdClusterClient {
    name: CompactString,
    root: CompactString,
    etcd: Client,
}

pub struct EtcdClusterMetaDaemon {
    env: ClusterEnv,
    client: EtcdClusterClient,
}

struct EtcdClusterMetaHandle {
    client: EtcdClusterClient,
    daemon: JoinHandle<Result<()>>,
    _dropper: DropOwner,
}

#[async_trait]
impl TabletDeployer for EtcdClusterMetaDaemon {
    type Version = Revision;

    fn nodes(&self) -> &Arc<dyn NodeRegistry> {
        self.env.nodes()
    }

    fn replicas(&self) -> usize {
        self.env.replicas()
    }

    async fn put_deployment(
        &self,
        key: &[u8],
        version: Self::Version,
        deployment: &TabletDeployment,
    ) -> Result<Self::Version> {
        ClusterMetaClient::put_deployment(self, key, deployment, Some(version)).await
    }
}

impl EtcdClusterMetaDaemon {
    fn random_bootstrap_logs(&self) -> (Vec<String>, Vec<String>) {
        let mut names = Vec::with_capacity(BOOTSTRAP_LOGS_NUMBER);
        let mut uris = Vec::with_capacity(names.capacity());
        for _ in 0..BOOTSTRAP_LOGS_NUMBER {
            let name = Uuid::new_v4().to_string();
            uris.push(self.env.log().locate_log(&name).into());
            names.push(name);
        }
        (names, uris)
    }

    async fn create_descriptor(&mut self, lock: &[u8]) -> Result<(Revision, Vec<String>, ClusterDescriptor)> {
        let (log_names, log_uris) = self.random_bootstrap_logs();
        let meta = ClusterDescriptor {
            name: self.client.name.to_string(),
            generation: 0,
            manifest_log: Default::default(),
            timestamp: Default::default(),
            bootstrap_logs: log_uris,
            obsoleted_logs: Default::default(),
        };
        let revision = self.put_descriptor(lock, &meta, None).await?;
        Ok((revision, log_names, meta))
    }

    async fn bootstrap_tablet_data(
        &mut self,
        log_name: String,
        writes: impl Into<Vec<protos::Write>>,
        ts: Timestamp,
    ) -> Result<OwnedLogAddress> {
        let log_address = self.env.log().create_log(&log_name, ByteSize::default()).await?;
        let mut log_producer = self.env.log().produce_log(&log_address).await?;
        let message = DataMessage {
            epoch: 0,
            sequence: 1,
            temporal: Temporal::Timestamp(ts),
            operation: Some(DataOperation::Batch(protos::Batch { writes: writes.into() })),
            closed_timestamp: Some(ts),
            leader_expiration: Some(ts),
        };
        log_producer.send(&message.encode_to_vec()).await?;
        Ok(log_address)
    }

    async fn bootstrap_tablet_manifest_log(
        &mut self,
        log_name: String,
        data_log_uri: OwnedLogAddress,
        tablet_id: TabletId,
        mut shards: Vec<(ShardId, KeyRange)>,
    ) -> Result<(OwnedLogAddress, Vec<ShardDescriptor>)> {
        shards.sort_by(|a, b| a.1.start.cmp(&b.1.start));
        let shards: Vec<_> = shards
            .into_iter()
            .map(|(id, range)| ShardDescriptor { id: id.into(), range, tablet_id: tablet_id.into() })
            .collect();
        let tablet = TabletDescription {
            id: tablet_id.into(),
            generation: 0,
            data_log: data_log_uri.into(),
            shards: shards
                .iter()
                .map(|shard| ShardDescription {
                    id: shard.id,
                    range: shard.range.clone(),
                    segments: vec![],
                    merge_bounds: ShardMergeBounds::None,
                })
                .collect(),
        };
        let manifest = TabletManifest { tablet, ..TabletManifest::default() };
        let message = ManifestMessage { epoch: 0, sequence: 1, manifest: Some(manifest) };
        let log_address = self.env.log().create_log(&log_name, ByteSize::mib(10)).await?;
        let mut log_producer = self.env.log().produce_log(&log_address).await?;
        log_producer.send(&message.encode_to_vec()).await?;
        Ok((log_address, shards))
    }

    async fn bootstrap_tablet_manifest(
        &mut self,
        log_name: String,
        data_log_uri: OwnedLogAddress,
        id: TabletId,
        shards: Vec<(ShardId, KeyRange)>,
    ) -> Result<(TabletDescriptor, Vec<ShardDescriptor>)> {
        let (log_uri, shards) = self.bootstrap_tablet_manifest_log(log_name, data_log_uri, id, shards).await?;
        let descriptor = TabletDescriptor { id: id.into(), generation: 0, manifest_log: log_uri.into() };
        Ok((descriptor, shards))
    }

    async fn bootstrap_system_tablet(
        &mut self,
        log_names: &mut Vec<String>,
        ts: Timestamp,
    ) -> Result<(TabletDescriptor, Vec<ShardDescriptor>)> {
        let key = keys::system_key("tablet-id-counter".as_bytes());
        let value = protos::Value::Int(4);
        let write = protos::Write { key, value: Some(value), sequence: 0 };
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), [write], ts).await?;
        self.bootstrap_tablet_manifest(log_names.pop().unwrap(), data_log_uri, SYSTEM_TABLET_INITIAL_ID, vec![(
            SYSTEM_SHARD_INITAL_ID,
            KeyRange::new(keys::SYSTEM_KEY_PREFIX, keys::USER_KEY_PREFIX),
        )])
        .await
    }

    async fn bootstrap_user_tablet(
        &mut self,
        log_names: &mut Vec<String>,
        ts: Timestamp,
    ) -> Result<(TabletDescriptor, Vec<ShardDescriptor>)> {
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), vec![], ts).await?;
        self.bootstrap_tablet_manifest(log_names.pop().unwrap(), data_log_uri, DATA_TABLET_INITIAL_ID, vec![(
            DATA_SHARD_INITAL_ID,
            KeyRange::new(keys::USER_KEY_PREFIX, keys::MAX_KEY),
        )])
        .await
    }

    async fn bootstrap_range_tablet(
        &mut self,
        log_names: &mut Vec<String>,
        descriptors: Vec<ShardDescriptor>,
        ts: Timestamp,
    ) -> Result<(TabletDescriptor, Vec<ShardDescriptor>)> {
        let mut writes = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let key = keys::range_key(&descriptor.range.end);
            let value = protos::Value::Bytes(descriptor.encode_to_vec());
            let write = protos::Write { key, value: Some(value), sequence: 0 };
            writes.push(write);
        }
        let mut key = writes.last().unwrap().key.clone();
        key.push(0);
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), writes, ts).await?;
        self.bootstrap_tablet_manifest(log_names.pop().unwrap(), data_log_uri, RANGE_TABLET_INITIAL_ID, vec![(
            RANGE_SHARD_INITAL_ID,
            KeyRange::new(keys::RANGE_KEY_PREFIX, key),
        )])
        .await
    }

    async fn bootstrap_root_tablet(
        &mut self,
        log_names: &mut Vec<String>,
        tablets: Vec<TabletDescriptor>,
        shards: Vec<ShardDescriptor>,
        ts: Timestamp,
    ) -> Result<OwnedLogAddress> {
        let mut writes = Vec::with_capacity(shards.len());
        for shard in shards {
            let key = keys::root_key(&shard.range.end);
            let value = protos::Value::Bytes(shard.encode_to_vec());
            let write = protos::Write { key, value: Some(value), sequence: 0 };
            writes.push(write);
        }
        let mut key = writes.last().unwrap().key.clone();
        key.push(0);
        for tablet in tablets.iter() {
            let key = keys::deployment_key(tablet.id.into());
            let deployment = TabletDeployment { id: tablet.id, ..Default::default() };
            let value = protos::Value::Bytes(deployment.encode_to_vec());
            let write = protos::Write { key, value: Some(value), sequence: 0 };
            writes.push(write);
        }
        for tablet in tablets {
            let key = keys::descriptor_key(tablet.id.into());
            let value = protos::Value::Bytes(tablet.encode_to_vec());
            let write = protos::Write { key, value: Some(value), sequence: 0 };
            writes.push(write);
        }
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), writes, ts).await?;
        let (log_address, _) = self
            .bootstrap_tablet_manifest_log(log_names.pop().unwrap(), data_log_uri, TabletId::ROOT, vec![
                (ShardId::ROOT, KeyRange::new(keys::ROOT_KEY_PREFIX, key)),
                (ShardId::DESCRIPTOR, keys::descriptor_range()),
                (ShardId::DEPLOYMENT, keys::deployment_range()),
            ])
            .await?;
        Ok(log_address)
    }

    async fn bootstrap_descriptor(
        &mut self,
        lock: &[u8],
        revision: Revision,
        mut log_names: Vec<String>,
        mut descriptor: ClusterDescriptor,
    ) -> Result<(Revision, ClusterDescriptor)> {
        let now = self.env.clock().now();
        let (system_tablet, system_shards) = self.bootstrap_system_tablet(&mut log_names, now).await?;
        let (user_tablet, user_shards) = self.bootstrap_user_tablet(&mut log_names, now).await?;
        let (range_tablet, range_shards) =
            self.bootstrap_range_tablet(&mut log_names, [system_shards, user_shards].concat(), now).await?;
        let root_tablet_log_uri = self
            .bootstrap_root_tablet(&mut log_names, vec![range_tablet, system_tablet, user_tablet], range_shards, now)
            .await?;
        descriptor.timestamp = self.env.clock().now();
        descriptor.generation += 1;
        descriptor.manifest_log = root_tablet_log_uri.into();
        descriptor.bootstrap_logs.clear();
        let revision = self.put_descriptor(lock, &descriptor, Some(revision)).await?;
        Ok((revision, descriptor))
    }

    async fn init_descriptor(&mut self, lock: &[u8]) -> Result<(Revision, ClusterDescriptor)> {
        let (revision, log_names, meta) = self.create_descriptor(lock).await?;
        self.bootstrap_descriptor(lock, revision, log_names, meta).await
    }

    async fn restore_descriptor(
        &mut self,
        lock: &[u8],
        mut revision: i64,
        mut meta: ClusterDescriptor,
    ) -> Result<(Revision, ClusterDescriptor)> {
        if meta.name != self.client.name {
            return Err(anyhow!("unexpected cluster name: expect {}, got {}", self.client.name, meta.name));
        }
        if !meta.bootstrap_logs.is_empty() {
            meta.obsoleted_logs.append(&mut meta.bootstrap_logs);
            let (log_names, log_uris) = self.random_bootstrap_logs();
            meta.bootstrap_logs = log_uris;
            revision = self.put_descriptor(lock, &meta, Some(revision)).await?;
            self.bootstrap_descriptor(lock, revision, log_names, meta).await
        } else {
            Ok((revision, meta))
        }
    }

    async fn serve(&mut self) -> Result<()> {
        let lease = EtcdHelper::grant_lease(&mut self.client.etcd, None).await?;
        let lock = self.lock(lease.id()).await?;
        match self.get_descriptor().await? {
            (_, None) => self.init_descriptor(&lock).await?,
            (revision, Some(meta)) => self.restore_descriptor(&lock, revision, meta).await?,
        };
        let (revision, deployment) = match self.get_deployment().await? {
            (_, None) => (0, TabletDeployment { id: TabletId::ROOT.into(), ..Default::default() }),
            (revision, Some(deployment)) => (revision, deployment),
        };
        self.serve_deployment(lock, revision, deployment).await
    }

    pub async fn start(
        name: impl Into<CompactString>,
        uri: ServiceUri<'_>,
        env: ClusterEnv,
    ) -> Result<Box<dyn ClusterMetaHandle>> {
        let (resource_uri, params) = uri.parts();
        let etcd = EtcdHelper::connect(resource_uri.endpoint(), params).await?;
        let client = EtcdClusterClient { name: name.into(), root: resource_uri.path().to_compact_string(), etcd };
        let (drop_owner, mut drop_watcher) = utils::drop_watcher();
        let mut daemon = EtcdClusterMetaDaemon { env, client: client.clone() };
        let handle = tokio::spawn(async move {
            select! {
                r = daemon.serve() => r,
                _ = drop_watcher.dropped() => Ok(()),
            }
        });
        Ok(Box::new(EtcdClusterMetaHandle { client, daemon: handle, _dropper: drop_owner }))
    }
}

#[async_trait]
trait ClusterMetaClient: Send + Sync + 'static {
    fn name(&self) -> &str;

    fn root(&self) -> &str;

    fn etcd(&self) -> &Client;

    fn etcd_mut(&mut self) -> &mut Client;

    fn descriptor_key(&self) -> String {
        format!("{}/meta/descriptor", self.root())
    }

    fn deployment_key(&self) -> String {
        format!("{}/meta/deployment", self.root())
    }

    fn lock_key(&self) -> String {
        format!("{}/meta/leader", self.root())
    }

    async fn lock(&mut self, lease_id: i64) -> Result<Vec<u8>> {
        let path = self.lock_key();
        EtcdHelper::lock(self.etcd_mut(), path, lease_id).await
    }

    async fn get_descriptor(&mut self) -> Result<(Revision, Option<ClusterDescriptor>)> {
        let (revision, data) = self.get_key(self.descriptor_key()).await?;
        let Some(data) = data else {
            return Ok((revision, None));
        };
        let descriptor = ClusterDescriptor::decode(data.as_slice())?;
        if descriptor.name != self.name() {
            bail!("cluster descriptor name mismatch: expect {}, got {}", self.name(), descriptor.name)
        }
        Ok((revision, Some(descriptor)))
    }

    async fn get_deployment(&mut self) -> Result<(Revision, Option<TabletDeployment>)> {
        let (revision, data) = self.get_key(self.deployment_key()).await?;
        let Some(data) = data else {
            return Ok((revision, None));
        };
        let deployment = TabletDeployment::decode(data.as_slice())?;
        Ok((revision, Some(deployment)))
    }

    async fn get_key(&mut self, key: String) -> Result<(Revision, Option<Vec<u8>>)> {
        let mut response = self.etcd_mut().get(key, None).await?.0;
        if let Some(kv) = response.kvs.pop() {
            let revision = kv.mod_revision;
            Ok((revision, Some(kv.value)))
        } else if let Some(header) = response.header {
            Ok((header.revision, None))
        } else {
            Err(anyhow!("etcd get reponse has no header"))
        }
    }

    async fn put_key(
        &self,
        lock: &[u8],
        key: Vec<u8>,
        data: Vec<u8>,
        revision: Option<Revision>,
    ) -> Result<Option<Revision>> {
        let revision_cmp = match revision {
            None | Some(0) => Compare::create_revision(key.clone(), CompareOp::Equal, 0),
            Some(revision) => Compare::mod_revision(key.clone(), CompareOp::Equal, revision),
        };
        let lock_cmp = Compare::create_revision(lock, CompareOp::NotEqual, 0);
        let put = TxnOp::put(key, data, None);
        let txn = Txn::new().when([revision_cmp, lock_cmp]).and_then([put]);
        let response = self.etcd().kv_client().txn(txn).await?;
        if !response.succeeded() {
            Ok(None)
        } else {
            Ok(Some(response.header().unwrap().revision()))
        }
    }

    async fn put_deployment(
        &self,
        lock: &[u8],
        deployment: &TabletDeployment,
        revision: Option<Revision>,
    ) -> Result<Revision> {
        let key = self.deployment_key().into_bytes();
        match self.put_key(lock, key, deployment.encode_to_vec(), revision).await? {
            None => Err(anyhow!("cluster deployment changed or lock expired")),
            Some(revision) => Ok(revision),
        }
    }

    async fn put_descriptor(
        &self,
        lock: &[u8],
        descriptor: &ClusterDescriptor,
        revision: Option<Revision>,
    ) -> Result<Revision> {
        let key = self.descriptor_key().into_bytes();
        match self.put_key(lock, key, descriptor.encode_to_vec(), revision).await? {
            None => Err(anyhow!("cluster descriptor changed or lock expired")),
            Some(revision) => Ok(revision),
        }
    }

    async fn poll_deployment(&self, stream: &mut WatchStream) -> Result<TabletDeployment> {
        match stream.message().await? {
            None => bail!("cluster deployment stream got closed"),
            Some(message) => parse_deployment(message),
        }
    }

    async fn watch_descriptor(&mut self, timeout: Option<Duration>) -> Result<ClusterDescriptorWatcher> {
        let (revision, descriptor) = self.get_descriptor().await?;
        let key = self.descriptor_key();
        let options = WatchOptions::new().with_start_revision(revision + 1);
        let (watcher, stream) = self.etcd_mut().watch(key, Some(options)).await?;
        ClusterDescriptorWatcher::watch(descriptor, watcher, stream, timeout).await
    }

    async fn watch_deployment(&mut self, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher> {
        let (revision, deployment) = self.get_deployment().await?;
        let key = self.deployment_key();
        let options = WatchOptions::new().with_start_revision(revision + 1);
        let (watcher, mut stream) = self.etcd_mut().watch(key, Some(options)).await?;
        let deployment = if let Some(deployment) = deployment {
            deployment
        } else {
            select! {
                _ = tokio::time::sleep(timeout.unwrap_or(CLUSTER_BOOT_TIMEOUT)) => bail!("cluster deployment not found"),
                r = self.poll_deployment(&mut stream) => match r {
                    Err(err) => return Err(err),
                    Ok(deployment) => deployment,
                },
            }
        };
        Ok(ClusterDeploymentWatcher::watch(watcher, stream, deployment).await)
    }
}

impl ClusterMetaClient for EtcdClusterClient {
    fn name(&self) -> &str {
        &self.name
    }

    fn root(&self) -> &str {
        &self.root
    }

    fn etcd(&self) -> &Client {
        &self.etcd
    }

    fn etcd_mut(&mut self) -> &mut Client {
        &mut self.etcd
    }
}

impl ClusterMetaClient for EtcdClusterMetaDaemon {
    fn name(&self) -> &str {
        &self.client.name
    }

    fn root(&self) -> &str {
        &self.client.root
    }

    fn etcd(&self) -> &Client {
        &self.client.etcd
    }

    fn etcd_mut(&mut self) -> &mut Client {
        &mut self.client.etcd
    }
}

impl ClusterMetaClient for EtcdClusterMetaHandle {
    fn name(&self) -> &str {
        &self.client.name
    }

    fn root(&self) -> &str {
        &self.client.root
    }

    fn etcd(&self) -> &Client {
        &self.client.etcd
    }

    fn etcd_mut(&mut self) -> &mut Client {
        &mut self.client.etcd
    }
}

#[derive(Clone)]
pub struct ClusterDeploymentMonitor {
    _dropper: Arc<DropOwner>,
    deployment: Arc<ArcSwapOption<TabletDeployment>>,
}

impl ClusterDeploymentMonitor {
    pub async fn new(mut receiver: watch::Receiver<Arc<TabletDeployment>>, deployment: Arc<TabletDeployment>) -> Self {
        let (drop_owner, mut drop_watcher) = utils::drop_watcher();
        let deployment = Arc::new(ArcSwapOption::new(Some(deployment)));
        let publisher = deployment.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = drop_watcher.dropped() => break,
                    r = receiver.changed() => if r.is_err() {
                        publisher.swap(None);
                        break;
                    } else {
                        publisher.swap(Some(receiver.consume()));
                    },
                }
            }
        });
        Self { _dropper: Arc::new(drop_owner), deployment }
    }

    pub fn latest(&self) -> Option<Arc<TabletDeployment>> {
        self.deployment.load_full()
    }
}

#[derive(Clone)]
pub struct ClusterDescriptorWatcher {
    _dropper: Arc<DropOwner>,
    descriptor: Arc<ArcSwapOption<ClusterDescriptor>>,
}

impl ClusterDescriptorWatcher {
    async fn watch(
        descriptor: Option<ClusterDescriptor>,
        watcher: Watcher,
        mut stream: WatchStream,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let descriptor = match descriptor {
            Some(descriptor) => descriptor,
            None => select! {
                _ = tokio::time::sleep(timeout.unwrap_or(CLUSTER_BOOT_TIMEOUT)) => bail!("cluster descriptor not found"),
                r = Self::poll_descriptor(&mut stream) => match r {
                    Err(err) => return Err(err),
                    Ok(descriptor) => descriptor,
                }
            },
        };
        let descriptor = Arc::new(ArcSwapOption::from_pointee(Some(descriptor)));
        let (drop_owner, drop_watcher) = utils::drop_watcher();
        tokio::spawn(Self::poll(watcher, stream, drop_watcher, descriptor.clone()));
        Ok(Self { _dropper: drop_owner.into(), descriptor })
    }

    async fn poll(
        _watcher: Watcher,
        mut stream: WatchStream,
        mut drop_watcher: DropWatcher,
        descriptor: Arc<ArcSwapOption<ClusterDescriptor>>,
    ) -> Result<()> {
        loop {
            select! {
                _ = drop_watcher.dropped() => break,
                Ok(message) = stream.message() => {
                    let Some(response) = message else {
                        break;
                    };
                    let new = Self::parse_descriptor(response)?;
                    descriptor.swap(Some(Arc::new(new)));
                },
            }
        }
        Ok(())
    }

    async fn poll_descriptor(stream: &mut WatchStream) -> Result<ClusterDescriptor> {
        match stream.message().await? {
            None => bail!("cluster descriptor stream got closed"),
            Some(message) => Self::parse_descriptor(message),
        }
    }

    fn parse_descriptor(response: WatchResponse) -> Result<ClusterDescriptor> {
        let event = response.events().last().ok_or_else(|| anyhow!("cluster descriptor watch receives no event"))?;
        match event.event_type() {
            EventType::Delete => bail!("cluster descriptor got deleted"),
            EventType::Put => {
                let kv = event.kv().ok_or_else(|| anyhow!("cluster descriptor watch receives no kv"))?;
                let descriptor = ClusterDescriptor::decode(kv.value())?;
                Ok(descriptor)
            },
        }
    }

    pub fn latest(&self) -> Option<Arc<ClusterDescriptor>> {
        self.descriptor.load_full()
    }
}

#[derive(Clone)]
pub struct ClusterDeploymentWatcher {
    monitor: ClusterDeploymentMonitor,
    receiver: watch::Receiver<Arc<TabletDeployment>>,
    deployment: Arc<TabletDeployment>,
}

impl ClusterDeploymentWatcher {
    pub async fn new(uri: ServiceUri<'_>, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher> {
        let (resource_uri, params) = uri.parts();
        let etcd = EtcdHelper::connect(resource_uri.endpoint(), params).await?;
        let mut client =
            EtcdClusterClient { name: Default::default(), root: resource_uri.path().to_compact_string(), etcd };
        client.watch_deployment(timeout).await
    }

    /// Waits for and consumes update.
    pub async fn changed(&mut self) -> Option<&TabletDeployment> {
        if self.receiver.changed().await.is_err() {
            None
        } else {
            Some(self.latest())
        }
    }

    /// Consumes latest update.
    pub fn latest(&mut self) -> &TabletDeployment {
        if let Some(deployment) = self.read_deployment() {
            self.deployment = deployment;
        }
        &self.deployment
    }

    pub fn monitor(&self) -> ClusterDeploymentMonitor {
        self.monitor.clone()
    }

    fn read_deployment(&mut self) -> Option<Arc<TabletDeployment>> {
        let value = self.receiver.borrow_and_update();
        if Arc::ptr_eq(&self.deployment, &value) {
            None
        } else {
            Some((*value).clone())
        }
    }

    pub async fn wait_for(&mut self, mut f: impl FnMut(&TabletDeployment) -> bool) -> Option<&TabletDeployment> {
        {
            let watcher = unsafe { &mut *(self as *const Self as *mut Self) };
            let latest = watcher.latest();
            if f(latest) {
                return Some(latest);
            }
        }
        loop {
            let watcher = unsafe { &mut *(self as *const Self as *mut Self) };
            let Some(latest) = watcher.changed().await else {
                break;
            };
            if f(latest) {
                return Some(latest);
            }
        }
        None
    }

    async fn poll(
        _watcher: Watcher,
        mut stream: WatchStream,
        sender: watch::Sender<Arc<TabletDeployment>>,
    ) -> Result<()> {
        loop {
            select! {
                _ = sender.closed() => break,
                Ok(message) = stream.message() => {
                    let Some(response) = message else {
                        break;
                    };
                    let meta = parse_deployment(response)?;
                    if sender.send(Arc::new(meta)).is_err() {
                        break;
                    }
                },
            }
        }
        Ok(())
    }

    async fn watch(watcher: Watcher, stream: WatchStream, deployment: TabletDeployment) -> Self {
        let deployment = Arc::new(deployment);
        let (sender, receiver) = watch::channel(deployment.clone());
        tokio::spawn(async move { Self::poll(watcher, stream, sender).await });
        let monitor = ClusterDeploymentMonitor::new(receiver.clone(), deployment.clone()).await;
        ClusterDeploymentWatcher { monitor, receiver, deployment }
    }
}

#[async_trait]
pub trait ClusterMetaHandle: Send {
    async fn watch_descriptor(&mut self, timeout: Option<Duration>) -> Result<ClusterDescriptorWatcher>;

    async fn watch_deployment(&mut self, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher>;

    async fn join(self) -> Result<()>;
}

#[async_trait]
impl ClusterMetaHandle for EtcdClusterMetaHandle {
    async fn watch_descriptor(&mut self, timeout: Option<Duration>) -> Result<ClusterDescriptorWatcher> {
        ClusterMetaClient::watch_descriptor(self, timeout).await
    }

    async fn watch_deployment(&mut self, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher> {
        ClusterMetaClient::watch_deployment(self, timeout).await
    }

    async fn join(self) -> Result<()> {
        self.daemon.await?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use assertor::*;
    use async_trait::async_trait;
    use ignore_result::Ignore;
    use prost::Message;
    use tokio::net::TcpListener;
    use tokio::select;
    use tokio::sync::watch;
    use tonic::transport::server::TcpIncoming;

    use crate::cluster::etcd::tests::*;
    use crate::cluster::etcd::EtcdHelper;
    use crate::cluster::{EtcdNodeRegistry, *};
    use crate::endpoint::{Endpoint, ServiceUri};
    use crate::log::tests::*;
    use crate::log::LogRegistry;
    use crate::protos::{
        BatchRequest,
        BatchResponse,
        LocateRequest,
        LocateResponse,
        ParticipateTxnRequest,
        ParticipateTxnResponse,
        TabletDeployRequest,
        TabletDeployResponse,
        TabletDeployment,
        TabletHeartbeatRequest,
        TabletHeartbeatResponse,
        TabletService,
        TabletServiceServer,
    };
    use crate::utils::{self, DropOwner, WatchConsumer as _};

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    #[should_panic(expected = "cluster deployment not found")]
    async fn test_cluster_deployment_watcher_not_found() {
        let etcd = etcd_container();
        ClusterDeploymentWatcher::new(etcd.uri(), Some(Duration::from_secs(2))).await.unwrap();
    }

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn test_cluster_deployment_watcher_update() {
        let etcd = etcd_container();
        let etcd_uri = etcd.uri();
        let mut client = EtcdHelper::connect(etcd_uri.endpoint(), etcd_uri.params()).await.unwrap();
        let mut deployment = TabletDeployment::default();
        client.put(r"/meta/deployment", deployment.encode_to_vec(), None).await.unwrap();

        let mut watcher = ClusterDeploymentWatcher::new(etcd_uri, Some(Duration::from_secs(2))).await.unwrap();
        assert_that!(watcher.latest()).is_equal_to(&deployment);

        for _ in 0..5 {
            deployment =
                TabletDeployment { epoch: deployment.epoch + 1, generation: deployment.generation + 1, ..deployment };
            client.put(r"/meta/deployment", deployment.encode_to_vec(), None).await.unwrap();
            let deployment = watcher.changed().await.unwrap().clone();
            assert_that!(deployment).is_equal_to(&deployment);
            assert_that!(watcher.latest()).is_equal_to(&deployment);
        }
    }

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn test_cluster_deployment_watcher_deleted() {
        let etcd = etcd_container();
        let etcd_uri = etcd.uri();
        let mut client = EtcdHelper::connect(etcd_uri.endpoint(), etcd_uri.params()).await.unwrap();
        let deployment = TabletDeployment::default();
        client.put(r"/meta/deployment", deployment.encode_to_vec(), None).await.unwrap();

        let mut watcher = ClusterDeploymentWatcher::new(etcd_uri, Some(Duration::from_secs(2))).await.unwrap();

        client.delete(r"/meta/deployment", None).await.unwrap();
        assert!(watcher.changed().await.is_none());
    }

    struct TestTabletServiceInner {
        heartbeat: (watch::Sender<TabletHeartbeatRequest>, watch::Receiver<TabletHeartbeatRequest>),
        deployment: (watch::Sender<TabletDeployment>, watch::Receiver<TabletDeployment>),
    }

    impl Default for TestTabletServiceInner {
        fn default() -> Self {
            let heartbeat = watch::channel(Default::default());
            let deployment = watch::channel(Default::default());
            Self { heartbeat, deployment }
        }
    }

    #[derive(Default)]
    struct TestTabletService {
        inner: Arc<Mutex<TestTabletServiceInner>>,
    }

    impl TestTabletService {
        pub fn subscribe_deployment(&self) -> watch::Receiver<TabletDeployment> {
            self.inner.lock().unwrap().deployment.1.clone()
        }

        pub fn subscribe_heartbeat(&self) -> watch::Receiver<TabletHeartbeatRequest> {
            self.inner.lock().unwrap().heartbeat.1.clone()
        }
    }

    #[async_trait]
    impl TabletService for TestTabletService {
        type ParticipateTxnStream =
            tokio_stream::wrappers::UnboundedReceiverStream<Result<ParticipateTxnResponse, tonic::Status>>;

        async fn deploy_tablet(
            &self,
            request: tonic::Request<TabletDeployRequest>,
        ) -> Result<tonic::Response<TabletDeployResponse>, tonic::Status> {
            let request = request.into_inner();
            self.inner.lock().unwrap().deployment.0.send_replace(request.deployment.clone());
            let response = TabletDeployResponse { deployments: vec![request.deployment.clone()] };
            Ok(tonic::Response::new(response))
        }

        async fn heartbeat_tablet(
            &self,
            request: tonic::Request<TabletHeartbeatRequest>,
        ) -> Result<tonic::Response<TabletHeartbeatResponse>, tonic::Status> {
            let mut inner = self.inner.lock().unwrap();
            inner.heartbeat.0.send_replace(request.into_inner());
            let deployment = inner.deployment.1.consume();
            let response = TabletHeartbeatResponse { deployment: Some(deployment) };
            Ok(tonic::Response::new(response))
        }

        async fn batch(
            &self,
            _request: tonic::Request<BatchRequest>,
        ) -> Result<tonic::Response<BatchResponse>, tonic::Status> {
            unimplemented!()
        }

        async fn participate_txn(
            &self,
            _request: tonic::Request<tonic::Streaming<ParticipateTxnRequest>>,
        ) -> Result<tonic::Response<Self::ParticipateTxnStream>, tonic::Status> {
            unimplemented!()
        }

        async fn locate(
            &self,
            _request: tonic::Request<LocateRequest>,
        ) -> Result<tonic::Response<LocateResponse>, tonic::Status> {
            unimplemented!()
        }
    }

    struct TestNode {
        node_id: NodeId,
        service: Arc<TestTabletService>,
        _drop_owner: DropOwner,
    }

    impl TestNode {
        async fn start(uri: ServiceUri<'_>) -> Self {
            let node_id = NodeId::new_random();
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let address = format!("http://{}", listener.local_addr().unwrap());
            let endpoint = Endpoint::try_from(address.as_str()).unwrap();

            let (_nodes, lease) =
                EtcdNodeRegistry::join(uri, node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
            let (_drop_owner, mut drop_watcher) = utils::drop_watcher();

            let service = Arc::new(TestTabletService::default());
            let incoming = TcpIncoming::from_listener(listener, true, Some(Duration::from_millis(300))).unwrap();
            let node = TestNode { node_id, service: service.clone(), _drop_owner };
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(TabletServiceServer::from_arc(service))
                    .serve_with_incoming_shutdown(incoming, async move { drop_watcher.dropped().await })
                    .await
                    .ignore();
                drop(lease)
            });
            node
        }
    }

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn test_cluster_meta_deploy() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();
        let node1 = TestNode::start(cluster_uri.clone()).await;

        let log_factory = TestLogFactory::new("test");
        let mut log_registry = LogRegistry::default();
        log_registry.register(log_factory.clone()).unwrap();
        let log_manager =
            log_registry.into_manager(&ServiceUri::try_from("test://test-cluster").unwrap()).await.unwrap();
        let log_manager = Arc::new(log_manager);

        let (nodes, _cluster_lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), NodeId::new_random(), None).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.clone(), nodes).with_replicas(2);

        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env).await.unwrap();

        let mut deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let deployment_monitor = deployment_watcher.monitor();
        let cluster_deployment =
            deployment_watcher.wait_for(|deployment| !deployment.servers.is_empty()).await.unwrap().clone();

        let mut deployment_receiver = node1.service.subscribe_deployment();
        deployment_receiver.changed().await.unwrap();
        assert_that!(deployment_receiver.consume()).is_equal_to(&cluster_deployment);
        assert_that!(cluster_deployment.servers).is_equal_to(vec![node1.node_id.to_string()]);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_that!(deployment_monitor.latest().unwrap().as_ref()).is_equal_to(&cluster_deployment);

        let mut heartbeat_receiver = node1.service.subscribe_heartbeat();

        heartbeat_receiver.changed().await.unwrap();
        assert_that!(heartbeat_receiver.borrow_and_update().clone())
            .is_equal_to(TabletHeartbeatRequest { tablet_id: 1 });

        let node2 = TestNode::start(cluster_uri.clone()).await;
        let cluster_deployment = deployment_watcher.changed().await.unwrap().clone();
        assert_that!(cluster_deployment.servers)
            .is_equal_to(vec![node1.node_id.to_string(), node2.node_id.to_string()]);

        deployment_receiver.changed().await.unwrap();
        assert_that!(deployment_receiver.consume()).is_equal_to(&cluster_deployment);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_that!(deployment_monitor.latest().unwrap().as_ref()).is_equal_to(&cluster_deployment);

        let node3 = TestNode::start(cluster_uri.clone()).await;
        select! {
            _ = deployment_watcher.changed() => { panic!("expect no new deployment") },
            _ = tokio::time::sleep(Duration::from_secs(2)) => {},
        }

        drop(node1);
        let cluster_deployment = deployment_watcher
            .wait_for(|deployment| *deployment.servers.last().unwrap() == node3.node_id.to_string())
            .await
            .unwrap()
            .clone();
        assert_that!(cluster_deployment.servers)
            .is_equal_to(vec![node2.node_id.to_string(), node3.node_id.to_string()]);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_that!(deployment_monitor.latest().unwrap().as_ref()).is_equal_to(&cluster_deployment);

        drop(node2);
        let cluster_deployment = deployment_watcher
            .wait_for(|deployment| *deployment.servers.first().unwrap() == node3.node_id.to_string())
            .await
            .unwrap()
            .clone();
        assert_that!(cluster_deployment.servers).is_equal_to(vec![node3.node_id.to_string()]);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_that!(deployment_monitor.latest().unwrap().as_ref()).is_equal_to(&cluster_deployment);

        let (nodes2, _cluster_lease2) =
            EtcdNodeRegistry::join(cluster_uri.clone(), NodeId::new_random(), None).await.unwrap();
        let cluster_env2 = ClusterEnv::new(log_manager, nodes2).with_replicas(2);
        let mut cluster_meta_handle2 =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env2).await.unwrap();

        drop(cluster_meta_handle);

        let mut deployment_watcher2 = cluster_meta_handle2.watch_deployment(None).await.unwrap();
        let cluster_deployment2 =
            deployment_watcher2.wait_for(|d| d.epoch >= cluster_deployment.epoch).await.unwrap().clone();
        assert_that!(cluster_deployment2).is_equal_to(cluster_deployment);
    }
}
