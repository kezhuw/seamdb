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

use anyhow::{anyhow, bail, ensure, Context, Result};
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
use ignore_result::Ignore;
use prost::Message as _;
use tokio::select;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint as TonicEndpoint};
use tracing::{span, Instrument, Level};
use uuid::Uuid;

use super::etcd::EtcdHelper;
use super::node::{NodeId, NodeRegistry};
use super::ClusterEnv;
use crate::endpoint::{Endpoint, OwnedEndpoint, ServiceUri};
use crate::keys;
use crate::log::LogAddress;
use crate::protos::{
    self,
    ClusterMeta,
    DataMessage,
    DataOperation,
    HeartbeatRequest,
    ManifestMessage,
    TabletDeployRequest,
    TabletDeployment,
    TabletDescription,
    TabletDescriptor,
    TabletListRequest,
    TabletManifest,
    TabletMergeBounds,
    TabletRange,
    TabletServiceClient,
    TabletStore,
    Temporal,
    Timestamp,
};
use crate::utils::{self, DropOwner, WatchConsumer as _};

type Revision = i64;

const HEARTBEAT_DURATION: Duration = Duration::from_secs(1);
const HEARTBEAT_EXPIRATION: Duration = HEARTBEAT_DURATION.saturating_mul(4);
const CLUSTER_BOOT_TIMEOUT: Duration = Duration::from_secs(5);

const BOOTSTRAP_LOGS_NUMBER: usize = 6;

fn parse_meta(name: &str, response: WatchResponse) -> Result<ClusterMeta> {
    let event = response.events().last().ok_or_else(|| anyhow!("cluster meta watch receives no event"))?;
    match event.event_type() {
        EventType::Delete => bail!("cluster meta got deleted"),
        EventType::Put => {
            let kv = event.kv().ok_or_else(|| anyhow!("cluster meta watch receives no kv"))?;
            let meta = ClusterMeta::decode(kv.value())?;
            if meta.name != name {
                bail!("cluster meta name mismatch: expect {}, got {}", name, meta.name)
            }
            Ok(meta)
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

    async fn create_meta(&mut self, lock: &[u8]) -> Result<(Revision, Vec<String>, ClusterMeta)> {
        let (log_names, log_uris) = self.random_bootstrap_logs();
        let meta = ClusterMeta {
            name: self.client.name.to_string(),
            epoch: 0,
            generation: 0,
            servers: Default::default(),
            log: Default::default(),
            bootstrap_logs: log_uris,
            obsoleted_logs: Default::default(),
        };
        let revision = self.put_meta(lock, &meta, None).await?;
        Ok((revision, log_names, meta))
    }

    async fn bootstrap_tablet_data(
        &mut self,
        log_name: String,
        writes: impl Into<Vec<protos::Write>>,
        ts: Timestamp,
    ) -> Result<LogAddress> {
        let log_address = self.env.log().create_log(&log_name, ByteSize::mib(512)).await?;
        let mut log_producer = self.env.log().produce_log(&log_address).await?;
        let message = DataMessage {
            epoch: 0,
            sequence: 1,
            temporal: Some(Temporal::Timestamp(Timestamp::default())),
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
        data_log_uri: LogAddress,
        id: u64,
        start: &[u8],
        end: &[u8],
    ) -> Result<LogAddress> {
        let tablet = TabletDescription {
            id,
            generation: 0,
            store: TabletStore {
                segments: Default::default(),
                log: data_log_uri.into(),
                file: Default::default(),
                range: Some(TabletRange { start: start.to_owned(), end: end.to_owned() }),
            },
        };
        let manifest = TabletManifest { tablet, ..TabletManifest::default() };
        let message = ManifestMessage { epoch: 0, sequence: 1, manifest: Some(manifest) }.encode_to_vec();
        let log_address = self.env.log().create_log(&log_name, ByteSize::mib(10)).await?;
        let mut log_producer = self.env.log().produce_log(&log_address).await?;
        log_producer.send(&message.encode_to_vec()).await?;
        Ok(log_address)
    }

    async fn bootstrap_tablet_manifest(
        &mut self,
        log_name: String,
        data_log_uri: LogAddress,
        id: u64,
        start: &[u8],
        end: &[u8],
    ) -> Result<TabletDescriptor> {
        let log_uri = self.bootstrap_tablet_manifest_log(log_name, data_log_uri, id, start, end).await?;
        let descriptor = TabletDescriptor {
            id,
            generation: 0,
            range: TabletRange { start: start.to_owned(), end: end.to_owned() },
            log: log_uri.into(),
            merge_bounds: TabletMergeBounds::All,
        };
        Ok(descriptor)
    }

    async fn bootstrap_user_tablet(&mut self, log_names: &mut Vec<String>, ts: Timestamp) -> Result<TabletDescriptor> {
        let key = keys::system_key("tablet-id-counter".as_bytes());
        let value = protos::Value::Int(3);
        let write = protos::Write { key, value: Some(value), sequence: 0 };
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), [write], ts).await?;
        self.bootstrap_tablet_manifest(log_names.pop().unwrap(), data_log_uri, 3, keys::DATA_KEY_PREFIX, keys::MAX_KEY)
            .await
    }

    async fn bootstrap_range_tablet(
        &mut self,
        log_names: &mut Vec<String>,
        user_tablet_descriptor: TabletDescriptor,
        ts: Timestamp,
    ) -> Result<TabletDescriptor> {
        let key = keys::range_key(keys::MAX_KEY);
        let deployment =
            TabletDeployment { tablet: user_tablet_descriptor, epoch: 0, generation: 0, servers: Default::default() };
        let value = protos::Value::Bytes(deployment.encode_to_vec());
        let write = protos::Write { key, value: Some(value), sequence: 0 };
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), [write], ts).await?;
        self.bootstrap_tablet_manifest(
            log_names.pop().unwrap(),
            data_log_uri,
            2,
            keys::RANGE_KEY_PREFIX,
            keys::DATA_KEY_PREFIX,
        )
        .await
    }

    async fn bootstrap_root_tablet(
        &mut self,
        log_names: &mut Vec<String>,
        range_tablet_descriptor: TabletDescriptor,
        ts: Timestamp,
    ) -> Result<LogAddress> {
        let key = keys::root_key(keys::MAX_KEY);
        let deployment =
            TabletDeployment { tablet: range_tablet_descriptor, epoch: 0, generation: 0, servers: Default::default() };
        let value = protos::Value::Bytes(deployment.encode_to_vec());
        let write = protos::Write { key, value: Some(value), sequence: 0 };
        let data_log_uri = self.bootstrap_tablet_data(log_names.pop().unwrap(), [write], ts).await?;
        self.bootstrap_tablet_manifest_log(
            log_names.pop().unwrap(),
            data_log_uri,
            1,
            keys::ROOT_KEY_PREFIX,
            keys::RANGE_KEY_PREFIX,
        )
        .await
    }

    async fn bootstrap_meta(
        &mut self,
        lock: &[u8],
        revision: Revision,
        mut log_names: Vec<String>,
        mut meta: ClusterMeta,
    ) -> Result<(Revision, ClusterMeta)> {
        let now = self.env.clock().now();
        let user_tablet_descriptor = self.bootstrap_user_tablet(&mut log_names, now).await?;
        let range_tablet_descriptor = self.bootstrap_range_tablet(&mut log_names, user_tablet_descriptor, now).await?;
        let root_tablet_log_uri = self.bootstrap_root_tablet(&mut log_names, range_tablet_descriptor, now).await?;
        meta.generation += 1;
        meta.log = root_tablet_log_uri.into();
        meta.bootstrap_logs.clear();
        let revision = self.put_meta(lock, &meta, Some(revision)).await?;
        Ok((revision, meta))
    }

    async fn sleep(d: Duration) {
        if !d.is_zero() {
            tokio::time::sleep(d).await
        }
    }

    async fn publish_meta(
        &mut self,
        lock: &[u8],
        revision: Option<Revision>,
        meta: &ClusterMeta,
        channel: &watch::Sender<TabletDeployment>,
    ) -> Result<Revision> {
        let revision = self.put_meta(lock, meta, revision).await?;
        channel.send(TabletDeployment::from(meta)).ignore();
        Ok(revision)
    }

    async fn serve_meta(&mut self, lock: Vec<u8>, mut revision: Revision, mut meta: ClusterMeta) -> Result<()> {
        let (crash_reporter, mut crash_watcher) = mpsc::unbounded_channel();
        let (deployment_sender, deployment_watcher) = watch::channel(TabletDeployment::from(&meta));
        for node in meta.servers.clone().into_iter() {
            let node = NodeId(node);
            let nodes = self.env.nodes().clone();
            let deployment_receiver = deployment_watcher.clone();
            let crash_reporter = crash_reporter.clone();
            let deployment_span = span!(Level::INFO, "cluster meta deployment(recover)", %node);
            tokio::spawn(
                async move {
                    if let Err(err) = nodes.recover_deployment(&node, deployment_receiver).await {
                        tracing::info!("meta deployment terminated: {}", err);
                    }
                    crash_reporter.send(node).ignore();
                }
                .instrument(deployment_span),
            );
        }
        let mut changed = false;
        let min_servers = self.env.replicas();
        let mut backoff = Duration::ZERO;
        loop {
            select! {
                biased;
                _ = Self::sleep(backoff), if changed || meta.servers.len() < min_servers => {
                    if meta.servers.len() >= min_servers {
                        changed = false;
                        meta.generation += 1;
                        revision = self.publish_meta(&lock, Some(revision), &meta, &deployment_sender).await?;
                        continue;
                    }
                    // TODO: the selected node could be the one just reported as crashed
                    let Some((node, addr)) = self.env.nodes().select_node() else {
                        continue;
                    };
                    if !meta.servers.iter().any(|s| *s == node.0) {
                        if meta.servers.is_empty() {
                            meta.epoch += 1;
                            meta.generation = 0;
                        } else {
                            meta.generation += 1;
                        }
                        meta.servers.push(node.0.clone());
                        revision = self.publish_meta(&lock, Some(revision), &meta, &deployment_sender).await?;
                        let nodes = self.env.nodes().clone();
                        let deployment_receiver = deployment_watcher.clone();
                        let crash_reporter = crash_reporter.clone();
                        let deployment_span = span!(Level::INFO, "cluster meta deployment", %node, %addr);
                        tokio::spawn(async move {
                            if let Err(err) = nodes.start_deployment(&node, addr, deployment_receiver).await {
                                tracing::info!("meta deployment terminated: {}", err);
                            }
                            crash_reporter.send(node).ignore();
                        }.instrument(deployment_span));
                        continue;
                    } else if changed {
                        meta.generation += 1;
                        revision = self.publish_meta(&lock, Some(revision), &meta, &deployment_sender).await?;
                        continue;
                    }
                    backoff += backoff / 2 + Duration::from_secs(1);
                },
                Some(node) = crash_watcher.recv() => {
                    let Some(position) = meta.servers.iter().position(|s| *s == node.0) else {
                        continue;
                    };
                    meta.servers.remove(position);
                    if meta.servers.is_empty() {
                        backoff = Duration::ZERO;
                    } else if position == 0 {
                        meta.epoch += 1;
                        meta.generation = 0;
                        revision = self.publish_meta(&lock, Some(revision), &meta, &deployment_sender).await?;
                        continue;
                    }
                    changed = true;
                    backoff = backoff.min(Duration::from_secs(1));
                },
            }
        }
    }

    async fn init_meta(&mut self, lock: &[u8]) -> Result<(Revision, ClusterMeta)> {
        let (revision, log_names, meta) = self.create_meta(lock).await?;
        self.bootstrap_meta(lock, revision, log_names, meta).await
    }

    async fn restore_meta(
        &mut self,
        lock: &[u8],
        mut revision: i64,
        mut meta: ClusterMeta,
    ) -> Result<(Revision, ClusterMeta)> {
        if meta.name != self.client.name {
            return Err(anyhow!("unexpected cluster name: expect {}, got {}", self.client.name, meta.name));
        }
        if !meta.bootstrap_logs.is_empty() {
            meta.obsoleted_logs.append(&mut meta.bootstrap_logs);
            let (log_names, log_uris) = self.random_bootstrap_logs();
            meta.bootstrap_logs = log_uris;
            revision = self.put_meta(lock, &meta, Some(revision)).await?;
            self.bootstrap_meta(lock, revision, log_names, meta).await
        } else {
            Ok((revision, meta))
        }
    }

    async fn serve(&mut self) -> Result<()> {
        let lease = EtcdHelper::grant_lease(&mut self.client.etcd, None).await?;
        let lock = self.lock(lease.id()).await?;
        let (revision, meta) = match self.get_meta().await? {
            (_, None) => self.init_meta(&lock).await?,
            (revision, Some(meta)) => self.restore_meta(&lock, revision, meta).await?,
        };
        self.serve_meta(lock, revision, meta).await
    }

    pub async fn start(
        name: impl Into<CompactString>,
        uri: ServiceUri,
        env: ClusterEnv,
    ) -> Result<Box<dyn ClusterMetaHandle>> {
        let (resource_id, params) = uri.into();
        let etcd = EtcdHelper::connect(resource_id.endpoint(), &params).await?;
        let client = EtcdClusterClient { name: name.into(), root: resource_id.path().to_compact_string(), etcd };
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
impl ClusterMetaDeployer for Arc<dyn NodeRegistry> {
    fn nodes(&self) -> &dyn NodeRegistry {
        self.as_ref()
    }
}

#[async_trait]
impl ClusterMetaDeployer for dyn NodeRegistry {
    fn nodes(&self) -> &dyn NodeRegistry {
        self
    }
}

#[async_trait]
trait ClusterMetaDeployer {
    fn nodes(&self) -> &dyn NodeRegistry;

    async fn connect_node(&self, node: &NodeId) -> Result<TabletServiceClient<Channel>> {
        let addr =
            self.nodes().get_node_endpoint(node).ok_or_else(|| anyhow!("node {node} not found, probably died"))?;
        self.connect_node_with_addr(node, addr.as_ref()).await
    }

    async fn connect_node_with_addr(&self, node: &NodeId, addr: Endpoint<'_>) -> Result<TabletServiceClient<Channel>> {
        let channel = TonicEndpoint::from_shared(addr.to_string())
            .with_context(|| format!("node {:?} get invalid grpc addr {}", node, addr))?
            .connect_timeout(HEARTBEAT_EXPIRATION)
            .connect()
            .await
            .with_context(|| format!("fail to connect to {:?} at {}", node, addr))?;
        Ok(TabletServiceClient::new(channel))
    }

    async fn serve_deployment(
        &self,
        node: &NodeId,
        mut receiver: watch::Receiver<TabletDeployment>,
        mut client: TabletServiceClient<Channel>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(HEARTBEAT_DURATION);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            select! {
                _ = receiver.changed() => {
                    let deployment = receiver.consume();
                    let is_member = deployment.servers.iter().any(|s| *s == node.0);
                    let request = TabletDeployRequest { deployment };
                    client.deploy_tablet(request).await?;
                    if !is_member {
                        break;
                    }
                },
                _ = interval.tick() => {
                    client.heartbeat(HeartbeatRequest{ tablet_id: 1 }).await?;
                },
            }
        }
        Ok(())
    }

    async fn reestablish_deployment(
        &self,
        node: &NodeId,
        receiver: &mut watch::Receiver<TabletDeployment>,
    ) -> Result<TabletServiceClient<Channel>> {
        let range = TabletRange { start: keys::ROOT_KEY_PREFIX.to_owned(), end: keys::RANGE_KEY_PREFIX.to_owned() };
        let request = TabletListRequest { range };
        let mut client = self.connect_node(node).await?;
        let mut response = client.list_tablets(request).await?.into_inner();
        match response.deployments.len() {
            0 => bail!("no deployments on node {:?}", node),
            1 => {},
            n => bail!("{} meta deployments on node {:?}", n, node),
        };
        let deployed = response.deployments.remove(0);
        let deployment = receiver.consume();
        ensure!(
            deployment.epoch >= deployed.epoch,
            "cluster meta epoch {} is smaller than deployment epoch {}",
            deployment.epoch,
            deployed.epoch
        );
        if deployment.epoch != deployed.epoch || deployment.generation != deployed.generation {
            client.deploy_tablet(TabletDeployRequest { deployment }).await?;
        }
        Ok(client)
    }

    async fn start_deployment(
        &self,
        node: &NodeId,
        addr: OwnedEndpoint,
        mut receiver: watch::Receiver<TabletDeployment>,
    ) -> Result<()> {
        let mut client = self.connect_node_with_addr(node, addr.as_ref()).await?;
        client.deploy_tablet(TabletDeployRequest { deployment: receiver.consume() }).await?;
        self.serve_deployment(node, receiver, client).await
    }

    async fn recover_deployment(&self, node: &NodeId, mut receiver: watch::Receiver<TabletDeployment>) -> Result<()> {
        let client = self.reestablish_deployment(node, &mut receiver).await?;
        self.serve_deployment(node, receiver, client).await
    }
}

#[async_trait]
trait ClusterMetaClient: Send + Sync + 'static {
    fn name(&self) -> &str;

    fn root(&self) -> &str;

    fn etcd(&mut self) -> &mut Client;

    fn meta_key(&self) -> String {
        format!("{}/meta/data", self.root())
    }

    fn lock_key(&self) -> String {
        format!("{}/meta/leader", self.root())
    }

    async fn lock(&mut self, lease_id: i64) -> Result<Vec<u8>> {
        let path = self.lock_key();
        EtcdHelper::lock(self.etcd(), path, lease_id).await
    }

    async fn get_meta(&mut self) -> Result<(Revision, Option<ClusterMeta>)> {
        let path = self.meta_key();
        let mut response = self.etcd().get(path, None).await?.0;
        if let Some(kv) = response.kvs.pop() {
            let revision = kv.mod_revision;
            let meta = ClusterMeta::decode(kv.value.as_slice())?;
            if meta.name != self.name() {
                bail!("cluster meta name mismatch: expect {}, got {}", self.name(), meta.name)
            }
            Ok((revision, Some(meta)))
        } else if let Some(header) = response.header {
            Ok((header.revision, None))
        } else {
            Err(anyhow!("etcd get reponse has no header"))
        }
    }

    async fn put_meta(&mut self, lock: &[u8], meta: &ClusterMeta, revision: Option<Revision>) -> Result<Revision> {
        let key = self.meta_key().into_bytes();
        let revision_cmp = if let Some(revision) = revision {
            Compare::mod_revision(key.clone(), CompareOp::Equal, revision)
        } else {
            Compare::create_revision(key.clone(), CompareOp::Equal, 0)
        };
        let lock_cmp = Compare::create_revision(lock, CompareOp::NotEqual, 0);
        let put = TxnOp::put(key, meta.encode_to_vec(), None);
        let txn = Txn::new().when([revision_cmp, lock_cmp]).and_then([put]);
        let response = self.etcd().txn(txn).await?;
        if !response.succeeded() {
            return Err(anyhow!("cluster meta changed or lock expired"));
        }
        Ok(response.header().unwrap().revision())
    }

    async fn poll_meta(&self, stream: &mut WatchStream) -> Result<ClusterMeta> {
        match stream.message().await? {
            None => bail!("cluster meta stream got closed"),
            Some(message) => parse_meta(self.name(), message),
        }
    }

    async fn watch_deployment(&mut self, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher> {
        let (revision, meta) = self.get_meta().await?;
        let key = self.meta_key();
        let options = WatchOptions::new().with_start_revision(revision + 1);
        let (watcher, mut stream) = self.etcd().watch(key, Some(options)).await?;
        let meta = if let Some(meta) = meta {
            meta
        } else {
            select! {
                _ = tokio::time::sleep(timeout.unwrap_or(CLUSTER_BOOT_TIMEOUT)) => bail!("cluster meta not found"),
                r = self.poll_meta(&mut stream) => match r {
                    Err(err) => return Err(err),
                    Ok(meta) => meta,
                },
            }
        };
        Ok(ClusterDeploymentWatcher::watch(watcher, stream, meta))
    }
}

impl ClusterMetaClient for EtcdClusterClient {
    fn name(&self) -> &str {
        &self.name
    }

    fn root(&self) -> &str {
        &self.root
    }

    fn etcd(&mut self) -> &mut Client {
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

    fn etcd(&mut self) -> &mut Client {
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

    fn etcd(&mut self) -> &mut Client {
        &mut self.client.etcd
    }
}

#[derive(Clone)]
pub struct ClusterDeploymentWatcher {
    receiver: watch::Receiver<Arc<TabletDeployment>>,
    deployment: Arc<TabletDeployment>,
}

impl ClusterDeploymentWatcher {
    pub async fn new(name: &str, uri: ServiceUri, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher> {
        let (resource_id, params) = uri.into();
        let etcd = EtcdHelper::connect(resource_id.endpoint(), &params).await?;
        let mut client =
            EtcdClusterClient { name: name.to_compact_string(), root: resource_id.path().to_compact_string(), etcd };
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
        name: String,
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
                    let meta = parse_meta(&name, response)?;
                    if sender.send(Arc::new(meta.into())).is_err() {
                        break;
                    }
                },
            }
        }
        Ok(())
    }

    fn watch(watcher: Watcher, stream: WatchStream, mut meta: ClusterMeta) -> Self {
        let name = std::mem::take(&mut meta.name);
        let deployment = Arc::new(TabletDeployment::from(meta));
        let (sender, receiver) = watch::channel(deployment.clone());
        tokio::spawn(async move { Self::poll(name, watcher, stream, sender).await });
        ClusterDeploymentWatcher { receiver, deployment }
    }
}

#[async_trait]
pub trait ClusterMetaHandle: Send {
    async fn watch_deployment(&mut self, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher>;

    async fn join(self) -> Result<()>;
}

#[async_trait]
impl ClusterMetaHandle for EtcdClusterMetaHandle {
    async fn watch_deployment(&mut self, timeout: Option<Duration>) -> Result<ClusterDeploymentWatcher> {
        let client = self as &mut dyn ClusterMetaClient;
        client.watch_deployment(timeout).await
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
    use tracing_test::traced_test;

    use crate::cluster::etcd::tests::*;
    use crate::cluster::etcd::EtcdHelper;
    use crate::cluster::{EtcdNodeRegistry, *};
    use crate::endpoint::{Endpoint, ServiceUri};
    use crate::keys;
    use crate::log::tests::*;
    use crate::log::LogRegistry;
    use crate::protos::{
        HeartbeatRequest,
        HeartbeatResponse,
        TabletDeployRequest,
        TabletDeployResponse,
        TabletDeployment,
        TabletListRequest,
        TabletListResponse,
        TabletRange,
        TabletService,
        TabletServiceServer,
    };
    use crate::utils::{self, DropOwner, WatchConsumer as _};

    #[tokio::test]
    #[should_panic(expected = "cluster meta not found")]
    async fn test_cluster_meta_watcher_not_found() {
        let etcd = etcd_container();
        ClusterDeploymentWatcher::new("cluster1", etcd.uri(), Some(Duration::from_secs(2))).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "cluster meta name mismatch")]
    async fn test_cluster_meta_watcher_mismatch_name() {
        let etcd = etcd_container();
        let etcd_uri = etcd.uri();
        let mut client = EtcdHelper::connect(etcd_uri.endpoint(), etcd_uri.params()).await.unwrap();
        let meta = ClusterMeta { name: "cluster2".to_string(), ..ClusterMeta::default() };
        client.put(r"/meta/data", meta.encode_to_vec(), None).await.unwrap();

        ClusterDeploymentWatcher::new("cluster1", etcd_uri, Some(Duration::from_secs(2))).await.unwrap();
    }

    #[tokio::test]
    async fn test_cluster_meta_watcher_update() {
        let etcd = etcd_container();
        let etcd_uri = etcd.uri();
        let mut client = EtcdHelper::connect(etcd_uri.endpoint(), etcd_uri.params()).await.unwrap();
        let mut meta = ClusterMeta { name: "cluster1".to_string(), ..ClusterMeta::default() };
        client.put(r"/meta/data", meta.encode_to_vec(), None).await.unwrap();

        let mut watcher =
            ClusterDeploymentWatcher::new("cluster1", etcd_uri, Some(Duration::from_secs(2))).await.unwrap();
        assert_that!(watcher.latest()).is_equal_to(&TabletDeployment::from(meta.clone()));

        for _ in 0..5 {
            meta = ClusterMeta { epoch: meta.epoch + 1, generation: meta.generation + 1, ..meta };
            client.put(r"/meta/data", meta.encode_to_vec(), None).await.unwrap();
            let deployment = watcher.changed().await.unwrap().clone();
            assert_that!(deployment).is_equal_to(&TabletDeployment::from(meta.clone()));
            assert_that!(watcher.latest()).is_equal_to(&deployment);
        }
    }

    #[tokio::test]
    async fn test_cluster_meta_watcher_deleted() {
        let etcd = etcd_container();
        let etcd_uri = etcd.uri();
        let mut client = EtcdHelper::connect(etcd_uri.endpoint(), etcd_uri.params()).await.unwrap();
        let meta = ClusterMeta { name: "cluster1".to_string(), ..ClusterMeta::default() };
        client.put(r"/meta/data", meta.encode_to_vec(), None).await.unwrap();

        let mut watcher =
            ClusterDeploymentWatcher::new("cluster1", etcd_uri, Some(Duration::from_secs(2))).await.unwrap();

        client.delete(r"/meta/data", None).await.unwrap();
        assert!(watcher.changed().await.is_none());
    }

    struct TestTabletServiceInner {
        list: (watch::Sender<TabletRange>, watch::Receiver<TabletRange>),
        heartbeat: (watch::Sender<HeartbeatRequest>, watch::Receiver<HeartbeatRequest>),
        deployment: (watch::Sender<TabletDeployment>, watch::Receiver<TabletDeployment>),
    }

    impl Default for TestTabletServiceInner {
        fn default() -> Self {
            let list = watch::channel(Default::default());
            let heartbeat = watch::channel(Default::default());
            let deployment = watch::channel(Default::default());
            Self { list, heartbeat, deployment }
        }
    }

    #[derive(Default)]
    struct TestTabletService {
        inner: Arc<Mutex<TestTabletServiceInner>>,
    }

    impl TestTabletService {
        pub fn subscribe_list(&self) -> watch::Receiver<TabletRange> {
            self.inner.lock().unwrap().list.1.clone()
        }

        pub fn subscribe_deployment(&self) -> watch::Receiver<TabletDeployment> {
            self.inner.lock().unwrap().deployment.1.clone()
        }

        pub fn subscribe_heartbeat(&self) -> watch::Receiver<HeartbeatRequest> {
            self.inner.lock().unwrap().heartbeat.1.clone()
        }
    }

    #[async_trait]
    impl TabletService for TestTabletService {
        async fn list_tablets(
            &self,
            request: tonic::Request<TabletListRequest>,
        ) -> Result<tonic::Response<TabletListResponse>, tonic::Status> {
            let mut inner = self.inner.lock().unwrap();
            inner.list.0.send_replace(request.into_inner().range);
            let deployment = inner.deployment.0.consume();
            let response = TabletListResponse { deployments: vec![deployment] };
            Ok(tonic::Response::new(response))
        }

        async fn deploy_tablet(
            &self,
            request: tonic::Request<TabletDeployRequest>,
        ) -> Result<tonic::Response<TabletDeployResponse>, tonic::Status> {
            let request = request.into_inner();
            self.inner.lock().unwrap().deployment.0.send_replace(request.deployment.clone());
            let response = TabletDeployResponse { deployments: vec![request.deployment.clone()] };
            Ok(tonic::Response::new(response))
        }

        async fn heartbeat(
            &self,
            request: tonic::Request<HeartbeatRequest>,
        ) -> Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
            self.inner.lock().unwrap().heartbeat.0.send_replace(request.into_inner());
            Ok(tonic::Response::new(HeartbeatResponse::default()))
        }
    }

    struct TestNode {
        node_id: NodeId,
        service: Arc<TestTabletService>,
        _drop_owner: DropOwner,
    }

    impl TestNode {
        async fn start(uri: ServiceUri) -> Self {
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
    #[traced_test]
    async fn test_cluster_meta_deploy() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();
        let node1 = TestNode::start(cluster_uri.clone()).await;

        let log_factory = TestLogFactory::new("test");
        let mut log_registry = LogRegistry::default();
        log_registry.register(log_factory.clone()).unwrap();
        let log_manager = log_registry
            .into_manager(&Endpoint::try_from("test://test-cluster").unwrap(), &Default::default())
            .await
            .unwrap();
        let log_manager = Arc::new(log_manager);

        let (nodes, _cluster_lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), NodeId::new_random(), None).await.unwrap();
        let cluster_env = ClusterEnv::new(log_manager.clone(), nodes).with_replicas(2);

        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env).await.unwrap();

        let mut deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_deployment =
            deployment_watcher.wait_for(|deployment| !deployment.servers.is_empty()).await.unwrap().clone();

        let mut deployment_receiver = node1.service.subscribe_deployment();
        deployment_receiver.changed().await.unwrap();
        assert_that!(deployment_receiver.consume()).is_equal_to(&cluster_deployment);
        assert_that!(cluster_deployment.servers).is_equal_to(vec![node1.node_id.to_string()]);

        let mut heartbeat_receiver = node1.service.subscribe_heartbeat();

        heartbeat_receiver.changed().await.unwrap();
        assert_that!(heartbeat_receiver.borrow_and_update().clone()).is_equal_to(HeartbeatRequest { tablet_id: 1 });

        let node2 = TestNode::start(cluster_uri.clone()).await;
        let cluster_deployment = deployment_watcher.changed().await.unwrap().clone();
        assert_that!(cluster_deployment.servers)
            .is_equal_to(vec![node1.node_id.to_string(), node2.node_id.to_string()]);

        deployment_receiver.changed().await.unwrap();
        assert_that!(deployment_receiver.consume()).is_equal_to(&cluster_deployment);

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

        drop(node2);
        let cluster_deployment = deployment_watcher
            .wait_for(|deployment| *deployment.servers.first().unwrap() == node3.node_id.to_string())
            .await
            .unwrap()
            .clone();
        assert_that!(cluster_deployment.servers).is_equal_to(vec![node3.node_id.to_string()]);

        let (nodes2, _cluster_lease2) =
            EtcdNodeRegistry::join(cluster_uri.clone(), NodeId::new_random(), None).await.unwrap();
        let cluster_env2 = ClusterEnv::new(log_manager, nodes2).with_replicas(2);
        let mut cluster_meta_handle2 =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env2).await.unwrap();

        let mut list_receiver = node3.service.subscribe_list();
        select! {
            _ = list_receiver.changed() => panic!("expect no deployment list"),
            _ = tokio::time::sleep(Duration::from_secs(2)) => {},
        }

        drop(cluster_meta_handle);

        list_receiver.changed().await.unwrap();
        let range = list_receiver.consume();
        assert_that!(range).is_equal_to(TabletRange {
            start: keys::ROOT_KEY_PREFIX.to_owned(),
            end: keys::RANGE_KEY_PREFIX.to_owned(),
        });

        let mut deployment_watcher2 = cluster_meta_handle2.watch_deployment(None).await.unwrap();
        let cluster_deployment2 =
            deployment_watcher2.wait_for(|d| d.epoch >= cluster_deployment.epoch).await.unwrap().clone();
        assert_that!(cluster_deployment2).is_equal_to(cluster_deployment);
    }
}
