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

use std::cmp;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_cell::sync::{AsyncCell, Get};
use asyncs::select;
use hashbrown::hash_map::{Entry as HashEntry, HashMap};
use hex_simd::AsciiCase;
use ignore_result::Ignore;
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

use super::deployer::RangeTabletDeployer;
use super::store::TabletStore;
use super::types::{StreamingChannel, TabletRequest, TabletServiceRequest};
use crate::clock::Clock;
use crate::cluster::{ClusterEnv, NodeId};
use crate::endpoint::{ResourceUri, ServiceUri};
use crate::log::LogPosition;
use crate::protos::{
    self,
    BatchError,
    BatchRequest,
    BatchResponse,
    DataMessage,
    DataOperation,
    HasTxnMeta,
    ManifestMessage,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    ShardDescriptor,
    ShardResponse,
    ShardSegment,
    TabletDeployment,
    TabletDescription,
    TabletDescriptor,
    TabletHeartbeatResponse,
    TabletId,
    Temporal,
};
use crate::tablet::file::CompactedFileWriter;
use crate::tablet::{
    BatchResult,
    FollowingTablet,
    LeadingTablet,
    LogMessageConsumer,
    ReplicationStage,
    ReplicationTracker,
    Request,
    TabletClient,
    TabletLoader,
};
use crate::utils::{self, DropOwner};

struct BatchResponser {
    temporal: Temporal,
    responses: Vec<ShardResponse>,
    responser: oneshot::Sender<Result<BatchResponse, BatchError>>,
}

impl BatchResponser {
    pub fn send(self) {
        let Self { temporal, responses, responser } = self;
        let response = BatchResponse { temporal, responses, deployments: Default::default() };
        responser.send(Ok(response)).ignore();
    }
}

struct WritingBatch {
    replication: ReplicationTracker,
    responser: Option<BatchResponser>,
    offset_notifier: Option<Arc<AsyncCell<LogPosition>>>,
}

struct PendingCompaction {
    files: Vec<String>,
}

impl PendingCompaction {
    pub fn files(&self) -> &[String] {
        &self.files
    }
}

#[derive(Default, Debug)]
struct CompletedCompaction {
    offset: LogPosition,
    files: VecDeque<String>,
}

impl CompletedCompaction {
    pub fn new(offset: LogPosition, files: VecDeque<String>) -> Self {
        Self { offset, files }
    }
}

#[derive(Clone)]
pub struct TabletServiceState {
    node: NodeId,
    clock: Clock,
    loader: TabletLoader,
    messager: mpsc::WeakSender<TabletServiceRequest>,
    cluster: ClusterEnv,
}

enum ServingTablet {
    Leader(LeadingTablet),
    Follower(FollowingTablet),
}

impl ServingTablet {
    pub fn shards(&self) -> (TabletId, Vec<ShardDescriptor>) {
        let (id, shards) = match self {
            ServingTablet::Leader(leader) => (leader.id(), leader.shards()),
            ServingTablet::Follower(follower) => (follower.id(), follower.shards()),
        };
        let shards = shards
            .iter()
            .map(|shard| ShardDescriptor { id: shard.id, range: shard.range.clone(), tablet_id: id.into() })
            .collect();
        (id, shards)
    }
}

trait Unify {
    fn unify(&self);
}

impl<T> Unify for T {
    fn unify(&self) {}
}

impl TabletServiceState {
    pub fn new(node: NodeId, cluster: ClusterEnv, messager: mpsc::WeakSender<TabletServiceRequest>) -> Self {
        let clock = cluster.clock().clone();
        let loader = TabletLoader::new(cluster.log().clone(), cluster.fs().clone());
        Self { node, clock, loader, messager, cluster }
    }

    fn create_pending_compaction(&self, tablet: &TabletDescription) -> PendingCompaction {
        let mut files = Vec::with_capacity(tablet.shards.len());
        for _ in 0..tablet.shards.len() {
            let uri = self.cluster.fs().resource_of(&uuid::Uuid::new_v4().to_string()).unwrap();
            files.push(uri.into_string())
        }
        PendingCompaction { files }
    }

    fn deploy(
        self: &Arc<TabletServiceState>,
        mut deployment: TabletDeployment,
        self_requester: mpsc::WeakUnboundedSender<TabletRequest>,
        mut requester: mpsc::UnboundedReceiver<TabletRequest>,
    ) {
        let state = self.clone();
        info!("deploying tablet: {:?}", deployment);
        tokio::spawn(async move {
            if let Err(err) = state.serve(&mut deployment, self_requester, &mut requester).await {
                warn!("tablet deployment {:?} quit: {:?}", deployment, err);
            }
            drop(requester);
            info!("unloading tablet: {:?}", deployment);
            if let Some(sender) = state.messager.upgrade() {
                sender.send(TabletServiceRequest::UnloadTablet { deployment }).await.ignore();
            }
        });
    }

    async fn serve(
        &self,
        deployment: &mut TabletDeployment,
        self_requester: mpsc::WeakUnboundedSender<TabletRequest>,
        requester: &mut mpsc::UnboundedReceiver<TabletRequest>,
    ) -> Result<()> {
        info!("serving tablet: {:?}", deployment);
        let client = TabletClient::new(self.cluster.clone());
        let (_, descriptor) = client.get_tablet_descriptor(deployment.id.into()).await?;
        let mut tablet = match deployment.servers.iter().position(|s| *s == self.node.as_ref()) {
            None => return Ok(()),
            Some(0) => ServingTablet::Leader(self.load_leading(deployment, &descriptor).await?),
            Some(_) => ServingTablet::Follower(self.load_following(&descriptor).await?),
        };
        let (id, shards) = tablet.shards();
        if let Some(sender) = self.messager.upgrade() {
            sender.send(TabletServiceRequest::DeployedTablet { id, shards }).await.ignore();
        }
        loop {
            match tablet {
                ServingTablet::Leader(leader) => {
                    match self.lead(deployment, leader, self_requester.clone(), requester).await? {
                        None => return Ok(()),
                        Some(follower) => tablet = ServingTablet::Follower(follower),
                    }
                },
                ServingTablet::Follower(follower) => {
                    match self.follow(deployment, &descriptor, follower, requester).await? {
                        None => return Ok(()),
                        Some(leader) => tablet = ServingTablet::Leader(leader),
                    }
                },
            }
        }
    }

    async fn load_leading(
        &self,
        deployment: &TabletDeployment,
        descriptor: &TabletDescriptor,
    ) -> Result<LeadingTablet> {
        self.loader
            .fence_load_tablet(
                self.clock.clone(),
                TabletClient::new(self.cluster.clone()),
                deployment.epoch,
                descriptor.manifest_log.as_str(),
            )
            .await
    }

    async fn load_following(&self, descriptor: &TabletDescriptor) -> Result<FollowingTablet> {
        self.loader.load_tablet(descriptor.manifest_log.as_str()).await
    }

    async fn compact(
        &self,
        store: TabletStore,
        offset_waiter: Get<Arc<AsyncCell<LogPosition>>>,
        mut files: Vec<String>,
    ) -> Result<CompletedCompaction> {
        info!("compacting tablet({}, {:?}) to files {:?}", store.tablet_id(), store.shards(), files);
        let mut compacted_files = VecDeque::with_capacity(files.len());
        for shard in store.shards() {
            let file = files.remove(files.len() - 1);
            let uri = ResourceUri::parse_named("file uri", &file).unwrap();
            let writer = self.cluster.fs().create(&uri).await?;
            let mut writer = CompactedFileWriter::new(writer).await.unwrap();
            let mut serialized_key = Vec::with_capacity(128);
            let mut serialized_value = Vec::with_capacity(256);
            for txn in store.shard_txns(shard.id.into()) {
                serialized_key.clear();
                serialized_value.clear();
                write!(&mut serialized_key, "0_txns/{}", txn.id()).ignore();
                txn.encode(&mut serialized_value).unwrap();
                writer.append(&serialized_key, &serialized_value).await.unwrap();
            }
            for (key, intent) in store.shard_intents(shard.id.into()) {
                serialized_key.clear();
                serialized_value.clear();
                serialized_key.extend_from_slice(b"1_intents/");
                hex_simd::encode_append(key, &mut serialized_key, AsciiCase::Lower);
                intent.encode(&mut serialized_value).unwrap();
                writer.append(&serialized_key, &serialized_value).await.unwrap();
            }
            for (key, ts, value) in store.shard_keys(shard.id.into()) {
                serialized_key.clear();
                serialized_value.clear();
                serialized_key.extend_from_slice(b"2_values/");
                hex_simd::encode_append(key, &mut serialized_key, AsciiCase::Lower);
                write!(&mut serialized_key, "/{}", ts).ignore();
                value.encode(&mut serialized_value).unwrap();
                writer.append(&serialized_key, &serialized_value).await.unwrap();
            }
            writer.finish().await.unwrap();
            compacted_files.push_back(file);
        }
        debug!("wait for offset");
        let offset = offset_waiter.await;
        info!(
            "compacted tablet({}, {:?}) logs up to {} to files {:?}",
            store.tablet_id(),
            store.shards(),
            offset,
            compacted_files
        );
        Ok(CompletedCompaction::new(offset, compacted_files))
    }

    fn start_deployment(
        &self,
        tablet: &LeadingTablet,
        requester: mpsc::WeakUnboundedSender<TabletRequest>,
    ) -> Option<DropOwner> {
        let shard_id = tablet.deployment_shard_id()?;
        let requester = requester.upgrade()?;
        let (drop_owner, drop_watcher) = utils::drop_watcher();
        RangeTabletDeployer::start(tablet.id(), shard_id, self.cluster.clone(), requester, drop_watcher);
        Some(drop_owner)
    }

    async fn lead(
        &self,
        deployment: &mut TabletDeployment,
        mut tablet: LeadingTablet,
        self_requester: mpsc::WeakUnboundedSender<TabletRequest>,
        requester: &mut mpsc::UnboundedReceiver<TabletRequest>,
    ) -> Result<Option<FollowingTablet>> {
        let now = self.clock.now();
        let watermark_duration = Duration::from_secs(5);
        let leader_expiration = tablet.leader_expiration().max(now + watermark_duration * 2);
        tablet.rotate(tablet.leader_expiration(), leader_expiration).await?;
        self.clock.update(tablet.closed_timestamp());

        let _drop_owner = self.start_deployment(&tablet, self_requester);

        let mut pending_compaction = None;
        let mut ongoing_compaction = None;

        let mut manifest_messages = VecDeque::with_capacity(5);
        let mut writing_batches = VecDeque::with_capacity(128);

        let mut next_watermark = tokio::time::sleep(watermark_duration);
        let mut unblocking_requests = VecDeque::with_capacity(16);
        let mut closing = false;
        while !closing {
            while let Some(request) = unblocking_requests.pop_front() {
                let Some(result) = tablet.process_request(request)? else {
                    continue;
                };

                match result {
                    BatchResult::Read { temporal, responses, responser, mut blocker } => {
                        trace!("batch reads, response: {:?}", responses);
                        let request_ts = temporal.timestamp();
                        self.clock.update(request_ts);
                        match blocker.is_empty() {
                            true => responser
                                .send(Ok(BatchResponse { temporal, responses, deployments: Default::default() }))
                                .ignore(),
                            false => tokio::spawn(async move {
                                let stage = blocker.wait().await;
                                let result = if stage == ReplicationStage::Replicated {
                                    Ok(BatchResponse { temporal, responses, deployments: Default::default() })
                                } else {
                                    assert!(stage == ReplicationStage::Failed);
                                    Err(BatchError::with_message("replication failed".to_string()))
                                };
                                responser.send(result).ignore();
                            })
                            .unify(),
                        };
                        continue;
                    },
                    BatchResult::Write { temporal, responses, responser, writes, replication, requests } => {
                        trace!("batch writes: {:?}, response: {:?}", writes, responses);
                        let request_ts = temporal.timestamp();
                        self.clock.update(request_ts);
                        let message = DataMessage {
                            temporal: temporal.clone(),
                            operation: if writes.is_empty() {
                                None
                            } else {
                                Some(DataOperation::Batch(protos::Batch { writes }))
                            },
                            ..tablet.new_data_message()
                        };
                        tablet.store.producer.queue(&message)?;
                        writing_batches.push_back(WritingBatch {
                            replication,
                            responser: Some(BatchResponser { temporal, responses, responser }),
                            offset_notifier: None,
                        });
                        unblocking_requests.extend(requests.into_iter());
                    },
                    BatchResult::Error { error, responser } => responser.send(Err(error)).ignore(),
                };
            }

            select! {
                _ = unsafe { std::pin::Pin::new_unchecked(&mut next_watermark) } => {
                    let now = self.clock.now();
                    let closing_timestamp = tablet.closed_timestamp().max(now - watermark_duration).min(tablet.leader_expiration());
                    let leader_expiration = now + watermark_duration * 2;

                    tablet.update_watermark(closing_timestamp, leader_expiration);
                    let accumulated_messages = tablet.update_compaction_stat();
                    if accumulated_messages >= self.cluster.tablet_compaction_messages() && pending_compaction.is_none() && ongoing_compaction.is_none() {
                        let compaction = self.create_pending_compaction(tablet.description());
                        tablet.add_dirty_files(compaction.files());
                        pending_compaction = Some(compaction);
                    }
                    next_watermark = tokio::time::sleep(watermark_duration);
                    let message = tablet.new_manifest_message();
                    tablet.manifest.producer.queue(&message)?;
                    manifest_messages.push_back(message);
                },
                result = ongoing_compaction.as_mut().unwrap(), if ongoing_compaction.is_some() => {
                    ongoing_compaction = None;
                    match result {
                        Err(err) => {
                            warn!("fail to complete tablet compaction {}: {}", tablet.id(), err);
                        },
                        Ok(Err(err)) => {
                            info!("fail to compact tablet {}: {}", tablet.id(), err);
                        },
                        Ok(Ok(mut completed_compaction @ CompletedCompaction { .. })) => {
                            let mut manifest = tablet.new_cloned_manifest();
                            for shard in manifest.tablet.shards.iter_mut() {
                                while let Some(segment) = shard.segments.pop() {
                                    if !segment.file.is_empty() {
                                        manifest.obsoleted_files.push(segment.file);
                                    }
                                }
                                let compacted_file = completed_compaction.files.pop_front().unwrap();
                                shard.segments.push(ShardSegment {
                                    file: compacted_file,
                                    ..Default::default()
                                });
                            }
                            let uri = ServiceUri::parse_named("log uri", &manifest.tablet.data_log).unwrap();
                            let uri = uri.with_query("offset", &completed_compaction.offset).unwrap();
                            manifest.tablet.data_log = uri.into_string();
                            info!("tablet compacted: {:?}", manifest);
                            let message = tablet.with_manifest_message(manifest);
                            tablet.manifest.producer.queue(&message)?;
                            manifest_messages.push_back(message);
                        }
                    }
                },
                result = tablet.manifest.producer.wait() => {
                    let _offset = result?;
                    let message: ManifestMessage = manifest_messages.pop_front().unwrap();
                    let manifest = message.manifest.unwrap();
                    tablet.update_manifest(manifest);
                    if let Some(compaction) = pending_compaction.take() {
                        let store = tablet.store.snapshot();
                        let offset_waiter = match writing_batches.back_mut() {
                            Some(batch) => {
                                let offset_notifier = batch.offset_notifier.get_or_insert_with(AsyncCell::shared);
                                offset_notifier.get_shared()
                            }
                            None => {
                                let last_position = tablet.store.store.get_last_position().unwrap();
                                Get(AsyncCell::new_with(last_position).into_shared())
                            }
                        };
                        tablet.reset_compaction_stat();
                        ongoing_compaction = Some(asyncs::spawn({
                            let state = self.clone();
                            async move {
                                state.compact(store, offset_waiter, compaction.files().to_owned()).await
                            }
                        }));
                    }
                },
                result = tablet.store.producer.wait() => {
                    let offset = result?;
                    let mut batch = writing_batches.pop_front().unwrap();
                    if let Some(offset_notifier) = batch.offset_notifier.take() {
                        offset_notifier.set(offset.clone());
                    }
                    tablet.store.store.set_last_position(offset);
                    batch.replication.commit();
                    if let Some(responser) = batch.responser {
                        responser.send();
                    }
                },
                Some(ref mut txn) = tablet.store.store.updated_txns().recv() => {
                    trace!("update txn {:?}", txn);
                    let (replication, requests) = tablet.store.store.update_txn(txn);
                    trace!("unblock txn {}(epoch:{}, {:?}) requests {:?}", txn.id(), txn.epoch(), txn.status(), requests);
                    unblocking_requests.extend(requests.into_iter());
                    if let Some(replication) = replication {
                        self.clock.update(txn.commit_ts());
                        txn.write_set.clear();
                        let message = DataMessage {
                            temporal: Temporal::Transaction(std::mem::take(txn)),
                            ..tablet.new_data_message()
                        };
                        tablet.store.producer.queue(&message)?;
                        writing_batches.push_back(WritingBatch {
                            replication,
                            responser: None,
                            offset_notifier: None,
                        });
                    }
                },
                result = requester.recv() => {
                    let Some(request) = result else {
                        closing = true;
                        continue;
                    };
                    let (mut batch, responser) = match request {
                        TabletRequest::Batch { batch, responser } => (batch, responser),
                        TabletRequest::ParticipateTxn { request, channel } => {
                            tablet.store.store.participate_txn(request, channel);
                            continue;
                        },
                        TabletRequest::Deploy { epoch, generation, servers } => {
                            deployment.epoch = epoch;
                            deployment.generation = generation;
                            deployment.servers = servers;
                            match deployment.servers.iter().position(|x| *x == self.node.0) {
                                None => return Ok(None),
                                Some(i) => if i != 0 {
                                    return Err(anyhow!("unimplemented: relinquish leader"));
                                } else {
                                    continue;
                                },
                            }
                        },
                    };
                    trace!("batch request: {:?}", batch);
                    if batch.temporal == Temporal::default() {
                        batch.temporal = Temporal::from(self.clock.now());
                    }

                    unblocking_requests.push_back(Request::new(batch, responser));
                },
            }
        }
        Ok(None)
    }

    async fn follow(
        &self,
        deployment: &mut TabletDeployment,
        descriptor: &TabletDescriptor,
        mut tablet: FollowingTablet,
        requester: &mut mpsc::UnboundedReceiver<TabletRequest>,
    ) -> Result<Option<LeadingTablet>> {
        loop {
            select! {
                message = tablet.manifest.consumer.read_message() => {
                    let message = message?.1;
                    tablet.apply_manifest_message(message)?;
                },
                message = tablet.store.consumer.read_message() => {
                    let (position, message) = message?;
                    tablet.store.store.apply(message)?;
                    tablet.store.store.set_last_position(position);
                },
                request = requester.recv() => match request {
                    None => return Ok(None),
                    Some(request) => match request {
                        TabletRequest::Batch { batch, responser } => {
                            let result = tablet.query_batch(deployment, batch);
                            responser.send(result).ignore();
                        },
                        TabletRequest::ParticipateTxn { .. } => {},
                        TabletRequest::Deploy { epoch, generation, servers } => {
                            deployment.update(epoch, generation, servers);
                            match deployment.index(&self.node) {
                                None => return Ok(None),
                                Some(0) => {
                                    let tablet = self.loader.lead_tablet(
                                        self.clock.clone(),
                                        TabletClient::new(self.cluster.clone()),
                                        epoch,
                                        descriptor.manifest_log.as_str(),
                                        tablet
                                    ).await?;
                                    return Ok(Some(tablet));
                                },
                                Some(_) => {},
                            }
                        },
                    }
                },
            }
        }
    }
}

#[derive(Default)]
struct OrderedShards {
    shards: Vec<ShardDescriptor>,
    tablets: HashMap<TabletId, Vec<ShardDescriptor>>,
}

impl OrderedShards {
    pub fn locate(&self, key: &[u8]) -> Option<&ShardDescriptor> {
        if let Ok(i) = self.shards.binary_search_by(|shard| shard.range.compare(key)) {
            return Some(&self.shards[i]);
        }
        None
    }

    pub fn unload_tablet(&mut self, id: TabletId) {
        let Some(shards) = self.tablets.remove(&id) else {
            return;
        };
        shards.iter().for_each(|shard| self.remove_shard(shard));
    }

    pub fn install_tablet(&mut self, id: TabletId, shards: Vec<ShardDescriptor>) {
        match self.tablets.entry(id) {
            HashEntry::Occupied(mut entry) => {
                let exists = entry.get_mut();
                if exists == &shards {
                    return;
                }
                exists.clone_from(&shards);
            },
            HashEntry::Vacant(entry) => {
                entry.insert(shards.clone());
            },
        };
        self.extend(shards);
    }

    fn remove_shard(&mut self, shard: &ShardDescriptor) {
        let i = self.shards.partition_point(|x| x.range.end <= shard.range.start);
        while i < self.shards.len() && self.shards[i].range.is_intersect_with(&shard.range) {
            self.shards.remove(i);
        }
    }

    fn extend(&mut self, shards: Vec<ShardDescriptor>) {
        if self.shards.is_empty() {
            self.shards = shards;
            self.shards.sort_by(|a, b| a.range.start.cmp(&b.range.start));
            return;
        }
        for shard in shards.into_iter() {
            let i = self.shards.partition_point(|x| x.range.end <= shard.range.start);
            while i < self.shards.len() && self.shards[i].is_predecessor_of(&shard) {
                self.shards.remove(i);
            }
            self.shards.insert(i, shard);
        }
    }
}

pub(super) struct TabletServiceManager {
    state: Arc<TabletServiceState>,
    shards: OrderedShards,
    tablets: HashMap<TabletId, (TabletDeployment, mpsc::UnboundedSender<TabletRequest>)>,
}

impl TabletServiceManager {
    pub fn new(state: Arc<TabletServiceState>) -> Self {
        Self { state, shards: Default::default(), tablets: HashMap::with_capacity(128) }
    }

    fn participate_txn(
        &self,
        request: ParticipateTxnRequest,
        channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
        responser: oneshot::Sender<Result<()>>,
    ) {
        let Some(shard) = self.shards.locate(request.txn.key()) else {
            debug!("no shard to participate txn {} key: {:?}", request.txn.id(), request.txn.key());
            return;
        };
        trace!(
            "route to tablet {} to participate txn {} key: {:?}",
            shard.tablet_id,
            request.txn.id(),
            request.txn.key()
        );
        if let Some((_, requester)) = self.tablets.get(&shard.tablet_id) {
            let request = TabletRequest::ParticipateTxn { request, channel };
            if requester.send(request).is_err() {
                responser.send(Err(anyhow!("tablet {} closed", shard.tablet_id))).ignore();
                return;
            }
        } else {
            responser.send(Err(anyhow!("tablet {} not found", shard.tablet_id))).ignore();
            return;
        }
        responser.send(Ok(())).ignore();
    }

    fn apply_batch(&mut self, batch: BatchRequest, responser: oneshot::Sender<Result<BatchResponse, BatchError>>) {
        if let Some((_, requester)) = self.tablets.get(&batch.tablet_id) {
            let request = TabletRequest::Batch { batch, responser };
            if let Err(mpsc::error::SendError(TabletRequest::Batch { batch, responser })) = requester.send(request) {
                responser.send(Err(BatchError::with_message(format!("tablet {} closed", batch.tablet_id)))).ignore();
            }
        } else {
            responser.send(Err(BatchError::with_message(format!("tablet {} not found", batch.tablet_id)))).ignore();
        }
    }

    fn unload_tablet(&mut self, deployment: TabletDeployment) {
        self.shards.unload_tablet(deployment.id.into());
        self.tablets.remove(&deployment.id);
    }

    fn heartbeat_tablet(&mut self, tablet_id: TabletId, responser: oneshot::Sender<TabletHeartbeatResponse>) {
        let deployment = self.tablets.get(&tablet_id).map(|(deployment, _)| deployment.clone());
        responser.send(TabletHeartbeatResponse { deployment }).ignore();
    }

    fn deploy_tablet(&mut self, deployment: TabletDeployment) -> Result<Vec<TabletDeployment>> {
        match self.tablets.entry(deployment.id.into()) {
            HashEntry::Occupied(mut occupied) => match deployment.order(&occupied.get().0) {
                cmp::Ordering::Less => Err(anyhow!("regression deployment {:?} to {:?}", deployment, occupied.get().0)),
                cmp::Ordering::Equal => Ok(Default::default()),
                cmp::Ordering::Greater => {
                    let deployed = occupied.get_mut();
                    deployed
                        .1
                        .send(TabletRequest::Deploy {
                            epoch: deployment.epoch,
                            generation: deployment.generation,
                            servers: deployment.servers.clone(),
                        })
                        .ignore();
                    deployed.0 = deployment;
                    Ok(Default::default())
                },
            },
            HashEntry::Vacant(vacant) => {
                let (requester, receiver) = mpsc::unbounded_channel();
                let weak_requster = requester.downgrade();
                vacant.insert((deployment.clone(), requester));
                self.state.deploy(deployment, weak_requster, receiver);
                Ok(Default::default())
            },
        }
    }

    pub async fn serve(&mut self, mut requester: mpsc::Receiver<TabletServiceRequest>) {
        info!("serving node: {:?}", self.state.node);
        while let Some(request) = requester.recv().await {
            match request {
                TabletServiceRequest::DeployTablet { deployment, responser } => {
                    let result = self.deploy_tablet(deployment);
                    responser.send(result).ignore();
                },
                TabletServiceRequest::DeployedTablet { id, shards } => self.shards.install_tablet(id, shards),
                TabletServiceRequest::HeartbeatTablet { tablet_id, responser } => {
                    self.heartbeat_tablet(tablet_id, responser)
                },
                TabletServiceRequest::UnloadTablet { deployment } => self.unload_tablet(deployment),
                TabletServiceRequest::Batch { batch, responser } => self.apply_batch(batch, responser),
                TabletServiceRequest::ParticipateTxn { request, channel, responser } => {
                    self.participate_txn(request, channel, responser)
                },
            }
        }
        info!("node {} is down", self.state.node)
    }
}
