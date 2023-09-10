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
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hashbrown::hash_map::HashMap;
use ignore_result::Ignore;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use super::deployer::RangeTabletDeployer;
use super::types::{TabletRequest, TabletServiceRequest};
use crate::clock::Clock;
use crate::cluster::{ClusterEnv, NodeId};
use crate::keys::{self, KeyKind};
use crate::protos::{
    self,
    BatchRequest,
    BatchResponse,
    DataMessage,
    DataOperation,
    HeartbeatRequest,
    HeartbeatResponse,
    LocateRequest,
    LocateResponse,
    ManifestMessage,
    TabletDeployRequest,
    TabletDeployResponse,
    TabletDeployment,
    TabletListRequest,
    TabletListResponse,
    TabletRange,
    TabletService,
};
use crate::tablet::{
    BatchResult,
    FollowingTablet,
    LeadingTablet,
    LogMessageConsumer,
    ReplicationStage,
    TabletClient,
    TabletLoader,
    Temporal,
};
use crate::utils;

pub struct TabletServiceState {
    node: NodeId,
    clock: Clock,
    loader: TabletLoader,
    messager: mpsc::Sender<TabletServiceRequest>,
    cluster: ClusterEnv,
}

enum ServingTablet {
    Leader(LeadingTablet),
    Follower(FollowingTablet),
}

impl TabletServiceState {
    fn deploy(
        self: &Arc<TabletServiceState>,
        mut deployment: TabletDeployment,
        self_requester: mpsc::WeakUnboundedSender<TabletRequest>,
        mut requester: mpsc::UnboundedReceiver<TabletRequest>,
    ) {
        let state = self.clone();
        tokio::spawn(async move {
            if let Err(err) = state.serve(&mut deployment, self_requester, &mut requester).await {
                tracing::warn!("tablet deployment {:?} quit: {:?}", deployment, err);
            }
            drop(requester);
            state.messager.send(TabletServiceRequest::UnloadTablet { deployment }).await.ignore();
        });
    }

    async fn serve(
        &self,
        deployment: &mut TabletDeployment,
        self_requester: mpsc::WeakUnboundedSender<TabletRequest>,
        requester: &mut mpsc::UnboundedReceiver<TabletRequest>,
    ) -> Result<()> {
        let mut tablet = match deployment.servers.iter().position(|s| *s == self.node.as_ref()) {
            None => return Ok(()),
            Some(0) => ServingTablet::Leader(self.load_leading(deployment).await?),
            Some(_) => ServingTablet::Follower(self.load_following(deployment).await?),
        };
        loop {
            match tablet {
                ServingTablet::Leader(leader) => {
                    match self.lead(deployment, leader, self_requester.clone(), requester).await? {
                        None => return Ok(()),
                        Some(follower) => tablet = ServingTablet::Follower(follower),
                    }
                },
                ServingTablet::Follower(follower) => match self.follow(deployment, follower, requester).await? {
                    None => return Ok(()),
                    Some(leader) => tablet = ServingTablet::Leader(leader),
                },
            }
        }
    }

    async fn load_leading(&self, deployment: &TabletDeployment) -> Result<LeadingTablet> {
        self.loader.fence_load_tablet(deployment.epoch, deployment.tablet.log.as_str()).await
    }

    async fn load_following(&self, deployment: &TabletDeployment) -> Result<FollowingTablet> {
        self.loader.load_tablet(deployment.tablet.log.as_str()).await
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
        let closed_timestamp = tablet.leader_expiration().max(now - watermark_duration);
        let leader_expiration = tablet.leader_expiration().max(now + watermark_duration * 2);
        tablet.manifest.manifest.rotate();
        tablet.publish_watermark(closed_timestamp, leader_expiration).await?;
        self.clock.update(closed_timestamp);

        let _drop_owner = if let (KeyKind::Range { .. }, _) = keys::identify_key(&deployment.tablet.range.start)? {
            if let Some(self_requester) = self_requester.upgrade() {
                let (drop_owner, drop_watcher) = utils::drop_watcher();
                RangeTabletDeployer::start(deployment.tablet.id, self.cluster.clone(), self_requester, drop_watcher);
                Some(drop_owner)
            } else {
                None
            }
        } else {
            None
        };

        let mut manifest_messages = VecDeque::with_capacity(5);
        let mut writing_batches = VecDeque::with_capacity(128);

        let mut next_watermark = tokio::time::sleep(watermark_duration);
        loop {
            select! {
                _ = unsafe { std::pin::Pin::new_unchecked(&mut next_watermark) } => {
                    let now = self.clock.now();
                    let closed_timestamp = tablet.closed_timestamp().max(now - watermark_duration).min(tablet.leader_expiration());
                    tablet.update_closed_timestamp(closed_timestamp);
                    let leader_expiration = now + watermark_duration * 2;
                    next_watermark = tokio::time::sleep(watermark_duration);
                    let message = tablet.new_expiration_message(leader_expiration);
                    tablet.manifest.producer.queue(&message)?;
                    manifest_messages.push_back(message);
                },
                result = tablet.manifest.producer.wait() => {
                    let _offset = result?;
                    let message: ManifestMessage = manifest_messages.pop_front().unwrap();
                    let manifest = message.manifest.unwrap();
                    tablet.update_manifest(manifest);
                },
                result = tablet.store.producer.wait() => {
                    let _offset = result?;
                    let (sequence, BatchResult { ts, responses, mut replication, .. }, responser): (_, _, oneshot::Sender<Result<BatchResponse>>) = writing_batches.pop_front().unwrap();
                    replication.commit();
                    let response = BatchResponse {
                        timestamp: Some(ts),
                        responses,
                        deployments: Default::default(),
                    };
                    responser.send(Ok(response)).ignore();
                    tablet.store.cursor.sequence = sequence;
                },
                Some(request) = requester.recv() => {
                    let (batch, responser) = match request {
                        TabletRequest::Batch { batch, responser } => (batch, responser),
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
                    tracing::trace!("batch request: {:?}", batch);
                    let mut temporal = match batch.temporal {
                        None => Temporal::from(self.clock.now()),
                        Some(temporal) => Temporal::try_from(temporal)?,
                    };
                    let mut result = match tablet.process_batch(&mut temporal, batch.requests) {
                        Err(err) => {
                            tracing::trace!("batch error: {:?}", err);
                            responser.send(Err(err)).ignore();
                            continue;
                        },
                        Ok(result) => result,
                    };

                    tracing::trace!("batch writes: {:?}, response: {:?}", result.writes, result.responses);

                    let request_ts = temporal.timestamp();
                    self.clock.update(request_ts);
                    tablet.update_closed_timestamp(request_ts);

                    let writes = result.take_writes();
                    if writes.is_empty() {
                        let BatchResult { ts, mut blocker, responses, .. } = result;
                        if blocker.is_empty() {
                            let response = BatchResponse {
                                timestamp: Some(ts),
                                responses,
                                deployments: Default::default(),
                            };
                            responser.send(Ok(response)).ignore();
                        } else {
                            tokio::spawn(async move {
                                let stage = blocker.wait().await;
                                let result = if stage == ReplicationStage::Replicated {
                                    Ok(BatchResponse {
                                        timestamp: Some(ts),
                                        responses,
                                        deployments: Default::default(),
                                    })
                                } else {
                                    assert!(stage == ReplicationStage::Failed);
                                    Err(anyhow!("replication failed"))
                                };
                                responser.send(result).ignore();
                            });
                        }
                        continue;
                    }
                    let mut message = DataMessage {
                        operation: Some(DataOperation::Batch(protos::Batch { writes })),
                        ..tablet.new_data_message()
                    };
                    tablet.store.producer.queue(&message)?;
                    result.writes = message.take_writes();
                    writing_batches.push_back((message.sequence, result, responser));
                },
            }
        }
    }

    async fn follow(
        &self,
        deployment: &mut TabletDeployment,
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
                    tablet.store.store.apply(message?.1)?;
                },
                request = requester.recv() => match request {
                    None => return Ok(None),
                    Some(request) => match request {
                        TabletRequest::Batch { batch, responser } => {
                            let result = tablet.query_batch(deployment, batch);
                            responser.send(result).ignore();
                        },
                        TabletRequest::Deploy { epoch, generation, servers } => {
                            deployment.update(epoch, generation, servers);
                            match deployment.index(&self.node) {
                                None => return Ok(None),
                                Some(0) => {
                                    return Ok(Some(self.loader.lead_tablet(epoch, deployment.tablet.log.as_str(), tablet).await?));
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

pub struct TabletServiceManager {
    state: Arc<TabletServiceState>,
    deployments: Vec<TabletDeployment>,
    tablets: HashMap<u64, mpsc::UnboundedSender<TabletRequest>>,
}

impl TabletServiceManager {
    fn new(state: Arc<TabletServiceState>) -> Self {
        Self { state, deployments: Vec::with_capacity(128), tablets: HashMap::with_capacity(128) }
    }

    fn list_tablets(&self, range: &TabletRange) -> Vec<TabletDeployment> {
        let i = self.deployments.partition_point(|x| x.tablet.range.end <= range.start);
        self.deployments[i..].iter().take_while(|x| x.tablet.range.start < range.end).cloned().collect()
    }

    fn deploy(&mut self, deployment: TabletDeployment) {
        let (requester, receiver) = mpsc::unbounded_channel();
        let weak_requster = requester.downgrade();
        let occupied = self.tablets.insert(deployment.tablet.id, requester);
        assert!(occupied.is_none());
        self.state.deploy(deployment, weak_requster, receiver);
    }

    fn apply_batch(&mut self, batch: BatchRequest, responser: oneshot::Sender<Result<BatchResponse>>) {
        if let Some(requester) = self.tablets.get(&batch.tablet_id) {
            let request = TabletRequest::Batch { batch, responser };
            if let Err(mpsc::error::SendError(TabletRequest::Batch { batch, responser })) = requester.send(request) {
                responser.send(Err(anyhow!("tablet {} closed", batch.tablet_id))).ignore();
            }
        } else {
            responser.send(Err(anyhow!("tablet {} not found", batch.tablet_id))).ignore();
        }
    }

    fn unload_tablet(&mut self, deployment: TabletDeployment) {
        let range = &deployment.tablet.range;
        let i = self.deployments.partition_point(|x| x.tablet.range.end <= range.start);
        if i == self.deployments.len() || range.end <= self.deployments[i].tablet.range.start {
            return;
        }
        let overlapping = &mut self.deployments[i];
        assert!(deployment.tablet.id == overlapping.tablet.id);
        self.deployments.remove(i);
        self.tablets.remove(&deployment.tablet.id);
    }

    fn heartbeat_tablet(&mut self, tablet_id: u64, responser: oneshot::Sender<HeartbeatResponse>) {
        if self.tablets.get(&tablet_id).is_some() {
            responser.send(Default::default()).ignore();
        }
    }

    fn deploy_tablet(&mut self, deployment: TabletDeployment) -> Result<Vec<TabletDeployment>> {
        let range = &deployment.tablet.range;
        let i = self.deployments.partition_point(|x| x.tablet.range.end <= range.start);
        if i == self.deployments.len() || range.end <= self.deployments[i].tablet.range.start {
            // New deployment.
            self.deployments.insert(i, deployment.clone());
            self.deploy(deployment);
            return Ok(Default::default());
        }
        let overlapping = &mut self.deployments[i];
        if deployment.tablet.id != overlapping.tablet.id {
            return Err(anyhow!("deployment {:?} is overlapping with {:?}", deployment, overlapping));
        }
        match deployment.order(overlapping) {
            cmp::Ordering::Less => Err(anyhow!("regression deployment {:?} to {:?}", deployment, overlapping)),
            cmp::Ordering::Equal => Ok(Default::default()),
            cmp::Ordering::Greater => {
                if let Some(requester) = self.tablets.get(&overlapping.tablet.id) {
                    requester
                        .send(TabletRequest::Deploy {
                            epoch: deployment.epoch,
                            generation: deployment.generation,
                            servers: deployment.servers.clone(),
                        })
                        .ignore();
                }
                if deployment.servers.iter().any(|x| x == self.state.node.as_ref()) {
                    overlapping.epoch = deployment.epoch;
                    overlapping.generation = deployment.generation;
                    overlapping.servers = deployment.servers;
                } else {
                    self.deployments.remove(i);
                    self.tablets.remove(&deployment.tablet.id);
                }
                Ok(Default::default())
            },
        }
    }

    async fn serve(&mut self, mut requester: mpsc::Receiver<TabletServiceRequest>) {
        while let Some(request) = requester.recv().await {
            match request {
                TabletServiceRequest::ListTablets { range, responser } => {
                    let deployments = self.list_tablets(&range);
                    responser.send(deployments).ignore();
                },
                TabletServiceRequest::DeployTablet { deployment, responser } => {
                    let result = self.deploy_tablet(deployment);
                    responser.send(result).ignore();
                },
                TabletServiceRequest::HeartbeatTablet { tablet_id, responser } => {
                    self.heartbeat_tablet(tablet_id, responser)
                },
                TabletServiceRequest::UnloadTablet { deployment } => self.unload_tablet(deployment),
                TabletServiceRequest::Batch { batch, responser } => self.apply_batch(batch, responser),
            }
        }
    }
}

pub struct TabletServiceImpl {
    state: Arc<TabletServiceState>,
    client: TabletClient,
}

unsafe impl Send for TabletServiceImpl {}
unsafe impl Sync for TabletServiceImpl {}

impl TabletServiceImpl {
    pub fn new(node: NodeId, cluster: ClusterEnv) -> Self {
        let (requester, receiver) = mpsc::channel(5000);
        let state = Arc::new(TabletServiceState {
            node,
            clock: cluster.clock().clone(),
            loader: TabletLoader::new(cluster.log().clone()),
            messager: requester,
            cluster: cluster.clone(),
        });
        tokio::spawn({
            let mut manager = TabletServiceManager::new(state.clone());
            async move {
                manager.serve(receiver).await;
            }
        });
        let client = TabletClient::new(cluster);
        Self { state, client }
    }

    async fn request<T>(&self, request: TabletServiceRequest, receiver: oneshot::Receiver<T>) -> Result<T, Status> {
        self.state.messager.send(request).await.map_err(|_| Status::unavailable("service shutdown"))?;
        receiver.await.map_err(|_| Status::unavailable("service shutdown"))
    }

    async fn request_result<T>(
        &self,
        request: TabletServiceRequest,
        receiver: oneshot::Receiver<Result<T>>,
    ) -> Result<T, Status> {
        self.request(request, receiver).await?.map_err(|e| Status::invalid_argument(e.to_string()))
    }
}

#[async_trait]
impl TabletService for TabletServiceImpl {
    async fn deploy_tablet(
        &self,
        request: Request<TabletDeployRequest>,
    ) -> Result<Response<TabletDeployResponse>, Status> {
        let deployment = request.into_inner().deployment;
        let (sender, receiver) = oneshot::channel();
        let deployments =
            self.request_result(TabletServiceRequest::DeployTablet { deployment, responser: sender }, receiver).await?;
        Ok(Response::new(TabletDeployResponse { deployments }))
    }

    async fn list_tablets(&self, request: Request<TabletListRequest>) -> Result<Response<TabletListResponse>, Status> {
        let range = request.into_inner().range;
        let (sender, responser) = oneshot::channel();
        let deployments =
            self.request(TabletServiceRequest::ListTablets { range, responser: sender }, responser).await?;
        Ok(Response::new(TabletListResponse { deployments }))
    }

    async fn batch(&self, request: Request<BatchRequest>) -> Result<Response<BatchResponse>, Status> {
        let (sender, receiver) = oneshot::channel();
        let response = self
            .request_result(TabletServiceRequest::Batch { batch: request.into_inner(), responser: sender }, receiver)
            .await?;
        Ok(Response::new(response))
    }

    async fn locate(&self, request: Request<LocateRequest>) -> Result<Response<LocateResponse>, Status> {
        let query = request.into_inner();
        let deployment = self.client.locate(query.key).await?;
        let reply = LocateResponse { deployment };
        Ok(Response::new(reply))
    }

    async fn heartbeat(&self, request: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        let (sender, receiver) = oneshot::channel();
        let tablet_id = request.into_inner().tablet_id;
        let response =
            self.request(TabletServiceRequest::HeartbeatTablet { tablet_id, responser: sender }, receiver).await?;
        Ok(Response::new(response))
    }
}
