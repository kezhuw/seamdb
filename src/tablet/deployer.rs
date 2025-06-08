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
use futures::stream::{FuturesUnordered, StreamExt};
use ignore_result::Ignore;
use prost::Message as _;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::transport::{Channel, Endpoint as TonicEndpoint};
use tracing::{debug, instrument, span, trace, Instrument, Level};

use super::types::TabletRequest;
use crate::cluster::{ClusterEnv, NodeId, NodeRegistry};
use crate::endpoint::{Endpoint, OwnedEndpoint};
use crate::protos::{
    self,
    BatchRequest,
    DataRequest,
    FindRequest,
    FindResponse,
    PutRequest,
    ShardId,
    ShardRequest,
    TabletDeployRequest,
    TabletDeployment,
    TabletHeartbeatRequest,
    TabletId,
    TabletServiceClient,
    Temporal,
    Timestamp,
};
use crate::utils::{DropWatcher, WatchConsumer as _};

const HEARTBEAT_DURATION: Duration = Duration::from_secs(1);
const HEARTBEAT_EXPIRATION: Duration = HEARTBEAT_DURATION.saturating_mul(4);

#[async_trait]
pub trait TabletDeployer {
    type Version: Send + Sync;

    fn nodes(&self) -> &Arc<dyn NodeRegistry>;

    fn replicas(&self) -> usize;

    async fn put_deployment(
        &self,
        key: &[u8],
        version: Self::Version,
        deployment: &TabletDeployment,
    ) -> Result<Self::Version>;

    async fn publish_deployment(
        &self,
        key: &[u8],
        version: Self::Version,
        deployment: &TabletDeployment,
        channel: &watch::Sender<TabletDeployment>,
    ) -> Result<Self::Version> {
        let version = self.put_deployment(key, version, deployment).await?;
        channel.send(deployment.clone()).ignore();
        Ok(version)
    }

    async fn serve_deployment(
        &self,
        key: Vec<u8>,
        mut version: Self::Version,
        mut deployment: TabletDeployment,
    ) -> Result<()> {
        let tablet_id = deployment.id;
        let (crash_reporter, mut crash_watcher) = mpsc::unbounded_channel();
        let (deployment_sender, deployment_watcher) = watch::channel(deployment.clone());
        for node in deployment.servers.iter().cloned() {
            let node = NodeId(node);
            let nodes = self.nodes().clone();
            let deployment_receiver = deployment_watcher.clone();
            let crash_reporter = crash_reporter.clone();
            let deployment_span = span!(Level::INFO, "tablet deployment(recover)", %node, tablet_id);
            tokio::spawn(
                async move {
                    if let Err(err) = nodes.recover_deployment(&node, deployment_receiver).await {
                        tracing::info!("tablet deployment terminated: {}", err);
                    }
                    crash_reporter.send(node).ignore();
                }
                .instrument(deployment_span),
            );
        }
        let mut changed = false;
        let min_servers = self.replicas();
        let mut backoff = Duration::ZERO;
        loop {
            select! {
                biased;
                _ = tokio::time::sleep(backoff), if changed || deployment.servers.len() < min_servers => {
                    if deployment.servers.len() >= min_servers {
                        changed = false;
                        deployment.generation += 1;
                        version = self.publish_deployment(&key, version, &deployment, &deployment_sender).await?;
                        continue;
                    }
                    // TODO: the selected node could be the one just reported as crashed
                    let Some((node, addr)) = self.nodes().select_node() else {
                        continue;
                    };
                    if !deployment.servers.contains(&node.0) {
                        if deployment.servers.is_empty() {
                            deployment.epoch += 1;
                        } else {
                            deployment.generation += 1;
                        }
                        deployment.servers.push(node.0.clone());
                        version = self.publish_deployment(&key, version, &deployment, &deployment_sender).await?;
                        let nodes = self.nodes().clone();
                        let deployment_receiver = deployment_watcher.clone();
                        let crash_reporter = crash_reporter.clone();
                        let deployment_span = span!(Level::INFO, "cluster deployment", %node, %addr);
                        tokio::spawn(async move {
                            if let Err(err) = nodes.start_deployment(&node, addr, deployment_receiver).await {
                                tracing::info!("deployment terminated: {}", err);
                            }
                            crash_reporter.send(node).ignore();
                        }.instrument(deployment_span));
                        continue;
                    } else if changed {
                        deployment.generation += 1;
                        version = self.publish_deployment(&key, version, &deployment, &deployment_sender).await?;
                        continue;
                    }
                    backoff += backoff / 2 + Duration::from_secs(1);
                },
                Some(node) = crash_watcher.recv() => {
                    let Some(position) = deployment.servers.iter().position(|s| *s == node.0) else {
                        continue;
                    };
                    deployment.servers.remove(position);
                    if deployment.servers.is_empty() {
                        backoff = Duration::ZERO;
                    } else if position == 0 {
                        deployment.epoch += 1;
                        version = self.publish_deployment(&key, version, &deployment, &deployment_sender).await?;
                        continue;
                    }
                    changed = true;
                    backoff = backoff.min(Duration::from_secs(1));
                },
            }
        }
    }
}

#[async_trait]
impl TabletDeployServant for Arc<dyn NodeRegistry> {
    fn nodes(&self) -> &dyn NodeRegistry {
        self.as_ref()
    }
}

#[async_trait]
impl TabletDeployServant for dyn NodeRegistry {
    fn nodes(&self) -> &dyn NodeRegistry {
        self
    }
}

#[async_trait]
pub trait TabletDeployServant {
    fn nodes(&self) -> &dyn NodeRegistry;

    async fn connect_node(&self, node: &NodeId) -> Result<TabletServiceClient<Channel>> {
        let addr = self.nodes().get_endpoint(node).ok_or_else(|| anyhow!("node {node} not found, probably died"))?;
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
        mut deployed: TabletDeployment,
        mut receiver: watch::Receiver<TabletDeployment>,
        mut client: TabletServiceClient<Channel>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(HEARTBEAT_DURATION);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            select! {
                _ = receiver.changed() => {
                    let deployment = receiver.consume();
                    let is_member = deployment.servers.contains(&node.0);
                    let request = TabletDeployRequest { deployment: deployment.clone() };
                    client.deploy_tablet(request).await?;
                    if !is_member {
                        break;
                    }
                    deployed = deployment;
                },
                _ = interval.tick() => {
                    self.heartbeat_deployment(&mut client, node, &deployed).await?;
                },
            }
        }
        Ok(())
    }

    async fn heartbeat_deployment(
        &self,
        client: &mut TabletServiceClient<Channel>,
        node: &NodeId,
        deployment: &TabletDeployment,
    ) -> Result<TabletDeployment> {
        let request = TabletHeartbeatRequest { tablet_id: deployment.id };
        let response = client.heartbeat_tablet(request).await?.into_inner();
        let Some(deployed) = response.deployment else {
            bail!("node {} has no deployment for tablet {}", node, deployment.id)
        };
        ensure!(
            deployment.generation() >= deployed.generation(),
            "try to deploy tablet with epoch {} to node {} with deployment epoch {}",
            deployment.epoch,
            node,
            deployed.epoch
        );
        Ok(deployed)
    }

    async fn reestablish_deployment(
        &self,
        node: &NodeId,
        receiver: &mut watch::Receiver<TabletDeployment>,
    ) -> Result<(TabletServiceClient<Channel>, TabletDeployment)> {
        let mut client = self.connect_node(node).await?;
        let deployment = receiver.consume();
        let deployed = self.heartbeat_deployment(&mut client, node, &deployment).await?;
        if deployment.epoch != deployed.epoch || deployment.generation != deployed.generation {
            client.deploy_tablet(TabletDeployRequest { deployment: deployment.clone() }).await?;
        }
        Ok((client, deployment))
    }

    async fn start_deployment(
        &self,
        node: &NodeId,
        addr: OwnedEndpoint,
        mut receiver: watch::Receiver<TabletDeployment>,
    ) -> Result<()> {
        let mut client = self.connect_node_with_addr(node, addr.as_ref()).await?;
        let deployment = receiver.consume();
        client.deploy_tablet(TabletDeployRequest { deployment: deployment.clone() }).await?;
        self.serve_deployment(node, deployment, receiver, client).await
    }

    async fn recover_deployment(&self, node: &NodeId, mut receiver: watch::Receiver<TabletDeployment>) -> Result<()> {
        let (client, deployment) = self.reestablish_deployment(node, &mut receiver).await?;
        self.serve_deployment(node, deployment, receiver, client).await
    }
}

#[async_trait]
impl TabletDeployer for RangeTabletDeployer {
    type Version = Timestamp;

    fn nodes(&self) -> &Arc<dyn NodeRegistry> {
        self.cluster.nodes()
    }

    fn replicas(&self) -> usize {
        self.cluster.replicas()
    }

    async fn put_deployment(
        &self,
        key: &[u8],
        timestamp: Timestamp,
        deployment: &TabletDeployment,
    ) -> Result<Timestamp> {
        let put = PutRequest {
            key: key.to_owned(),
            value: Some(protos::Value::from_message(deployment)),
            sequence: 0,
            expect_ts: Some(timestamp),
        };
        let batch = BatchRequest {
            tablet_id: self.tablet_id.into(),
            uncertainty: None,
            temporal: Temporal::default(),
            requests: vec![ShardRequest { shard_id: self.shard_id.into(), request: DataRequest::Put(put) }],
        };
        let (sender, receiver) = oneshot::channel();
        let request = TabletRequest::Batch { batch, responser: sender };
        self.requester.send(request)?;
        let response = receiver.await??;
        let put = response.into_put().map_err(|r| anyhow!("expect put response, but got {:?}", r))?;
        Ok(put.write_ts)
    }
}

#[derive(Clone)]
pub struct RangeTabletDeployer {
    shard_id: ShardId,
    tablet_id: TabletId,
    requester: mpsc::UnboundedSender<TabletRequest>,
    cluster: ClusterEnv,
}

impl RangeTabletDeployer {
    async fn find_deployment(&self, key: Vec<u8>) -> Result<Option<(Vec<u8>, Timestamp, TabletDeployment)>> {
        let batch = BatchRequest {
            tablet_id: self.tablet_id.into(),
            uncertainty: None,
            temporal: Temporal::default(),
            requests: vec![ShardRequest {
                shard_id: self.shard_id.into(),
                request: DataRequest::Find(FindRequest { key, sequence: 0 }),
            }],
        };
        let (sender, receiver) = oneshot::channel();
        let request = TabletRequest::Batch { batch, responser: sender };
        self.requester.send(request)?;
        let response = receiver.await??;
        let find = response.into_find().map_err(|_| anyhow!(""))?;
        let FindResponse { key: located_key, value: Some(value) } = find else {
            return Ok(None);
        };
        let bytes = value.read_bytes(&located_key, "read deployment bytes")?;
        let deployment = TabletDeployment::decode(bytes)?;
        trace!("found deployment: key {:?}, ts {:?}, deployment {:?}", located_key, value.timestamp, deployment);
        Ok(Some((located_key, value.timestamp, deployment)))
    }

    async fn poll_deployments(
        &self,
        sender: &mpsc::Sender<Result<(Vec<u8>, Timestamp, TabletDeployment)>>,
    ) -> Result<()> {
        let Some((key, timestamp, deployment)) = self.find_deployment(vec![]).await? else {
            bail!("no deployments found")
        };
        sender.send(Ok((key.clone(), timestamp, deployment))).await.map_err(|_| anyhow!("deployer receiver closed"))?;
        let mut next_key = key;
        next_key.push(0);
        while let Some((key, timestamp, deployment)) = self.find_deployment(next_key).await? {
            sender
                .send(Ok((key.clone(), timestamp, deployment)))
                .await
                .map_err(|_| anyhow!("deployer receiver closed"))?;
            next_key = key;
            next_key.push(0);
        }
        Ok(())
    }

    fn load_deployments(&self) -> mpsc::Receiver<Result<(Vec<u8>, Timestamp, TabletDeployment)>> {
        let (sender, receiver) = mpsc::channel(100);
        let deployer = self.clone();
        tokio::spawn(async move {
            if let Err(err) = deployer.poll_deployments(&sender).await {
                sender.send(Err(err)).await.ignore();
            }
        });
        receiver
    }

    async fn serve_internally(&self) -> Result<()> {
        let mut deployments = self.load_deployments();
        let mut load_completed = false;
        let mut serve_futures = FuturesUnordered::new();
        while !(load_completed && serve_futures.is_empty()) {
            select! {
                r = deployments.recv(), if !load_completed => match r {
                    None => load_completed = true,
                    Some(Ok((key, timestamp, deployment))) => serve_futures.push(self.serve_deployment(key, timestamp, deployment)),
                    Some(Err(err)) => return Err(err),
                },
                Some(r) = serve_futures.next(), if !serve_futures.is_empty() => r?,
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(shard = %self.shard_id, tablet = %self.tablet_id))]
    async fn serve(&self) {
        if let Err(err) = self.serve_internally().await {
            debug!("fail to serve range tablet deployer: {err}");
        }
    }

    pub fn start(
        tablet_id: TabletId,
        shard_id: ShardId,
        cluster: ClusterEnv,
        requester: mpsc::UnboundedSender<TabletRequest>,
        mut drop_watcher: DropWatcher,
    ) {
        let deployer = RangeTabletDeployer { tablet_id, shard_id, requester, cluster };
        tokio::spawn(async move {
            select! {
                _ = drop_watcher.dropped() => {},
                _ = deployer.serve() => {},
            }
        });
    }
}
