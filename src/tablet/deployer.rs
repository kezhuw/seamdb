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
use tokio::select;
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint as TonicEndpoint};

use crate::cluster::{NodeId, NodeRegistry};
use crate::endpoint::{Endpoint, OwnedEndpoint};
use crate::keys;
use crate::protos::{
    HeartbeatRequest,
    TabletDeployRequest,
    TabletDeployment,
    TabletListRequest,
    TabletRange,
    TabletServiceClient,
};
use crate::utils::WatchConsumer as _;

const HEARTBEAT_DURATION: Duration = Duration::from_secs(1);
const HEARTBEAT_EXPIRATION: Duration = HEARTBEAT_DURATION.saturating_mul(4);

#[async_trait]
impl TabletDeployer for Arc<dyn NodeRegistry> {
    fn nodes(&self) -> &dyn NodeRegistry {
        self.as_ref()
    }
}

#[async_trait]
impl TabletDeployer for dyn NodeRegistry {
    fn nodes(&self) -> &dyn NodeRegistry {
        self
    }
}

#[async_trait]
pub trait TabletDeployer {
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
