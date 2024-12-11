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

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tracing::instrument;

use super::types::{StreamingChannel, TabletServiceRequest};
use crate::cluster::{ClusterEnv, NodeId};
use crate::protos::{
    BatchRequest,
    BatchResponse,
    LocateRequest,
    LocateResponse,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    TabletDeployRequest,
    TabletDeployResponse,
    TabletHeartbeatRequest,
    TabletHeartbeatResponse,
    TabletService,
};
use crate::tablet::service::{TabletServiceManager, TabletServiceState};
use crate::tablet::TabletClient;

pub struct TabletServiceImpl {
    state: Arc<TabletServiceState>,
    client: TabletClient,
}

unsafe impl Send for TabletServiceImpl {}
unsafe impl Sync for TabletServiceImpl {}

impl TabletServiceImpl {
    pub fn new(node: NodeId, cluster: ClusterEnv) -> Self {
        let (requester, receiver) = mpsc::channel(5000);
        let state = Arc::new(TabletServiceState::new(node, cluster.clone(), requester));
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
        self.state.messager().send(request).await.map_err(|_| Status::unavailable("service shutdown"))?;
        receiver.await.map_err(|_| Status::unavailable("service shutdown"))
    }

    async fn request_result<T>(
        &self,
        request: TabletServiceRequest,
        receiver: oneshot::Receiver<Result<T>>,
    ) -> Result<T, Status> {
        self.request(request, receiver).await?.map_err(|e| Status::invalid_argument(e.to_string()))
    }

    async fn request_message_result<T, E: prost::Message + std::error::Error>(
        &self,
        request: TabletServiceRequest,
        receiver: oneshot::Receiver<Result<T, E>>,
    ) -> Result<T, Status> {
        match self.request(request, receiver).await? {
            Ok(response) => Ok(response),
            Err(err) => Err(Status::with_details(tonic::Code::Internal, err.to_string(), err.encode_to_vec().into())),
        }
    }
}

#[async_trait]
impl TabletService for TabletServiceImpl {
    type ParticipateTxnStream = UnboundedReceiverStream<Result<ParticipateTxnResponse, tonic::Status>>;

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

    async fn heartbeat_tablet(
        &self,
        request: Request<TabletHeartbeatRequest>,
    ) -> Result<Response<TabletHeartbeatResponse>, Status> {
        let (sender, receiver) = oneshot::channel();
        let tablet_id = request.into_inner().tablet_id.into();
        let response =
            self.request(TabletServiceRequest::HeartbeatTablet { tablet_id, responser: sender }, receiver).await?;
        Ok(Response::new(response))
    }

    async fn batch(&self, request: Request<BatchRequest>) -> Result<Response<BatchResponse>, Status> {
        let (sender, receiver) = oneshot::channel();
        let response = self
            .request_message_result(
                TabletServiceRequest::Batch { batch: request.into_inner(), responser: sender },
                receiver,
            )
            .await?;
        Ok(Response::new(response))
    }

    #[instrument(skip_all)]
    async fn participate_txn(
        &self,
        request: Request<tonic::Streaming<ParticipateTxnRequest>>,
    ) -> Result<Response<Self::ParticipateTxnStream>, Status> {
        let mut streaming = request.into_inner();
        let (sender, response) = mpsc::unbounded_channel();
        let Some(request) = streaming.message().await? else {
            return Ok(Response::new(UnboundedReceiverStream::new(response)));
        };
        let channel = StreamingChannel::new(streaming, sender);
        let (request, receiver) = {
            let (sender, receiver) = oneshot::channel();
            let request = TabletServiceRequest::ParticipateTxn { request, channel, responser: sender };
            (request, receiver)
        };
        self.request_result(request, receiver).await?;
        Ok(Response::new(UnboundedReceiverStream::new(response)))
    }

    async fn locate(&self, request: Request<LocateRequest>) -> Result<Response<LocateResponse>, Status> {
        let query = request.into_inner();
        let deployment = self.client.locate(query.key).await?;
        let reply = LocateResponse { shard: deployment.shard().clone(), deployment: deployment.deployment().clone() };
        Ok(Response::new(reply))
    }
}
