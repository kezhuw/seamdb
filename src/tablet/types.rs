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

use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use futures::stream::Stream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::protos::{
    BatchRequest,
    BatchResponse,
    ParticipateTxnRequest,
    ParticipateTxnResponse,
    ShardDescriptor,
    TabletDeployment,
    TabletHeartbeatResponse,
    TabletId,
};
pub use crate::protos::{MessageId, TabletWatermark};

pub struct StreamingResponser<T> {
    sender: UnboundedSender<Result<T, tonic::Status>>,
}

impl<T> StreamingResponser<T> {
    pub fn new(sender: UnboundedSender<Result<T, tonic::Status>>) -> Self {
        Self { sender }
    }

    pub fn send(&self, v: T) -> bool {
        self.sender.send(Ok(v)).is_ok()
    }
}

pub struct StreamingRequester<T> {
    stream: tonic::Streaming<T>,
}

pub struct StreamingChannel<Request, Response> {
    stream: tonic::Streaming<Request>,
    sender: UnboundedSender<Result<Response, tonic::Status>>,
}

unsafe impl<Request, Response> Sync for StreamingChannel<Request, Response> {}
unsafe impl<T> Sync for StreamingRequester<T> {}

impl<Request, Response> StreamingChannel<Request, Response> {
    pub fn new(stream: tonic::Streaming<Request>, sender: UnboundedSender<Result<Response, tonic::Status>>) -> Self {
        Self { stream, sender }
    }

    pub fn into_splits(self) -> (StreamingRequester<Request>, StreamingResponser<Response>) {
        (StreamingRequester::new(self.stream), StreamingResponser::new(self.sender))
    }
}

impl<T> Stream for StreamingRequester<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut stream = unsafe { Pin::new_unchecked(&mut self.stream) };
        loop {
            return match stream.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(message)),
                Poll::Ready(Some(Err(_))) => continue,
            };
        }
    }
}

impl<T> StreamingRequester<T> {
    pub fn new(stream: tonic::Streaming<T>) -> Self {
        Self { stream }
    }

    pub async fn next(&mut self) -> Option<T> {
        match self.stream.message().await {
            Ok(message) => message,
            Err(_) => None,
        }
    }
}

pub enum TabletRequest {
    Batch {
        batch: BatchRequest,
        responser: oneshot::Sender<Result<BatchResponse>>,
    },
    Deploy {
        epoch: u64,
        generation: u64,
        servers: Vec<String>,
    },
    ParticipateTxn {
        request: ParticipateTxnRequest,
        channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
    },
}

pub enum TabletServiceRequest {
    DeployTablet {
        deployment: TabletDeployment,
        responser: oneshot::Sender<Result<Vec<TabletDeployment>>>,
    },
    DeployedTablet {
        id: TabletId,
        shards: Vec<ShardDescriptor>,
    },
    HeartbeatTablet {
        tablet_id: TabletId,
        responser: oneshot::Sender<TabletHeartbeatResponse>,
    },
    Batch {
        batch: BatchRequest,
        responser: oneshot::Sender<Result<BatchResponse>>,
    },
    UnloadTablet {
        deployment: TabletDeployment,
    },
    ParticipateTxn {
        request: ParticipateTxnRequest,
        channel: StreamingChannel<ParticipateTxnRequest, ParticipateTxnResponse>,
        responser: oneshot::Sender<Result<()>>,
    },
}
