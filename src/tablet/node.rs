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

use std::time::Duration;

use ignore_result::Ignore;
use scopeguard::defer;
use tokio::net::TcpListener;
use tonic::transport::server::{Server, TcpIncoming};

use crate::cluster::{ClusterEnv, NodeId, NodeLease};
use crate::protos::TabletServiceServer;
use crate::tablet::TabletServiceImpl;
use crate::utils::{self, DropOwner};

pub struct TabletNode {
    _drop_owner: DropOwner,
}

impl TabletNode {
    pub fn start(id: NodeId, listener: TcpListener, lease: Box<dyn NodeLease>, cluster: ClusterEnv) -> Self {
        let incoming = TcpIncoming::from_listener(listener, true, Some(Duration::from_millis(300))).unwrap();
        let service = TabletServiceImpl::new(id, cluster);
        let (_drop_owner, mut drop_watcher) = utils::drop_watcher();
        tokio::spawn(async move {
            defer! {
                drop(lease);
            }
            Server::builder()
                .add_service(TabletServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async move { drop_watcher.dropped().await })
                .await
                .ignore();
        });
        Self { _drop_owner }
    }
}
