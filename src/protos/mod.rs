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

//! Generated code for protobuf message and rpc.

#[rustfmt::skip]
mod generated;
pub use self::data_message::Operation as DataOperation;
pub use self::generated::*;
pub use self::tablet_service_client::TabletServiceClient;
pub use self::tablet_service_server::{TabletService, TabletServiceServer};
pub use crate::keys;

impl From<ClusterMeta> for TabletDeployment {
    fn from(meta: ClusterMeta) -> Self {
        let tablet = TabletDescriptor {
            id: 1,
            generation: 0,
            range: TabletRange { start: keys::ROOT_KEY_PREFIX.to_owned(), end: keys::RANGE_KEY_PREFIX.to_owned() },
            log: meta.log,
            merge_bounds: TabletMergeBounds::None,
        };
        Self { tablet, epoch: meta.epoch, generation: meta.generation, servers: meta.servers }
    }
}

impl From<&ClusterMeta> for TabletDeployment {
    fn from(meta: &ClusterMeta) -> Self {
        let tablet = TabletDescriptor {
            id: 1,
            generation: 0,
            range: TabletRange { start: keys::ROOT_KEY_PREFIX.to_owned(), end: keys::RANGE_KEY_PREFIX.to_owned() },
            log: meta.log.clone(),
            merge_bounds: TabletMergeBounds::None,
        };
        TabletDeployment { tablet, epoch: meta.epoch, generation: meta.generation, servers: meta.servers.clone() }
    }
}
