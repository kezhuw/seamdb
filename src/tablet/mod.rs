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

mod client;
mod deployer;
mod loader;
mod memory;
mod node;
mod service;
mod types;

pub use self::client::TabletClient;
pub use self::deployer::{TabletDeployServant, TabletDeployer};
pub use self::loader::{
    FollowingTablet,
    FollowingTabletManifest,
    FollowingTabletStore,
    LeadingTablet,
    LeadingTabletManifest,
    LeadingTabletStore,
    LogMessageConsumer,
    TabletLoader,
};
pub use self::node::TabletNode;
pub use self::service::TabletServiceImpl;
pub use self::types::{BatchResult, ReplicationStage, Temporal, Timestamp, TimestampedValue};
pub use crate::protos::{
    MessageId,
    TabletDeployment,
    TabletDepot,
    TabletDescription,
    TabletDescriptor,
    TabletManifest,
    TabletWatermark,
};
