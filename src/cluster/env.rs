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

use super::{ClusterDeploymentMonitor, ClusterDescriptorWatcher, NodeRegistry};
use crate::clock::Clock;
use crate::log::LogManager;
use crate::protos::{ClusterDescriptor, TabletDeployment};

#[derive(Clone)]
pub struct ClusterEnv {
    log: Arc<LogManager>,
    clock: Clock,
    nodes: Arc<dyn NodeRegistry>,
    replicas: usize,
    descriptor: Option<ClusterDescriptorWatcher>,
    deployment: Option<ClusterDeploymentMonitor>,
}

impl ClusterEnv {
    pub fn new(log: Arc<LogManager>, nodes: Arc<dyn NodeRegistry>) -> Self {
        Self { log, nodes, replicas: 3, clock: Clock::new(), descriptor: None, deployment: None }
    }

    pub fn with_replicas(self, replicas: usize) -> Self {
        Self { replicas: replicas.max(1), ..self }
    }

    pub fn with_descriptor(self, descriptor: ClusterDescriptorWatcher) -> Self {
        Self { descriptor: Some(descriptor), ..self }
    }

    pub fn with_deployment(self, deployment: ClusterDeploymentMonitor) -> Self {
        Self { deployment: Some(deployment), ..self }
    }

    #[inline]
    pub fn log(&self) -> &Arc<LogManager> {
        &self.log
    }

    #[inline]
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    #[inline]
    pub fn nodes(&self) -> &Arc<dyn NodeRegistry> {
        &self.nodes
    }

    #[inline]
    pub fn replicas(&self) -> usize {
        self.replicas
    }

    pub fn latest_descriptor(&self) -> Option<Arc<ClusterDescriptor>> {
        self.descriptor.as_ref().and_then(|d| d.latest())
    }

    pub fn latest_deployment(&self) -> Option<Arc<TabletDeployment>> {
        self.deployment.as_ref().and_then(|d| d.latest())
    }
}
