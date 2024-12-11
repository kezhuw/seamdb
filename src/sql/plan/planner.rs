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

use datafusion::common::DataFusionError;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::*;

struct PostgresPhsycialPlanner {}

#[async_trait::async_trait]
impl ExtensionPlanner for PostgresPhsycialPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(plan) = node.as_any().downcast_ref::<CreateTablePlan>() {
            let physical_plan = CreateTableExec::new(
                plan.table_ref.clone().resolve("", ""),
                plan.table_descriptor.clone(),
                plan.if_not_exists,
            );
            return Ok(Some(Arc::new(physical_plan)));
        }
        Ok(None)
    }
}

pub fn get_extension_planners() -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    vec![Arc::new(PostgresPhsycialPlanner {})]
}
