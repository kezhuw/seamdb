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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionMode,
    ExecutionPlan,
    Partitioning,
    PlanProperties,
};

use crate::sql::client::SqlClient;

#[derive(Debug)]
pub struct CreateDatabaseExec {
    name: String,
    if_not_exists: bool,
    schema: Arc<Schema>,
    properties: PlanProperties,
}

impl CreateDatabaseExec {
    pub fn new(name: String, if_not_exists: bool) -> Self {
        let schema = Arc::new(Schema::empty());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self { name, if_not_exists, schema, properties }
    }
}

impl DisplayAs for CreateDatabaseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CreateDatabase({})", self.name)
    }
}

impl ExecutionPlan for CreateDatabaseExec {
    fn name(&self) -> &str {
        "InsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execute(&self, _partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let client = context
            .session_config()
            .get_extension::<SqlClient>()
            .ok_or_else(|| DataFusionError::Execution("no sql client".to_string()))?;
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema.clone(), 1);
        let sender = builder.tx();
        let name = self.name.clone();
        let if_not_exists = self.if_not_exists;
        builder.spawn(async move {
            client.create_database(name, if_not_exists).await?;
            drop(sender);
            Ok(())
        });
        Ok(builder.build())
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
}
