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
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::catalog_common::ResolvedTableReference;
use datafusion::common::arrow::datatypes::Schema;
use datafusion::common::arrow::record_batch::RecordBatch;
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
use datafusion::sql::TableReference;
use ignore_result::Ignore;
use tokio::sync::mpsc;

use super::table::SqlTable;
use crate::sql::client::SqlClient;
use crate::sql::descriptor::TableDescriptorFetcher;
use crate::sql::SqlError;
#[derive(Debug)]
pub struct DropTableExec {
    name: TableReference,
    if_exists: bool,
    schema: Arc<Schema>,
    properties: PlanProperties,
}

impl DropTableExec {
    pub fn new(name: TableReference, if_exists: bool) -> Self {
        let schema = Arc::new(Schema::empty());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self { name, if_exists, schema, properties }
    }
}

impl DisplayAs for DropTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DropTable({})", self.name)
    }
}

impl ExecutionPlan for DropTableExec {
    fn name(&self) -> &str {
        "DropTableExec"
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
        let name = self.name.clone().resolve("", "");
        let if_exists = self.if_exists;
        builder.spawn(drop_table(client, name, if_exists, sender));
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

async fn drop_table_internally(
    client: Arc<SqlClient>,
    name: ResolvedTableReference,
    if_exists: bool,
) -> Result<bool, SqlError> {
    let mut fetcher = TableDescriptorFetcher::new(&client);
    let Some(table_provider) = fetcher.get_table(&name).await? else {
        if if_exists {
            return Ok(false);
        }
        return Err(SqlError::unexpected(format!("table {name} not exists")));
    };
    let table = table_provider.as_any().downcast_ref::<SqlTable>().unwrap();
    client.drop_table(&table.descriptor).await
}

async fn drop_table(
    client: Arc<SqlClient>,
    name: ResolvedTableReference,
    if_exists: bool,
    sender: mpsc::Sender<DFResult<RecordBatch>>,
) -> DFResult<()> {
    if let Err(err) = drop_table_internally(client, name, if_exists).await {
        sender.send(Err(err.into())).await.ignore();
    }
    Ok(())
}
