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
use datafusion::common::arrow::array::array::StringArray;
use datafusion::common::arrow::datatypes::{self, Field, Schema};
use datafusion::common::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
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
use lazy_static::lazy_static;
use prost::Message;
use tokio::sync::mpsc;

use crate::sql::client::SqlClient;
use crate::sql::SqlError;

lazy_static! {
    pub static ref CREATE_TABLE_SCHEMA: Arc<Schema> =
        Arc::new(Schema::new([Arc::new(Field::new("result", datatypes::DataType::Utf8, false))]));
}

use crate::protos::TableDescriptor;
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[derive_where::derive_where(PartialOrd)]
pub struct CreateTablePlan {
    #[derive_where(skip(EqHashOrd))]
    pub schema: Arc<DFSchema>,
    pub table_ref: TableReference,
    pub table_descriptor: TableDescriptor,
    pub if_not_exists: bool,
}

impl CreateTablePlan {
    pub fn new(reference: TableReference, descriptor: TableDescriptor, if_not_exists: bool) -> Self {
        let schema = Arc::new(DFSchema::try_from(CREATE_TABLE_SCHEMA.clone()).unwrap());
        Self { table_ref: reference, table_descriptor: descriptor, if_not_exists, schema }
    }
}

impl From<CreateTablePlan> for LogicalPlan {
    fn from(plan: CreateTablePlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension { node: Arc::new(plan) })
    }
}

impl UserDefinedLogicalNodeCore for CreateTablePlan {
    fn name(&self) -> &str {
        "CreateTablePlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "CreateTable({})", self.table_ref)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self, DataFusionError> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return Err(DataFusionError::Internal("CreateTablePlan has no inputs and expressions".to_string()));
        }
        Ok(self.clone())
    }
}

#[derive(Debug)]
pub struct CreateTableExec {
    table_ref: ResolvedTableReference,
    descriptor: TableDescriptor,
    if_not_exists: bool,
    properties: PlanProperties,
}

impl CreateTableExec {
    pub fn new(table_ref: ResolvedTableReference, descriptor: TableDescriptor, if_not_exists: bool) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Schema::empty().into()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self { table_ref, descriptor, if_not_exists, properties }
    }
}

impl DisplayAs for CreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CreateTableExec(table: {})", self.table_ref)
    }
}

impl ExecutionPlan for CreateTableExec {
    fn name(&self) -> &str {
        "CreateTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let client = context
            .session_config()
            .get_extension::<SqlClient>()
            .ok_or_else(|| DataFusionError::Execution("no sql kv client".to_string()))?;
        let mut builder = RecordBatchReceiverStreamBuilder::new(CREATE_TABLE_SCHEMA.clone(), 1);
        let sender = builder.tx();
        builder.spawn(create_table(
            client,
            self.table_ref.clone(),
            self.descriptor.clone(),
            self.if_not_exists,
            sender,
        ));
        Ok(builder.build())
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }
}

async fn create_table_internally(
    client: Arc<SqlClient>,
    reference: ResolvedTableReference,
    descriptor: TableDescriptor,
    if_not_exists: bool,
) -> Result<RecordBatch, DataFusionError> {
    let database = match client.get_database(&reference.catalog).await? {
        None => return Err(SqlError::DatabaseNotExists(reference.catalog.to_string()).into()),
        Some(database) => database,
    };
    let schema = match client.get_schema(database.id, &reference.schema).await? {
        None => return Err(SqlError::SchemaNotExists(format!("{}.{}", reference.catalog, reference.schema)).into()),
        Some(schema) => schema,
    };
    if client.get_table(database.id, schema.id, &reference.table).await?.is_some() {
        if if_not_exists {
            let record = RecordBatch::try_new(CREATE_TABLE_SCHEMA.clone(), vec![Arc::new(StringArray::from(vec![
                "already exists",
            ]))])?;
            return Ok(record);
        }
        return Err(SqlError::TableAlreadyExists(reference.to_string()).into());
    };
    let blob = descriptor.encode_to_vec();
    let (created, _id, _ts) =
        client.create_descriptor("table", schema.id, reference.table.to_string(), blob, if_not_exists).await?;
    let record = match created {
        true => RecordBatch::try_new(CREATE_TABLE_SCHEMA.clone(), vec![Arc::new(StringArray::from(vec!["created"]))])?,
        false => RecordBatch::try_new(CREATE_TABLE_SCHEMA.clone(), vec![Arc::new(StringArray::from(vec![
            "already exists",
        ]))])?,
    };
    Ok(record)
}

async fn create_table(
    client: Arc<SqlClient>,
    reference: ResolvedTableReference,
    descriptor: TableDescriptor,
    if_not_exists: bool,
    sender: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
) -> Result<(), DataFusionError> {
    let result = create_table_internally(client, reference, descriptor, if_not_exists).await;
    sender.send(result).await.ignore();
    Ok(())
}
