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

use datafusion::catalog::TableProvider;
use datafusion::common::arrow::array::{
    Array,
    BinaryArray,
    BooleanArray,
    Float32Array,
    Float64Array,
    Int16Array,
    Int32Array,
    Int64Array,
    StringArray,
    UInt64Array,
};
use datafusion::common::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::arrow::record_batch::RecordBatch;
use datafusion::common::{plan_err, DataFusionError, Result as DFResult, SchemaExt};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties};
use futures::prelude::stream::StreamExt;
use ignore_result::Ignore;
use lazy_static::lazy_static;
use tokio::sync::mpsc;
use tracing::{instrument, trace};

use super::table::SqlTable;
use crate::protos::{ColumnTypeKind, ColumnValue};
use crate::sql::client::{Column, Row, SqlClient};

lazy_static! {
    pub static ref INSERT_COUNT_SCHEMA: Arc<Schema> =
        Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
}

#[derive(Debug)]
pub struct InsertExec {
    table: SqlTable,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl InsertExec {
    pub fn new(table: SqlTable, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(table.schema().clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { table, input, properties }
    }
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InsertExec(table: {}, input: ", self.table.name())?;
        self.input.fmt_as(t, f)?;
        write!(f, ")")
    }
}

impl ExecutionPlan for InsertExec {
    fn name(&self) -> &str {
        "InsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        if self.table.schema().logically_equivalent_names_and_types(&self.input.schema()).is_err() {
            return plan_err!("insert expect schema {:?}, but get {:?}", self.table.schema(), self.input.schema());
        }
        let client = context
            .session_config()
            .get_extension::<SqlClient>()
            .ok_or_else(|| DataFusionError::Execution("no sql client".to_string()))?;
        let stream = self.input.execute(partition, context)?;
        let mut builder = RecordBatchReceiverStreamBuilder::new(INSERT_COUNT_SCHEMA.clone(), 1);
        let sender = builder.tx();
        builder.spawn(insert_into_table(client, self.table.clone(), stream, sender));
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

async fn insert_into_table_internally(
    client: Arc<SqlClient>,
    table: SqlTable,
    mut stream: SendableRecordBatchStream,
) -> DFResult<RecordBatch> {
    let schema = stream.schema();
    let mut rows = vec![];
    while let Some(record) = stream.next().await {
        let record = record?;
        rows.extend(std::iter::repeat_n(Row::default(), record.num_rows()));
        for (i, field) in schema.fields.iter().enumerate() {
            let column_values = record.column(i);
            if column_values.null_count() == record.num_rows() {
                continue;
            }
            let column_desc = table.descriptor.find_column(field.name().as_str()).unwrap();
            match column_desc.type_kind {
                ColumnTypeKind::Boolean => {
                    let array = column_values.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for (i, value) in array.values().iter().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Boolean(value))
                        };
                        rows[i].add_column(column);
                    }
                },
                ColumnTypeKind::Int16 => {
                    let array = column_values.as_any().downcast_ref::<Int16Array>().unwrap();
                    for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Int16(value.into()))
                        };
                        rows[i].add_column(column);
                    }
                },
                ColumnTypeKind::Int32 => {
                    let array = column_values.as_any().downcast_ref::<Int32Array>().unwrap();
                    for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Int32(value))
                        };
                        rows[i].add_column(column);
                    }
                },
                ColumnTypeKind::Int64 => {
                    let array = column_values.as_any().downcast_ref::<Int64Array>().unwrap();
                    for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Int64(value))
                        };
                        rows[i].add_column(column);
                    }
                },
                ColumnTypeKind::Float32 => {
                    let array = column_values.as_any().downcast_ref::<Float32Array>().unwrap();
                    for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Float32(value.into()))
                        };
                        rows[i].add_column(column);
                    }
                },
                ColumnTypeKind::Float64 => {
                    let array = column_values.as_any().downcast_ref::<Float64Array>().unwrap();
                    for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Float64(value.into()))
                        };
                        rows[i].add_column(column);
                    }
                },
                ColumnTypeKind::String => {
                    let array = column_values.as_any().downcast_ref::<StringArray>().unwrap();
                    for (i, row) in rows.iter_mut().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::String(array.value(i).to_owned()))
                        };
                        row.add_column(column);
                    }
                },
                ColumnTypeKind::Bytes => {
                    let array = column_values.as_any().downcast_ref::<BinaryArray>().unwrap();
                    for (i, row) in rows.iter_mut().enumerate() {
                        let column = if array.is_null(i) {
                            Column::new_null(column_desc.id)
                        } else {
                            Column::with_value(column_desc.id, ColumnValue::Bytes(array.value(i).to_owned()))
                        };
                        row.add_column(column);
                    }
                },
            }
        }
    }
    for row in rows.iter_mut() {
        trace!("prefill row: {row:?}");
        client.prefill_row(&table.descriptor, row).await?;
    }
    client.insert_rows_once(&table.descriptor, rows.iter()).await?;
    let record =
        RecordBatch::try_new(INSERT_COUNT_SCHEMA.clone(), vec![Arc::new(UInt64Array::from(vec![rows.len() as u64]))])?;
    Ok(record)
}

#[instrument(skip_all, fields(table.name = table.descriptor.name))]
async fn insert_into_table(
    client: Arc<SqlClient>,
    table: SqlTable,
    stream: SendableRecordBatchStream,
    sender: mpsc::Sender<DFResult<RecordBatch>>,
) -> DFResult<()> {
    let result = insert_into_table_internally(client, table, stream).await;
    sender.send(result).await.ignore();
    Ok(())
}
