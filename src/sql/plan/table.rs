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

use bytes::BufMut;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::arrow::array::builder::{
    BinaryBuilder,
    BooleanBuilder,
    Float32Builder,
    Float64Builder,
    Int16Builder,
    Int32Builder,
    Int64Builder,
    StringBuilder,
    StructBuilder,
};
use datafusion::common::arrow::datatypes::{self, Field, Fields, Schema, SchemaRef};
use datafusion::common::arrow::record_batch::RecordBatch;
use datafusion::common::{
    not_impl_err,
    plan_err,
    Constraint,
    Constraints,
    DFSchema,
    DataFusionError,
    Result as DFResult,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::logical_plan::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableSource, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{project_schema, Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::sql::ResolvedTableReference;
use tokio::sync::mpsc;

use super::insert::InsertExec;
use crate::kv::KvClient;
use crate::protos::{
    ColumnDescriptor,
    ColumnTypeKind,
    ColumnValue,
    IndexDescriptor,
    IndexKind,
    TableDescriptor,
    TimestampedKeyValue,
};
use crate::sql::client::SqlClient;
use crate::sql::error::SqlError;
use crate::sql::Row;

impl TryFrom<&datatypes::DataType> for ColumnTypeKind {
    type Error = DataFusionError;

    fn try_from(value: &datatypes::DataType) -> DFResult<ColumnTypeKind> {
        match value {
            datatypes::DataType::Int64 => Ok(ColumnTypeKind::Int64),
            datatypes::DataType::Binary => Ok(ColumnTypeKind::Bytes),
            datatypes::DataType::Utf8 => Ok(ColumnTypeKind::String),
            _ => plan_err!("unsupported data type {value}"),
        }
    }
}

impl From<ColumnTypeKind> for datatypes::DataType {
    fn from(kind: ColumnTypeKind) -> datatypes::DataType {
        match kind {
            ColumnTypeKind::Boolean => datatypes::DataType::Boolean,
            ColumnTypeKind::Int16 => datatypes::DataType::Int16,
            ColumnTypeKind::Int32 => datatypes::DataType::Int32,
            ColumnTypeKind::Int64 => datatypes::DataType::Int64,
            ColumnTypeKind::Float32 => datatypes::DataType::Float32,
            ColumnTypeKind::Float64 => datatypes::DataType::Float64,
            ColumnTypeKind::Bytes => datatypes::DataType::Binary,
            ColumnTypeKind::String => datatypes::DataType::Utf8,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqlTable {
    pub descriptor: TableDescriptor,
    schema: DFSchema,
    constraints: Constraints,
}

fn build_table_schema_and_constraints(descriptor: &TableDescriptor) -> (Schema, Constraints) {
    let fields: Fields = descriptor
        .columns
        .iter()
        .map(|column| Field::new(column.name.clone(), column.type_kind.into(), column.nullable))
        .collect();
    let schema = Schema::new(fields);
    let mut constraints = Vec::with_capacity(descriptor.indices.len());
    for index in descriptor.indices.iter() {
        let indices = match index.kind {
            IndexKind::NotUnique => {
                let primary_index = descriptor.primary_index();
                let unique_column_ids = index
                    .column_ids
                    .iter()
                    .copied()
                    .chain(primary_index.column_ids.iter().copied().filter(|id| !index.column_ids.contains(id)));
                descriptor.column_indices(unique_column_ids)
            },
            _ => descriptor.column_indices(index.column_ids.iter().copied()),
        };
        let constraint = if index.kind == IndexKind::PrimaryKey {
            Constraint::PrimaryKey(indices)
        } else {
            Constraint::Unique(indices)
        };
        constraints.push(constraint);
    }
    (schema, Constraints::new_unverified(constraints))
}

impl SqlTable {
    pub fn new(reference: ResolvedTableReference, descriptor: TableDescriptor) -> Self {
        let (schema, constraints) = build_table_schema_and_constraints(&descriptor);
        let schema = DFSchema::try_from(schema).unwrap().replace_qualifier(reference);
        Self { descriptor, schema, constraints }
    }

    pub fn name(&self) -> &str {
        &self.descriptor.name
    }
}

impl TableSource for SqlTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.inner().clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }
}

#[async_trait::async_trait]
impl TableProvider for SqlTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.inner().clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = project_schema(self.schema.inner(), projection)?;
        Ok(Arc::new(SqlTableScanExec::new(self.clone(), schema)))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if insert_op != InsertOp::Append {
            return not_impl_err!("INSERT INTO .. ON CONFLICT .. DO UPDATE SET ..");
        }
        let insert = InsertExec::new(self.clone(), input);
        Ok(Arc::new(insert))
    }
}

#[derive(Debug)]
struct SqlTableScanExec {
    table: SqlTable,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SqlTableScanExec {
    pub fn new(table: SqlTable, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { table, schema, properties }
    }
}

impl DisplayAs for SqlTableScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqlTableScanExec(table: {})", self.table.name())
    }
}

impl ExecutionPlan for SqlTableScanExec {
    fn name(&self) -> &str {
        "SqlTableScanExec"
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
            .ok_or_else(|| DataFusionError::Execution("no sql client".to_string()))?;
        let schema = self.schema.clone();
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 128);
        let table = self.table.clone();
        let sender = builder.tx();
        builder.spawn(scan_table(client, table, schema, sender));
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

struct ClusteredRowBuilder<'a> {
    table: &'a TableDescriptor,
    index: &'a IndexDescriptor,
    row: Row,
    columns: Vec<&'a ColumnDescriptor>,
    builder: RowBuilder,
}

struct RowBuilder {
    builder: StructBuilder,
}

impl RowBuilder {
    pub fn new(fields: Fields) -> Self {
        Self { builder: StructBuilder::from_fields(fields, 128) }
    }

    pub fn finish(&mut self) -> RecordBatch {
        let array = self.builder.finish();
        RecordBatch::from(array)
    }

    fn add_boolean_field(builder: &mut BooleanBuilder, value: Option<&ColumnValue>) {
        let value = value.map(|v| match v {
            ColumnValue::Boolean(v) => *v,
            _ => panic!("not int16: {v:?}"),
        });
        builder.append_option(value);
    }

    fn add_int16_field(builder: &mut Int16Builder, value: Option<&ColumnValue>) {
        let value = value.map(|v| match v {
            ColumnValue::Int16(int) => *int as i16,
            _ => panic!("not int16: {v:?}"),
        });
        builder.append_option(value);
    }

    fn add_int32_field(builder: &mut Int32Builder, value: Option<&ColumnValue>) {
        let value = value.map(|v| match v {
            ColumnValue::Int32(int) => *int,
            _ => panic!("not int: {v:?}"),
        });
        builder.append_option(value);
    }

    fn add_int64_field(builder: &mut Int64Builder, value: Option<&ColumnValue>) {
        let value = value.map(|v| match v {
            ColumnValue::Int64(int) => *int,
            _ => panic!("not int64: {v:?}"),
        });
        builder.append_option(value);
    }

    fn add_float32_field(builder: &mut Float32Builder, value: Option<&ColumnValue>) {
        let value = value.map(|v| match v {
            ColumnValue::Float32(v) => v.value,
            _ => panic!("not int: {v:?}"),
        });
        builder.append_option(value);
    }

    fn add_float64_field(builder: &mut Float64Builder, value: Option<&ColumnValue>) {
        let value = value.map(|v| match v {
            ColumnValue::Float64(v) => v.value,
            _ => panic!("not int64: {v:?}"),
        });
        builder.append_option(value);
    }

    fn add_binary_field(builder: &mut BinaryBuilder, value: Option<&ColumnValue>) {
        let bytes = value.map(|v| match v {
            ColumnValue::Bytes(bytes) => bytes.as_slice(),
            _ => panic!("not int: {v:?}"),
        });
        builder.append_option(bytes);
    }

    fn add_string_field(builder: &mut StringBuilder, value: Option<&ColumnValue>) {
        let bytes = value.map(|v| match v {
            ColumnValue::String(string) => string.as_str(),
            _ => panic!("not int: {v:?}"),
        });
        builder.append_option(bytes);
    }

    fn finish_one_row(&mut self) {
        self.builder.append(true);
    }

    fn add_field(&mut self, i: usize, type_kind: ColumnTypeKind, value: Option<&ColumnValue>) {
        match type_kind {
            ColumnTypeKind::Boolean => {
                let builder = self.builder.field_builder::<BooleanBuilder>(i).unwrap();
                Self::add_boolean_field(builder, value);
            },
            ColumnTypeKind::Int16 => {
                let builder = self.builder.field_builder::<Int16Builder>(i).unwrap();
                Self::add_int16_field(builder, value);
            },
            ColumnTypeKind::Int32 => {
                let builder = self.builder.field_builder::<Int32Builder>(i).unwrap();
                Self::add_int32_field(builder, value);
            },
            ColumnTypeKind::Int64 => {
                let builder = self.builder.field_builder::<Int64Builder>(i).unwrap();
                Self::add_int64_field(builder, value);
            },
            ColumnTypeKind::Float32 => {
                let builder = self.builder.field_builder::<Float32Builder>(i).unwrap();
                Self::add_float32_field(builder, value);
            },
            ColumnTypeKind::Float64 => {
                let builder = self.builder.field_builder::<Float64Builder>(i).unwrap();
                Self::add_float64_field(builder, value);
            },
            ColumnTypeKind::Bytes => {
                let builder = self.builder.field_builder::<BinaryBuilder>(i).unwrap();
                Self::add_binary_field(builder, value);
            },
            ColumnTypeKind::String => {
                let builder = self.builder.field_builder::<StringBuilder>(i).unwrap();
                Self::add_string_field(builder, value);
            },
        }
    }
}

impl<'a> ClusteredRowBuilder<'a> {
    pub fn new(fields: Fields, table: &'a TableDescriptor, index: &'a IndexDescriptor) -> Self {
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields.iter() {
            let column = table.find_column(field.name().as_str()).unwrap();
            columns.push(column);
        }
        Self { table, index, row: Default::default(), columns, builder: RowBuilder::new(fields) }
    }

    pub fn finish(&mut self) -> RecordBatch {
        self.builder.finish()
    }

    pub fn add_row(&mut self, row: TimestampedKeyValue) {
        self.row.clear();
        self.table.decode_key_columns_to(self.index, &row.key, self.row.columns_mut());
        let value_bytes = row.value.read_bytes(&row.key, "sql key value").unwrap();
        self.table.decode_value_columns_to(self.index, value_bytes, self.row.columns_mut());
        for (i, desc) in self.columns.iter().copied().enumerate() {
            let column = self.row.find_column(desc.id).unwrap();
            self.builder.add_field(i, desc.type_kind, column.value.as_ref());
        }
        self.builder.finish_one_row();
    }
}

async fn scan_table_internally(
    client: Arc<SqlClient>,
    table: SqlTable,
    schema: SchemaRef,
    sender: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
) -> Result<(), SqlError> {
    let primary_index = table.descriptor.primary_index();
    let mut start = table.descriptor.index_prefix(primary_index);
    let index_end = {
        let mut bytes = start.clone();
        bytes.put_u32(u32::MAX);
        bytes
    };
    let mut builder = ClusteredRowBuilder::new(schema.fields.clone(), &table.descriptor, primary_index);
    loop {
        let (resume_key, rows) = client.scan(start.into(), index_end.as_slice().into(), 0).await?;
        if !rows.is_empty() {
            for row in rows {
                builder.add_row(row);
            }
            let record_batch = builder.finish();
            if sender.send(Ok(record_batch)).await.is_err() {
                break;
            }
        }
        if resume_key.is_empty() || resume_key >= index_end {
            break;
        }
        start = resume_key;
    }
    Ok(())
}

async fn scan_table(
    client: Arc<SqlClient>,
    table: SqlTable,
    schema: SchemaRef,
    sender: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
) -> Result<(), DataFusionError> {
    scan_table_internally(client, table, schema, sender).await?;
    Ok(())
}
