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

mod functions;

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::ops::ControlFlow;
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
    RecordBatch,
    StringArray,
    UInt64Array,
};
use datafusion::common::arrow::datatypes::{self, Schema};
use datafusion::common::{plan_datafusion_err, DataFusionError, Result as DFResult};
use datafusion::config::ConfigOptions;
use datafusion::datasource::default_table_source::DefaultTableSource;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::sqlparser::ast::{CharacterLength, ColumnDef, CreateTable, Statement, TableConstraint};
use datafusion::logical_expr::var_provider::{is_system_variables, VarType};
use datafusion::logical_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion::sql::planner::{object_name_to_table_reference, ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::ast::{ColumnOption, DataType};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::{ResolvedTableReference, TableReference};
use futures::prelude::stream::StreamExt;
use futures::{Sink, Stream};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type, METADATA_DATABASE, METADATA_USER};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::PgWireBackendMessage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

use super::error::SqlError;
use super::plan::*;
use super::shared::*;
use super::traits::*;
use crate::protos::{CharacterTypeDeclaration, ColumnTypeDeclaration, ColumnTypeKind};
use crate::sql::context::{SqlClientInfo, SqlContext};
use crate::sql::PostgreSqlExecutor;
use crate::tablet::TabletClient;

pub fn create_all_scalar_functions(context: &Arc<SqlContext>) -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(self::functions::CurrentCatalog::new(context.clone()))),
        Arc::new(ScalarUDF::new_from_impl(self::functions::CurrentUser::new(context.clone()))),
        Arc::new(ScalarUDF::new_from_impl(self::functions::InetClientPort::new(context.clone()))),
    ]
}

pub struct PostgresPlanner<'a> {
    context: &'a (dyn PlannerContext + Sync),
}

impl<'a> PostgresPlanner<'a> {
    pub fn new(context: &'a (dyn PlannerContext + Sync)) -> Self {
        Self { context }
    }

    pub async fn plan(
        &self,
        sql: &str,
    ) -> Result<(LogicalPlan, HashMap<ResolvedTableReference, Arc<dyn TableProvider>>), SqlError> {
        let dialect = PostgreSqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)?;
        let statement = match statements.len() {
            0 => return Err(SqlError::NoStatement),
            1 => statements.remove(0),
            _ => return Err(SqlError::MultipleStatements),
        };
        self.plan_statement(statement).await
    }

    async fn plan_statement(
        &self,
        statement: Statement,
    ) -> Result<(LogicalPlan, HashMap<ResolvedTableReference, Arc<dyn TableProvider>>), SqlError> {
        let resolved_table_references = self.context.collect_table_references(&statement)?;
        let tables = self.context.fetch_table_references(resolved_table_references).await?;
        let provider = PostgresContextProvider::new(self.context.state(), &tables);
        let planner = SqlToRel::new(&provider);
        let plan = match statement {
            Statement::CreateTable(CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                name,
                columns,
                constraints,
                ..
            }) => {
                if or_replace {
                    return Err(SqlError::unsupported("CREATE OR REPLACE TABLE .."));
                }
                if temporary || global.is_some() {
                    return Err(SqlError::unsupported("CREATE [ GLOBAL | LOCAL ] { TEMPORARY | TEMP } TABLE .."));
                }
                if external {
                    return Err(SqlError::unsupported("CREATE EXTERNAL TABLE .."));
                }
                let table_ref = self.context.resolve_table_reference(object_name_to_table_reference(name, true)?);
                if columns.is_empty() {
                    return Err(SqlError::invalid(format!("no columns in creating table {table_ref}")));
                }
                let mut table_descriptor_builder = TableDescriptorBuilder::new();
                for ColumnDef { name, data_type, options } in columns {
                    let column_name = name.value;
                    let (serial, type_kind, type_declaration) = match data_type {
                        DataType::Boolean | DataType::Bool => (false, ColumnTypeKind::Boolean, None),
                        DataType::Text => (false, ColumnTypeKind::String, None),
                        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) => {
                            (false, ColumnTypeKind::Int32, None)
                        },
                        DataType::SmallInt(_) | DataType::Int2(_) => (false, ColumnTypeKind::Int16, None),
                        DataType::BigInt(_) | DataType::Int8(_) => (false, ColumnTypeKind::Int64, None),
                        DataType::Varchar(n) | DataType::CharacterVarying(n) => match n {
                            None => (false, ColumnTypeKind::String, None),
                            Some(CharacterLength::IntegerLength { unit: Some(_), .. }) => {
                                return Err(SqlError::invalid("character length units"))
                            },
                            Some(CharacterLength::IntegerLength { length, .. }) => (
                                false,
                                ColumnTypeKind::String,
                                Some(ColumnTypeDeclaration::Character(CharacterTypeDeclaration {
                                    max_length: length as u32,
                                })),
                            ),
                            Some(CharacterLength::Max) => return Err(SqlError::invalid("varchar(MAX)")),
                        },
                        DataType::Real | DataType::Float4 => (false, ColumnTypeKind::Float32, None),
                        DataType::DoublePrecision | DataType::Float8 => (false, ColumnTypeKind::Float64, None),
                        DataType::Custom(name, _modifiers) => {
                            let type_name = name.to_string();
                            match type_name.as_str() {
                                "smallserial" | "serial2" => (true, ColumnTypeKind::Int16, None),
                                "serial" | "serial4" => (true, ColumnTypeKind::Int32, None),
                                "bigserial" | "serial8" => (true, ColumnTypeKind::Int64, None),
                                _ => return Err(SqlError::unsupported(format!("data type {type_name}"))),
                            }
                        },
                        DataType::Bytea => (false, ColumnTypeKind::Bytes, None),
                        _ => return Err(SqlError::unsupported(format!("data type {data_type}"))),
                    };

                    let mut column_builder =
                        table_descriptor_builder.add_column(column_name, type_kind, type_declaration)?;
                    for option in options {
                        match option.option {
                            ColumnOption::Null => column_builder.set_nullable(true),
                            ColumnOption::NotNull => column_builder.set_nullable(false),
                            ColumnOption::Default(_) => return Err(SqlError::unimplemented("DEFAULT expr")),
                            ColumnOption::Unique { is_primary, characteristics } => {
                                if characteristics.is_some() {
                                    return Err(SqlError::unsupported("CREATE TABLE .. [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]"));
                                }
                                column_builder
                                    .add_unique_constraint(is_primary, option.name.map(|ident| ident.value))?;
                            },
                            ColumnOption::Materialized(_) => {
                                return Err(SqlError::invalid("CREATE TABLE .. (column_name .. MATERIALIZE ..)"))
                            },
                            ColumnOption::Ephemeral(_) => {
                                return Err(SqlError::invalid("CREATE TABLE .. (column_name .. EPHEMERAL ..)"))
                            },
                            ColumnOption::Alias(_) => {
                                return Err(SqlError::invalid("CREATE TABLE .. (column_name .. ALIAS ..) .."))
                            },
                            ColumnOption::ForeignKey { .. } => {
                                return Err(SqlError::unsupported("CREATE TABLE .. FOREIGN KEY .."))
                            },
                            ColumnOption::Check(_) => {
                                return Err(SqlError::unsupported("CREATE TABLE .. (column_name .. CHECK ..) .."))
                            },
                            ColumnOption::OnUpdate(_) => {
                                return Err(SqlError::unsupported("CREATE TABLE .. (column_name .. ON UPDATE ..)"))
                            },
                            ColumnOption::DialectSpecific(_) => {},
                            ColumnOption::CharacterSet(_) => {},
                            ColumnOption::Comment(_) => {},
                            ColumnOption::Collation(_) => {
                                return Err(SqlError::unsupported(
                                    "CREATE TABLE table_name (column_name .. COLLATE ..",
                                ));
                            },
                            ColumnOption::Generated { .. } => {
                                return Err(SqlError::unsupported("CREATE TABLE .. (column_name .. GENERATED ..)"))
                            },
                            ColumnOption::Options(_) => {
                                return Err(SqlError::unsupported("CREATE TABLE .. (column_name .. OPTIONS(..) ..)"))
                            },
                            option => return Err(SqlError::unsupported(format!("option: {option}"))),
                        }
                    }
                    column_builder.set_serial(serial);
                }
                for constraint in constraints {
                    match constraint {
                        TableConstraint::Unique { name, index_name, columns, .. } => {
                            let index_name = index_name.or(name).map(|ident| ident.value);
                            let column_names = columns.into_iter().map(|ident| ident.value).collect();
                            table_descriptor_builder.add_unique_index(index_name, column_names)?;
                        },
                        TableConstraint::PrimaryKey { name, index_name, columns, .. } => {
                            let index_name = index_name.or(name).map(|ident| ident.value);
                            let column_names = columns.into_iter().map(|ident| ident.value).collect();
                            table_descriptor_builder.add_primary_index(index_name, column_names)?;
                        },
                        TableConstraint::ForeignKey { .. } => return Err(SqlError::unsupported("FOREIGN KEY")),
                        TableConstraint::Check { .. } => return Err(SqlError::unsupported("CHECK")),
                        TableConstraint::Index { name, columns, .. } => {
                            let index_name = name.map(|ident| ident.value);
                            table_descriptor_builder
                                .add_index(index_name, columns.into_iter().map(|ident| ident.value).collect())?;
                        },
                        TableConstraint::FulltextOrSpatial { .. } => {
                            return Err(SqlError::unsupported(
                                "{FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)",
                            ))
                        },
                    }
                }
                let table_descriptor = table_descriptor_builder.build()?;
                CreateTablePlan::new(table_ref.into(), table_descriptor, if_not_exists).into()
            },
            _ => planner.sql_statement_to_plan(statement)?,
        };
        Ok((plan, tables))
    }
}

struct PostgresContextProvider<'a> {
    state: &'a SessionState,
    tables: &'a HashMap<ResolvedTableReference, Arc<dyn TableProvider>>,
}

impl<'a> PostgresContextProvider<'a> {
    pub fn new(state: &'a SessionState, tables: &'a HashMap<ResolvedTableReference, Arc<dyn TableProvider>>) -> Self {
        Self { state, tables }
    }
}

impl ContextProvider for PostgresContextProvider<'_> {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>, DataFusionError> {
        let resolved = self.state.resolve_table_reference(name);
        let Some(table) = self.tables.get(&resolved) else {
            return Err(DataFusionError::Plan(format!("no table source for {}", resolved)));
        };
        Ok(Arc::new(DefaultTableSource::new(table.clone())))
    }

    fn get_table_function_source(&self, name: &str, args: Vec<Expr>) -> DFResult<Arc<dyn TableSource>> {
        let tbl_func = self
            .state
            .table_functions()
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table function '{name}' not found"))?;
        let provider = tbl_func.create_table_provider(&args)?;

        Ok(Arc::new(DefaultTableSource::new(provider)))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<datatypes::DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) { VarType::System } else { VarType::UserDefined };

        self.state
            .execution_props()
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

pub struct PostgresqlQueryProcessor {
    client: TabletClient,
}

impl From<SqlError> for PgWireError {
    fn from(err: SqlError) -> PgWireError {
        PgWireError::ApiError(Box::new(err))
    }
}

impl NoopStartupHandler for PostgresqlQueryProcessor {}

fn convert_schema(schema: &Schema) -> Vec<FieldInfo> {
    schema
        .fields
        .iter()
        .map(|field| {
            FieldInfo::new(field.name().clone(), None, None, convert_data_type(field.data_type()), FieldFormat::Text)
        })
        .collect()
}

async fn try_send(sender: &mpsc::Sender<PgWireResult<DataRow>>, result: PgWireResult<DataRow>) -> ControlFlow<()> {
    match sender.send(result).await {
        Ok(_) => ControlFlow::Continue(()),
        Err(_) => ControlFlow::Break(()),
    }
}

async fn send_record_batch(
    fields: &Arc<Vec<FieldInfo>>,
    sender: &mpsc::Sender<PgWireResult<DataRow>>,
    result: DFResult<RecordBatch>,
) -> ControlFlow<()> {
    let record = match result {
        Err(err) => return try_send(sender, Err(PgWireError::ApiError(Box::new(err)))).await,
        Ok(record) => record,
    };
    let mut rows = vec![];
    rows.extend(std::iter::repeat_with(|| DataRowEncoder::new(fields.clone())).take(record.num_rows()));
    let schema = record.schema();
    for (j, field) in schema.fields.iter().enumerate() {
        let array = record.column(j);
        match field.data_type() {
            datatypes::DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                for (i, value) in array.values().iter().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value as i64) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(array.value(i)) };
                    row.encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                for (i, row) in rows.iter_mut().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(array.value(i)) };
                    row.encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            datatypes::DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for (i, value) in array.values().as_ref().iter().copied().enumerate() {
                    let value = if array.is_null(i) { None } else { Some(value) };
                    rows[i].encode_field(&value).unwrap();
                }
            },
            data_type => panic!("unsupported data type {}", data_type),
        }
    }
    for row in rows {
        try_send(sender, row.finish()).await?;
    }
    ControlFlow::Continue(())
}

fn convert_record_batch_stream(
    fields: Arc<Vec<FieldInfo>>,
    mut stream: SendableRecordBatchStream,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let (sender, receiver) = mpsc::channel(10);
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            if send_record_batch(&fields, &sender, item).await.is_break() {
                break;
            }
        }
    });
    ReceiverStream::new(receiver)
}

fn convert_data_type(data_type: &datatypes::DataType) -> Type {
    use datatypes::DataType::*;
    match data_type {
        Boolean => Type::BOOL,
        Int16 | UInt16 => Type::INT2,
        Int32 | UInt32 => Type::INT4,
        Int64 | UInt64 => Type::INT8,
        Utf8 => Type::TEXT,
        Float32 => Type::FLOAT4,
        Float64 => Type::FLOAT8,
        _ => panic!("unsupported data type: {data_type}"),
    }
}

impl<T> SqlClientInfo for T
where
    T: ClientInfo,
{
    fn database(&self) -> Option<&str> {
        self.metadata().get(METADATA_DATABASE).map(|s| s.as_str())
    }

    fn user(&self) -> &str {
        self.metadata().get(METADATA_USER).unwrap().as_str()
    }

    fn socket_addr(&self) -> SocketAddr {
        self.socket_addr()
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for PostgresqlQueryProcessor {
    async fn do_query<'a, C>(&self, client: &mut C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>, {
        debug!("SQL: {query}");
        let context = SqlContext::new(self.client.clone(), client);
        let executor = PostgreSqlExecutor::new(context.into());
        let mut stream = executor.execute_sql(query).await?;
        let schema = stream.schema();
        if schema.fields.is_empty() {
            while let Some(_record) = stream.next().await {}
            return Ok(vec![Response::EmptyQuery]);
        }
        let fields = Arc::new(convert_schema(&schema));
        let stream = convert_record_batch_stream(fields.clone(), stream);
        let response = QueryResponse::new(fields, stream);
        Ok(vec![Response::Query(response)])
    }
}

#[derive(Clone)]
pub struct PostgresqlHandlerFactory {
    processor: Arc<PostgresqlQueryProcessor>,
}

impl PostgresqlHandlerFactory {
    pub fn new(client: TabletClient) -> Self {
        Self { processor: Arc::new(PostgresqlQueryProcessor { client }) }
    }
}

impl PgWireHandlerFactory for PostgresqlHandlerFactory {
    type CopyHandler = NoopCopyHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type SimpleQueryHandler = PostgresqlQueryProcessor;
    type StartupHandler = PostgresqlQueryProcessor;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.processor.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.processor.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
    use datafusion::logical_expr::LogicalPlan;

    use super::PostgresPlanner;
    use crate::protos::{CharacterTypeDeclaration, ColumnTypeDeclaration, ColumnTypeKind};
    use crate::sql::plan::CreateTablePlan;

    fn session_state() -> SessionState {
        let mut config = SessionConfig::default();
        config.options_mut().catalog.create_default_catalog_and_schema = false;
        config.options_mut().catalog.default_catalog = "database1".to_string();
        config.options_mut().catalog.information_schema = true;
        SessionStateBuilder::new().with_default_features().with_config(config).build()
    }

    #[asyncs::test]
    async fn create_table() {
        let state = session_state();
        let planner = PostgresPlanner::new(&state);
        let (plan, _tables) = planner
            .plan(
                r#"CREATE TABLE IF NOT EXISTS example (
                    id bigserial PRIMARY KEY,
                    name varchar(40) NOT NULL,
                    description varchar,
                    CONSTRAINT unique_name UNIQUE(name)
                );"#,
            )
            .await
            .unwrap();
        let LogicalPlan::Extension(extension) = plan else { panic!("expect create table plan, get {plan}") };
        let plan = extension.node.as_any().downcast_ref::<CreateTablePlan>().unwrap();
        assert!(plan.if_not_exists);
        assert_eq!(plan.table_ref.table(), "example");

        let descriptor = &plan.table_descriptor;
        assert_eq!(descriptor.columns[0].id, 1);
        assert_eq!(descriptor.columns[0].name, "id");
        assert!(descriptor.columns[0].serial);
        assert!(!descriptor.columns[0].nullable);
        assert_eq!(descriptor.columns[0].type_kind, ColumnTypeKind::Int64);

        assert_eq!(descriptor.columns[1].id, 2);
        assert_eq!(descriptor.columns[1].name, "name");
        assert!(!descriptor.columns[1].nullable);
        assert_eq!(descriptor.columns[1].type_kind, ColumnTypeKind::String);
        assert_eq!(
            descriptor.columns[1].type_declaration,
            Some(ColumnTypeDeclaration::Character(CharacterTypeDeclaration { max_length: 40 }))
        );

        assert_eq!(descriptor.columns[2].id, 3);
        assert_eq!(descriptor.columns[2].name, "description");
        assert!(descriptor.columns[2].nullable);
        assert_eq!(descriptor.columns[2].type_kind, ColumnTypeKind::String);
        assert_eq!(descriptor.columns[2].type_declaration, None);

        assert_eq!(descriptor.indices[0].id, 1);
        assert!(descriptor.indices[0].unique);
        assert_eq!(descriptor.indices[0].column_ids, vec![1]);
        assert_eq!(descriptor.indices[0].storing_column_ids, vec![2, 3]);

        assert_eq!(descriptor.indices[1].id, 2);
        assert!(descriptor.indices[1].unique);
        assert_eq!(descriptor.indices[1].column_ids, vec![2]);
        assert_eq!(descriptor.indices[1].storing_column_ids, vec![1]);
    }
}
