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
mod context;
mod descriptor;
mod error;
mod plan;
pub mod postgresql;
mod row;
mod shared;
mod traits;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::plan_datafusion_err;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream};
use datafusion::logical_expr::logical_plan::dml::InsertOp;
use datafusion::logical_expr::{CreateCatalog, DdlStatement, DmlStatement, DropTable, LogicalPlan, WriteOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::sql::{ResolvedTableReference, TableReference};

pub use self::client::SqlClient;
use self::context::SqlContext;
use self::descriptor::TableDescriptorFetcher;
pub use self::error::SqlError;
use self::plan::{CreateDatabaseExec, DropTableExec};
use self::postgresql::PostgresPlanner;
pub use self::row::Row;
use self::traits::*;
use crate::kv::{KvClient, KvSemantics};

pub struct PostgreSqlExecutor {
    client: Arc<dyn KvClient>,
    state: SessionState,
    planner: DefaultPhysicalPlanner,
}

#[async_trait::async_trait]
impl PlannerContext for PostgreSqlExecutor {
    fn state(&self) -> &SessionState {
        &self.state
    }

    async fn fetch_table_references(
        &self,
        table_references: Vec<ResolvedTableReference>,
    ) -> Result<HashMap<ResolvedTableReference, Arc<dyn TableProvider>>, SqlError> {
        let mut tables = HashMap::with_capacity(table_references.len());
        let client = SqlClient::new(self.client.clone(), KvSemantics::Snapshot);
        let mut fetcher = TableDescriptorFetcher::new(&client);
        for table_ref in table_references {
            let Some(table) = fetcher.get_table(&table_ref).await? else {
                continue;
            };
            tables.insert(table_ref, table);
        }
        Ok(tables)
    }
}

impl PostgreSqlExecutor {
    pub fn new(context: Arc<SqlContext>) -> Self {
        let mut config = SessionConfig::default();
        config.options_mut().catalog.create_default_catalog_and_schema = false;
        config.options_mut().catalog.default_catalog = context.current_catalog().unwrap().to_string();
        config.options_mut().catalog.information_schema = true;
        let client = Arc::new(context.client().clone());
        let mut state = SessionStateBuilder::new().with_default_features().with_config(config).build();
        let scalar_functions = postgresql::create_all_scalar_functions(&context);
        scalar_functions.into_iter().for_each(|f| {
            state.register_udf(f).unwrap();
        });
        let planner = DefaultPhysicalPlanner::with_extension_planners(plan::get_extension_planners());
        Self { client, state, planner }
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<SendableRecordBatchStream, SqlError> {
        let planner = PostgresPlanner::new(self);
        let (plan, tables) = planner.plan(sql).await?;
        self.execute_plan(plan, tables).await
    }

    async fn create_physical_plan(
        &self,
        plan: LogicalPlan,
        tables: &HashMap<ResolvedTableReference, Arc<dyn TableProvider>>,
    ) -> Result<Arc<dyn ExecutionPlan>, SqlError> {
        let execution_plan: Arc<dyn ExecutionPlan> = match plan {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_)) => {
                return Err(SqlError::unsupported("create external table"))
            },
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_)) => {
                return Err(SqlError::unexpected("ddl CreateTable"))
            },
            LogicalPlan::Ddl(DdlStatement::CreateView(_)) => return Err(SqlError::unimplemented("create view")),
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(_)) => {
                return Err(SqlError::unimplemented("create schema"))
            },
            LogicalPlan::Ddl(DdlStatement::CreateCatalog(CreateCatalog { catalog_name, if_not_exists, .. })) => {
                Arc::new(CreateDatabaseExec::new(catalog_name, if_not_exists))
            },
            LogicalPlan::Ddl(DdlStatement::CreateIndex(_)) => return Err(SqlError::unimplemented("create index")),
            LogicalPlan::Ddl(DdlStatement::DropTable(DropTable { name, if_exists, .. })) => {
                let name = TableReference::from(self.resolve_table_reference(name));
                Arc::new(DropTableExec::new(name, if_exists))
            },
            LogicalPlan::Ddl(DdlStatement::DropView(_)) => return Err(SqlError::unimplemented("drop view")),
            LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(_)) => return Err(SqlError::unimplemented("drop schema")),
            LogicalPlan::Ddl(DdlStatement::CreateFunction(_)) => {
                return Err(SqlError::unimplemented("create function"))
            },
            LogicalPlan::Ddl(DdlStatement::DropFunction(_)) => return Err(SqlError::unimplemented("drop function")),
            LogicalPlan::Dml(DmlStatement { table_name, op: WriteOp::Insert(InsertOp::Append), input, .. }) => {
                let table_name = self.resolve_table_reference(table_name);
                let table = tables.get(&table_name).ok_or_else(|| plan_datafusion_err!("no table {table_name}"))?;
                let input_exec = Box::pin(self.create_physical_plan(Arc::unwrap_or_clone(input), tables)).await?;
                table.insert_into(&self.state, input_exec, InsertOp::Append).await?
            },
            _ => {
                let optimized_plan = self.state.optimize(&plan)?;
                self.planner.create_physical_plan(&optimized_plan, &self.state).await?
            },
        };

        Ok(execution_plan)
    }

    async fn execute_plan(
        &self,
        plan: LogicalPlan,
        tables: HashMap<ResolvedTableReference, Arc<dyn TableProvider>>,
    ) -> Result<SendableRecordBatchStream, SqlError> {
        let execution_plan = self.create_physical_plan(plan, &tables).await?;
        let mut state = self.state.clone();
        let client = SqlClient::new(self.client.clone(), KvSemantics::Transactional);
        state.config_mut().set_extension(Arc::new(client.clone()));
        let stream = execution_plan.execute(0, state.task_ctx())?;
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::common::arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
    use futures::prelude::stream::StreamExt;
    use tokio::net::TcpListener;

    use super::PostgreSqlExecutor;
    use crate::cluster::tests::etcd_container;
    use crate::cluster::{ClusterEnv, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
    use crate::endpoint::Endpoint;
    use crate::log::{LogManager, MemoryLogFactory};
    use crate::protos::TableDescriptor;
    use crate::sql::context::SqlContext;
    use crate::tablet::{TabletClient, TabletNode};

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn query() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::from(MemoryLogFactory);
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(TableDescriptor::POSTGRESQL_DIALECT_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let executor = PostgreSqlExecutor::new(
            SqlContext::new_unconnected(client, Some("test1".to_string()), "user1".to_string()).into(),
        );
        let mut stream = executor.execute_sql("CREATE DATABASE test1").await.unwrap();
        while let Some(_record) = stream.next().await {}

        let mut stream = executor
            .execute_sql(
                r#"CREATE TABLE table1 (
            id serial PRIMARY KEY,
            count bigint,
            price real,
            description text
        );"#,
            )
            .await
            .unwrap();
        while let Some(_record) = stream.next().await {}

        let mut stream = executor
            .execute_sql(
                r#"INSERT INTO table1
                (count, price, description)
                VALUES
                (4, 15.6, NULL),
                (3, 7.8, 'NNNNNN'),
                (8, 3.4, 'a'),
                (8, 2.9, 'b');
                "#,
            )
            .await
            .unwrap();
        let record = stream.next().await.unwrap().unwrap();
        let column = record.column(0);
        let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.value(0), 4u64);

        let mut stream = executor
            .execute_sql("select id, count, description from table1 ORDER BY count DESC, id ASC;")
            .await
            .unwrap();

        let record = stream.next().await.unwrap().unwrap();
        let column_id = record.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let column_count = record.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let column_description = record.column(2).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(column_id.value(0), 3);
        assert_eq!(column_count.value(0), 8);
        assert_eq!(column_description.value(0), "a");

        assert_eq!(column_id.value(1), 4);
        assert_eq!(column_count.value(1), 8);
        assert_eq!(column_description.value(1), "b");

        assert_eq!(column_id.value(2), 1);
        assert_eq!(column_count.value(2), 4);
        assert!(column_description.is_null(2));

        assert_eq!(column_id.value(3), 2);
        assert_eq!(column_count.value(3), 3);
        assert_eq!(column_description.value(3), "NNNNNN");

        let mut stream = executor
            .execute_sql("select current_catalog, current_database() as database, current_schema(), inet_client_port()")
            .await
            .unwrap();
        let record = stream.next().await.unwrap().unwrap();
        let column_catalog = record.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let column_database = record.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let column_schema = record.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let column_client_port = record.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(column_catalog.value(0), "test1");
        assert_eq!(column_database.value(0), "test1");
        assert_eq!(column_schema.value(0), "public");
        assert_eq!(column_client_port.value(0), 0);
    }

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn unique_nulls_distinct() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::from(MemoryLogFactory);
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(TableDescriptor::POSTGRESQL_DIALECT_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let executor = PostgreSqlExecutor::new(
            SqlContext::new_unconnected(client, Some("test1".to_string()), "user1".to_string()).into(),
        );
        let mut stream = executor.execute_sql("CREATE DATABASE test1").await.unwrap();
        while let Some(_record) = stream.next().await {}

        let mut stream = executor
            .execute_sql(
                r#"CREATE TABLE table1 (
            id serial PRIMARY KEY,
            name text,
            description text,
            CONSTRAINT unique_name UNIQUE NULLS DISTINCT (name)
        );"#,
            )
            .await
            .unwrap();
        while let Some(_record) = stream.next().await {}

        let mut stream = executor
            .execute_sql(
                r#"INSERT INTO table1
                (name, description)
                VALUES
                (NULL, 'row1'),
                ('name2', 'row2'),
                (NULL, 'row3');
                "#,
            )
            .await
            .unwrap();
        let record = stream.next().await.unwrap().unwrap();
        let column = record.column(0);
        let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.value(0), 3u64);

        let mut stream =
            executor.execute_sql("select id, name, description from table1 ORDER BY id ASC;").await.unwrap();

        let record = stream.next().await.unwrap().unwrap();
        let column_id = record.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let column_name = record.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let column_description = record.column(2).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(column_id.value(0), 1);
        assert!(column_name.is_null(0));
        assert_eq!(column_description.value(0), "row1");

        assert_eq!(column_id.value(1), 2);
        assert_eq!(column_name.value(1), "name2");
        assert_eq!(column_description.value(1), "row2");

        assert_eq!(column_id.value(2), 3);
        assert!(column_name.is_null(2));
        assert_eq!(column_description.value(2), "row3");

        let mut stream = executor
            .execute_sql(
                r#"INSERT INTO table1
                (name, description)
                VALUES
                ('name2', 'row4');
                "#,
            )
            .await
            .unwrap();
        stream.next().await.unwrap().unwrap_err();
    }

    #[tokio::test]
    #[test_log::test]
    #[tracing_test::traced_test]
    async fn unique_nulls_not_distinct() {
        let etcd = etcd_container();
        let cluster_uri = etcd.uri().with_path("/team1/seamdb1").unwrap();

        let node_id = NodeId::new_random();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let endpoint = Endpoint::try_from(address.as_str()).unwrap();
        let (nodes, lease) =
            EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
        let log_manager = LogManager::from(MemoryLogFactory);
        let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
        let mut cluster_meta_handle =
            EtcdClusterMetaDaemon::start("seamdb1", cluster_uri.clone(), cluster_env.clone()).await.unwrap();
        let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
        let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
        let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
        let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
        let client = TabletClient::new(cluster_env).scope(TableDescriptor::POSTGRESQL_DIALECT_PREFIX);
        tokio::time::sleep(Duration::from_secs(20)).await;

        let executor = PostgreSqlExecutor::new(
            SqlContext::new_unconnected(client, Some("test1".to_string()), "user1".to_string()).into(),
        );
        let mut stream = executor.execute_sql("CREATE DATABASE test1").await.unwrap();
        while let Some(_record) = stream.next().await {}

        let mut stream = executor
            .execute_sql(
                r#"CREATE TABLE table1 (
            id serial PRIMARY KEY,
            name text,
            description text,
            CONSTRAINT unique_name UNIQUE NULLS NOT DISTINCT (name)
        );"#,
            )
            .await
            .unwrap();
        while let Some(_record) = stream.next().await {}

        let mut stream = executor
            .execute_sql(
                r#"INSERT INTO table1
                (name, description)
                VALUES
                (NULL, 'row1'),
                ('name2', 'row2'),
                (NULL, 'row3');
                "#,
            )
            .await
            .unwrap();
        stream.next().await.unwrap().unwrap_err();
    }
}
