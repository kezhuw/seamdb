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

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::catalog_common::ResolvedTableReference;
use datafusion::common::DataFusionError;
use datafusion::execution::session_state::SessionState;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::{visit_relations, Statement};
use datafusion::sql::TableReference;

use super::error::SqlError;
use crate::protos::{ColumnDescriptor, IndexDescriptor, TableDescriptor};

pub trait TableSchema {
    fn add_column(&mut self, column: ColumnDescriptor) -> &mut ColumnDescriptor;
    fn add_index(&mut self, index: IndexDescriptor);
}

impl TableSchema for TableDescriptor {
    fn add_column(&mut self, mut column: ColumnDescriptor) -> &mut ColumnDescriptor {
        column.id = self.last_column_id + 1;
        self.last_column_id = column.id;
        self.columns.push(column);
        self.columns.last_mut().unwrap()
    }

    fn add_index(&mut self, mut index: IndexDescriptor) {
        index.id = self.last_index_id + 1;
        if index.storing_column_ids.is_empty() {
            match self.indices.first() {
                None => {
                    assert!(index.unique);
                    // This is the primary index.
                    index.storing_column_ids.extend(
                        self.columns
                            .iter()
                            .map(|column| column.id)
                            .filter(|column_id| !index.column_ids.iter().copied().any(|id| id == *column_id)),
                    );
                },
                Some(primary_index) => index.storing_column_ids.extend_from_slice(&primary_index.column_ids),
            }
        }
        if index.name.is_empty() {
            index.name = match (self.indices.len(), index.unique) {
                (0, true) => format!("primary_index_{}", index.id),
                (_, true) => format!("unique_index_{}", index.id),
                (_, _) => format!("index_{}", index.id),
            };
        }
        self.last_index_id = index.id;
        self.indices.push(index);
    }
}

#[async_trait::async_trait]
pub trait PlannerContext {
    fn state(&self) -> &SessionState;

    fn collect_table_references(&self, statement: &Statement) -> Result<Vec<ResolvedTableReference>, DataFusionError> {
        let mut relations = HashSet::new();
        let _ = visit_relations(statement, |relation| {
            relations.insert(relation.clone());
            ControlFlow::<()>::Continue(())
        });
        relations
            .into_iter()
            .map(|x| object_name_to_table_reference(x, true).map(|x| self.resolve_table_reference(x)))
            .collect::<Result<_, DataFusionError>>()
    }

    fn resolve_table_reference(&self, name: TableReference) -> ResolvedTableReference {
        let default_catalog = &self.state().config_options().catalog.default_catalog;
        name.resolve(default_catalog, "public")
    }

    async fn fetch_table_references(
        &self,
        table_references: Vec<ResolvedTableReference>,
    ) -> Result<HashMap<TableReference, Arc<dyn TableProvider>>, SqlError> {
        let catalogs = self.state().catalog_list();
        let mut tables = HashMap::new();
        for table_ref in table_references {
            let Some(catalog) = catalogs.catalog(&table_ref.catalog) else {
                continue;
            };
            let Some(schema) = catalog.schema(&table_ref.schema) else {
                continue;
            };
            let Some(table) = schema.table(&table_ref.table).await? else {
                continue;
            };
            tables.insert(table_ref.into(), table);
        }
        Ok(tables)
    }
}

#[async_trait::async_trait]
impl PlannerContext for SessionState {
    fn state(&self) -> &SessionState {
        self
    }
}
