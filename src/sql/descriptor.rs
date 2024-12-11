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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::catalog_common::ResolvedTableReference;
use datafusion::sql::TableReference;

use super::client::SqlClient;
use super::error::SqlError;
use crate::protos::{DatabaseDescriptor, SchemaDescriptor};
use crate::sql::plan::SqlTable;

struct DatabaseMeta {
    #[allow(unused)]
    descriptor: DatabaseDescriptor,
    schemas: Vec<SchemaDescriptor>,
}

pub struct TableDescriptorFetcher<'a> {
    client: &'a SqlClient,
    databases: HashMap<String, DatabaseMeta>,
    tables: HashMap<TableReference, Arc<dyn TableProvider>>,
}

impl<'a> TableDescriptorFetcher<'a> {
    pub fn new(client: &'a SqlClient) -> Self {
        Self { client, databases: Default::default(), tables: Default::default() }
    }

    pub async fn get_table(
        &mut self,
        table_ref: &ResolvedTableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>, SqlError> {
        if let Some(table) = self.tables.get(&TableReference::from(table_ref.clone())) {
            return Ok(Some(table.clone()));
        };
        let database = match self.databases.get(table_ref.catalog.as_ref()) {
            Some(database) => database,
            None => {
                let Some(database_descriptor) =
                    self.client.get_descriptor::<DatabaseDescriptor>(0, 0, table_ref.catalog.to_string()).await?
                else {
                    return Ok(None);
                };
                let schema_descriptors =
                    self.client.list_descriptors(database_descriptor.id, database_descriptor.id).await?;
                self.databases.insert(table_ref.catalog.to_string(), DatabaseMeta {
                    descriptor: database_descriptor,
                    schemas: schema_descriptors,
                });
                self.databases.get(table_ref.catalog.as_ref()).unwrap()
            },
        };
        let Some(schema) = database.schemas.iter().find(|d| d.name == table_ref.schema.as_ref()) else {
            return Ok(None);
        };
        let Some(table_descriptor) =
            self.client.get_descriptor(schema.database_id, schema.id, table_ref.table.to_string()).await?
        else {
            return Ok(None);
        };
        let table = Arc::new(SqlTable::new(table_descriptor));
        self.tables.insert(table_ref.clone().into(), table.clone());
        Ok(Some(table))
    }
}
