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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use tracing::trace;

use super::error::SqlError;
use crate::kv::{KvClient, KvError, KvSemantics, LazyInitTimestampedKvClient, SharedKvClient};
use crate::protos::{
    ColumnDescriptor,
    ColumnTypeKind,
    ColumnValue,
    DatabaseDescriptor,
    DescriptorMeta,
    IndexDescriptor,
    KeyRange,
    NamespaceDescriptor,
    SchemaDescriptor,
    TableDescriptor,
    Timestamp,
    TimestampedKeyValue,
    Value,
};
use crate::tablet::TabletClient;
use crate::txn::LazyInitTxn;

#[derive(Clone)]
pub struct SqlClient {
    semantics: KvSemantics,
    client: SharedKvClient,
}

impl SqlClient {
    pub fn new(client: Arc<dyn KvClient>, semantics: KvSemantics) -> Self {
        match (semantics, client.semantics()) {
            (KvSemantics::Inconsistent, KvSemantics::Inconsistent)
            | (KvSemantics::Snapshot, KvSemantics::Snapshot)
            | (KvSemantics::Transactional, KvSemantics::Transactional) => Self { semantics, client: client.into() },
            (KvSemantics::Inconsistent, _) => Self { semantics, client: SharedKvClient::new(client.client().clone()) },
            (KvSemantics::Snapshot, _) => Self {
                semantics,
                client: SharedKvClient::new(LazyInitTimestampedKvClient::new(client.client().clone())),
            },
            (KvSemantics::Transactional, _) => {
                Self { semantics, client: SharedKvClient::new(LazyInitTxn::new(client.client().clone())) }
            },
        }
    }

    pub async fn insert_rows_once(
        &self,
        table: &TableDescriptor,
        rows: impl Iterator<Item = &Row>,
    ) -> Result<Timestamp, SqlError> {
        for row in rows {
            trace!("insert row: {row:?}");
            for index in table.indices.iter() {
                let (key, value) = row.index_kv(table, index);
                self.client.put(key, Some(Value::Bytes(value)), Some(Timestamp::default())).await?;
            }
        }
        Ok(self.client.commit().await?)
    }

    pub async fn delete_rows(
        &self,
        table: &TableDescriptor,
        rows: impl Iterator<Item = &Row>,
        expect_ts: Option<Timestamp>,
    ) -> Result<(), SqlError> {
        for row in rows {
            trace!("delete row: {row:?}");
            for index in table.indices.iter() {
                let key = row.index_key(table, index);
                self.client.put(&key, None, expect_ts).await?;
            }
        }
        Ok(())
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDescriptor>, SqlError> {
        self.get_descriptor(0, 0, name.to_string()).await
    }

    pub async fn get_schema(&self, database_id: u64, name: &str) -> Result<Option<SchemaDescriptor>, SqlError> {
        self.get_descriptor(0, database_id, name.to_string()).await
    }

    pub async fn get_table(
        &self,
        _database_id: u64,
        schema_id: u64,
        name: &str,
    ) -> Result<Option<TableDescriptor>, SqlError> {
        self.get_descriptor(0, schema_id, name.to_string()).await
    }

    // Table:
    //   id ==> descriptor
    //   index: name (unique)
    pub async fn create_database(&self, name: String, if_not_exists: bool) -> Result<DatabaseDescriptor, SqlError> {
        let table = DatabasesTable::new();
        let mut database_row = Row::default();
        database_row.add_column(Column::with_value(table.parent_id_column().id, ColumnValue::Int64(0)));
        database_row.add_column(Column::with_value(table.name_column().id, ColumnValue::String(name.clone())));
        let name_key = database_row.index_key(table.descriptor(), table.naming_index());
        if let Some((timestamp, value)) = self.get(name_key.as_slice().into()).await? {
            let id = decode_serial_primary_value(table.descriptor(), table.id_column(), &value)?;
            return Ok(DatabaseDescriptor { id, name, timestamp });
        }

        let mut schema_row = Row::default();
        schema_row.add_column(Column::with_value(table.name_column().id, ColumnValue::String("public".to_string())));
        let serial_id_key = table.serial_id_key();

        loop {
            if let Some((timestamp, value)) = self.get(name_key.as_slice().into()).await? {
                if !if_not_exists {
                    return Err(SqlError::DatabaseAlreadyExists(name));
                }
                let id = decode_serial_primary_value(table.descriptor(), table.id_column(), &value)?;
                return Ok(DatabaseDescriptor { id, name, timestamp });
            }
            self.fill_create_database_rows(&table, &serial_id_key, &mut database_row, &mut schema_row).await?;
            match self
                .insert_rows_once(
                    table.descriptor(),
                    std::iter::once(&database_row).chain(std::iter::once(&schema_row)),
                )
                .await
            {
                Ok(ts) => {
                    return Ok(DatabaseDescriptor {
                        id: database_row.serial_column_value(table.id_column()),
                        name,
                        timestamp: ts,
                    })
                },
                Err(SqlError::UniqueIndexAlreadyExists) => match if_not_exists {
                    true => self.restart()?,
                    false => return Err(SqlError::DatabaseAlreadyExists(name)),
                },
                Err(err) => match err.is_retriable() {
                    true => continue,
                    false => return Err(err),
                },
            }
        }
    }

    // FIXME: Background job to drop table content.
    // FIXME: DeleteRequest to batch delete without scanning.
    pub async fn drop_table(&self, table: &TableDescriptor) -> Result<bool, SqlError> {
        let databases = DatabasesTable::new();
        let mut row = Row::default();
        row.add_column(Column::with_value(databases.id_column().id, ColumnValue::Int64(table.id as i64)));
        row.add_column(Column::with_value(databases.parent_id_column().id, ColumnValue::Int64(table.schema_id as i64)));
        row.add_column(Column::with_value(databases.name_column().id, ColumnValue::String(table.name.clone())));
        self.delete_rows(databases.descriptor(), std::iter::once(&row), Some(table.timestamp)).await?;
        let mut start = table.table_prefix();
        let end = {
            let mut bytes = start.clone();
            bytes.put_u8(0xff);
            bytes
        };
        loop {
            let (resume_key, rows) = self.scan(start.into(), end.as_slice().into(), 0).await?;
            for row in rows {
                self.put(row.key.into(), None, None).await?;
            }
            if resume_key.is_empty() || resume_key >= end {
                break;
            }
            start = resume_key;
        }
        self.commit().await?;
        Ok(true)
    }

    // Table:
    //   id ==> descriptor
    //   index: name (unique)
    pub async fn create_descriptor(
        &self,
        kind: &str,
        parent_id: u64,
        name: String,
        blob: Vec<u8>,
        if_not_exists: bool,
    ) -> Result<(bool, u64, Timestamp), SqlError> {
        let table = DatabasesTable::new();
        let mut row = Row::default();
        row.add_column(Column::with_value(table.parent_id_column().id, ColumnValue::Int64(parent_id as i64)));
        row.add_column(Column::with_value(table.name_column().id, ColumnValue::String(name.clone())));
        row.add_column(Column::with_value(table.descriptor_column().id, ColumnValue::Bytes(blob)));
        let name_key = row.index_key(table.descriptor(), table.naming_index());
        if let Some((timestamp, value)) = self.get(name_key.as_slice().into()).await? {
            if if_not_exists {
                let id = decode_serial_primary_value(table.descriptor(), table.id_column(), &value)?;
                return Ok((false, id, timestamp));
            }
            return Err(SqlError::identity_already_exists(kind, name));
        }

        loop {
            if let Some((timestamp, value)) = self.get(name_key.as_slice().into()).await? {
                if !if_not_exists {
                    return Err(SqlError::DatabaseAlreadyExists(name));
                }
                let id = decode_serial_primary_value(table.descriptor(), table.id_column(), &value)?;
                return Ok((false, id, timestamp));
            }
            self.prefill_row(table.descriptor(), &mut row).await?;
            match self.insert_rows_once(table.descriptor(), std::iter::once(&row)).await {
                Ok(ts) => return Ok((true, row.serial_column_value(table.id_column()), ts)),
                Err(SqlError::UniqueIndexAlreadyExists) => match if_not_exists {
                    true => self.restart()?,
                    false => return Err(SqlError::DatabaseAlreadyExists(name)),
                },
                Err(err) => match err.is_retriable() {
                    true => continue,
                    false => {
                        return Err(err);
                    },
                },
            }
        }
    }

    pub async fn prefill_row(&self, table: &TableDescriptor, row: &mut Row) -> Result<(), SqlError> {
        for column_descriptor in table.columns.iter() {
            let Some(column) = row.find_column(column_descriptor.id) else {
                if let Some(default_value) = &column_descriptor.default_value {
                    row.add_column(Column::with_value(column_descriptor.id, default_value.clone()));
                    continue;
                } else if column_descriptor.nullable {
                    row.add_column(Column::new_null(column_descriptor.id));
                    continue;
                } else if column_descriptor.serial {
                    let key = table.serial_key(column_descriptor);
                    let incremented = self.increment(key.into(), 1).await?;
                    let value = match column_descriptor.type_kind {
                        ColumnTypeKind::Int16 => {
                            if incremented > i16::MAX as i64 {
                                return Err(SqlError::unexpected(format!(
                                    "column {} overflow",
                                    column_descriptor.name
                                )));
                            }
                            ColumnValue::Int16(incremented as i32)
                        },
                        ColumnTypeKind::Int32 => {
                            if incremented > i32::MAX as i64 {
                                return Err(SqlError::unexpected(format!(
                                    "column {} overflow",
                                    column_descriptor.name
                                )));
                            }
                            ColumnValue::Int32(incremented as i32)
                        },
                        ColumnTypeKind::Int64 => ColumnValue::Int64(incremented),
                        type_kind => {
                            return Err(SqlError::unexpected(format!(
                                "column {} has type {:?}, is not a serial column type",
                                column_descriptor.name, type_kind
                            )));
                        },
                    };
                    row.add_column(Column::with_value(column_descriptor.id, value));
                    continue;
                }
                return Err(SqlError::MissingColumn(column_descriptor.name.clone()));
            };
            column.check(table, column_descriptor)?;
        }
        Ok(())
    }

    async fn fill_create_database_rows(
        &self,
        table: &DatabasesTable,
        serial_id_key: &[u8],
        database_row: &mut Row,
        schema_row: &mut Row,
    ) -> Result<(), SqlError> {
        let incremented = self.increment(serial_id_key.into(), 2).await?;
        let database_id = incremented - 1;
        let schema_id = incremented;
        database_row.add_column(Column::with_value(table.id_column().id, ColumnValue::Int64(database_id)));
        database_row.add_column(Column::new_null(table.descriptor_column().id));
        schema_row.add_column(Column::with_value(table.id_column().id, ColumnValue::Int64(schema_id)));
        schema_row.add_column(Column::with_value(table.parent_id_column().id, ColumnValue::Int64(database_id)));
        schema_row.add_column(Column::new_null(table.descriptor_column().id));
        Ok(())
    }

    pub async fn get_descriptor<T: NamespaceDescriptor>(
        &self,
        database_id: u64,
        parent_id: u64,
        name: String,
    ) -> Result<Option<T>, SqlError> {
        let table = DatabasesTable::new();
        let mut row = Row::default();
        row.add_column(Column::with_value(table.parent_id_column().id, ColumnValue::Int64(parent_id as i64)));
        row.add_column(Column::with_value(table.name_column().id, ColumnValue::String(name)));
        let index = table.naming_index();

        let key = row.index_key(table.descriptor(), index);
        let Some((ts, value)) = self.get(key.as_slice().into()).await? else {
            return Ok(None);
        };
        let (id, name, blob) = table.decode_columns(index, TimestampedKeyValue { timestamp: ts, key, value });
        let meta = DescriptorMeta { id, name, database_id, parent_id, timestamp: ts, blob };
        Ok(Some(T::from_meta(meta)))
    }

    pub async fn list_descriptors<T: NamespaceDescriptor>(
        &self,
        database_id: u64,
        parent_id: u64,
    ) -> Result<Vec<T>, SqlError> {
        let table = DatabasesTable::new();
        let index = table.naming_index();
        let mut row = Row::default();
        row.add_column(Column::with_value(table.parent_id_column().id, ColumnValue::Int64(parent_id as i64)));
        let KeyRange { mut start, end } = row.index_range(table.descriptor(), index);
        let mut descriptors = Vec::new();
        loop {
            let (resume_key, rows) = self.scan(start.into(), end.as_slice().into(), 0).await?;
            for row in rows {
                let timestamp = row.timestamp;
                let (id, name, blob) = table.decode_columns(index, row);
                let meta = DescriptorMeta { id, name, database_id, parent_id, timestamp, blob };
                descriptors.push(T::from_meta(meta));
            }
            if resume_key.is_empty() || resume_key >= end {
                break;
            }
            start = resume_key;
        }
        Ok(descriptors)
    }
}

#[async_trait::async_trait]
impl KvClient for SqlClient {
    fn client(&self) -> &TabletClient {
        self.client.client()
    }

    fn semantics(&self) -> KvSemantics {
        self.semantics
    }

    async fn get(&self, key: Cow<'_, [u8]>) -> Result<Option<(Timestamp, Value)>, KvError> {
        self.client.get(key).await
    }

    async fn scan(
        &self,
        start: Cow<'_, [u8]>,
        end: Cow<'_, [u8]>,
        limit: u32,
    ) -> Result<(Vec<u8>, Vec<TimestampedKeyValue>), KvError> {
        self.client.scan(start, end, limit).await
    }

    async fn put(
        &self,
        key: Cow<'_, [u8]>,
        value: Option<Value>,
        expect_ts: Option<Timestamp>,
    ) -> Result<Timestamp, KvError> {
        self.client.put(key, value, expect_ts).await
    }

    async fn increment(&self, key: Cow<'_, [u8]>, increment: i64) -> Result<i64, KvError> {
        self.client.increment(key, increment).await
    }

    async fn commit(&self) -> Result<Timestamp, KvError> {
        self.client.commit().await
    }

    async fn abort(&self) -> Result<(), KvError> {
        self.client.abort().await
    }

    fn restart(&self) -> Result<(), KvError> {
        self.client.restart()
    }
}

// tenant (k,v)
//   meta
//   database
//   table
//
//   databases
//     rows:
//       id => name
//     indices:
//       name => id
//   tables
//
//  table/index/primary_keys ==> (version, key1, key2)

#[derive(Debug, Clone)]
pub struct Column {
    id: u32,
    value: Option<ColumnValue>,
}

impl Column {
    pub fn new(id: u32, value: Option<ColumnValue>) -> Self {
        Self { id, value }
    }

    pub fn with_value(id: u32, value: ColumnValue) -> Self {
        Self { id, value: Some(value) }
    }

    pub fn new_null(id: u32) -> Self {
        Self { id, value: None }
    }

    pub fn check(&self, table: &TableDescriptor, column: &ColumnDescriptor) -> Result<(), SqlError> {
        let Some(value) = &self.value else {
            if !column.nullable {
                return Err(SqlError::NotNullableColumn { table: table.name.clone(), column: column.name.clone() });
            }
            return Ok(());
        };
        let value_type = value.type_kind();
        if value_type != column.type_kind {
            return Err(SqlError::MismatchColumnType {
                table: table.name.clone(),
                column: column.name.clone(),
                expect: column.type_kind,
                actual: value_type,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Row {
    columns: HashMap<u32, Column>,
}

impl Row {
    pub fn serial_column_value(&self, descriptor: &ColumnDescriptor) -> u64 {
        let Some(column) = self.columns.get(&descriptor.id) else {
            panic!("row does not have column {}", descriptor.name)
        };
        match &column.value {
            Some(ColumnValue::Int16(i)) => *i as u64,
            Some(ColumnValue::Int32(i)) => *i as u64,
            Some(ColumnValue::Int64(i)) => *i as u64,
            _ => panic!("row does not have serial column {}, value {:?}", descriptor.name, column.value),
        }
    }

    pub fn find_column(&self, id: u32) -> Option<&Column> {
        self.columns.get(&id)
    }

    pub fn take_column(&mut self, id: u32) -> Option<ColumnValue> {
        self.columns.remove(&id).and_then(|c| c.value)
    }

    pub fn index_range(&self, table: &TableDescriptor, index: &IndexDescriptor) -> KeyRange {
        let mut key = table.index_prefix(index);
        for id in index.column_ids.iter().copied() {
            match self.find_column(id) {
                None => {
                    let mut end = key.clone();
                    key.put_i32(id as i32);
                    end.put_i32((id + 1) as i32);
                    return KeyRange::new(key, end);
                },
                Some(column) => {
                    key.put_i32(id as i32);
                    column.value.as_ref().unwrap().encode_as_key(&mut key);
                },
            };
        }
        let mut end = key.clone();
        key.put_u32(0);
        end.put_u32(1);
        KeyRange::new(key, end)
    }

    pub fn index_key(&self, table: &TableDescriptor, index: &IndexDescriptor) -> Vec<u8> {
        let mut key = table.index_prefix(index);
        for id in index.column_ids.iter().copied() {
            let column = self.find_column(id).unwrap();
            key.put_u32(column.id);
            match column.value.as_ref() {
                None => key.put_i32(-1),
                Some(value) => value.encode_as_key(&mut key),
            }
        }
        key.put_u32(0);
        key
    }

    pub fn index_kv(&self, table: &TableDescriptor, index: &IndexDescriptor) -> (Vec<u8>, Vec<u8>) {
        let key = self.index_key(table, index);
        let mut value_bytes = Vec::new();
        for id in index.storing_column_ids.iter().copied() {
            let column = self.find_column(id).unwrap_or_else(|| panic!("no column {id} for index {}", index.id));
            value_bytes.put_u32(column.id);
            match column.value.as_ref() {
                None => value_bytes.put_i32(-1),
                Some(value) => value.encode(&mut value_bytes),
            }
        }
        value_bytes.put_u32(0);
        (key, value_bytes)
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.insert(column.id, column);
    }
}

pub struct DatabasesTable {
    descriptor: TableDescriptor,
}

impl DatabasesTable {
    pub fn new() -> Self {
        Self { descriptor: databases_table_descriptor() }
    }

    pub fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }

    pub fn serial_id_key(&self) -> Vec<u8> {
        let id_column = self.id_column();
        self.descriptor.serial_key(id_column)
    }

    pub fn id_column(&self) -> &ColumnDescriptor {
        &self.descriptor.columns[0]
    }

    pub fn parent_id_column(&self) -> &ColumnDescriptor {
        &self.descriptor.columns[1]
    }

    pub fn name_column(&self) -> &ColumnDescriptor {
        &self.descriptor.columns[2]
    }

    pub fn descriptor_column(&self) -> &ColumnDescriptor {
        &self.descriptor.columns[3]
    }

    pub fn naming_index(&self) -> &IndexDescriptor {
        &self.descriptor.indices[1]
    }

    pub fn decode_columns(&self, index: &IndexDescriptor, kv: TimestampedKeyValue) -> (u64, String, Vec<u8>) {
        let mut row = Row::default();
        for (i, value) in self.descriptor.decode_index_key(index, &kv.key).into_iter().enumerate() {
            row.add_column(Column::new(index.column_ids[i], Some(value)));
        }
        let index_values =
            self.descriptor.decode_storing_columns(index, kv.value.read_bytes(&kv.key, "sql key value").unwrap());
        for (i, value) in index_values.into_iter().enumerate() {
            row.add_column(Column::new(index.storing_column_ids[i], value));
        }
        let id = row.take_column(self.id_column().id).unwrap().into_u64();
        let name = row.take_column(self.name_column().id).unwrap().into_string();
        let bytes = row.take_column(self.descriptor_column().id).map(|v| v.into_bytes()).unwrap_or(Vec::default());
        (id, name, bytes)
    }
}

fn databases_table_descriptor() -> TableDescriptor {
    TableDescriptor {
        id: 0,
        database_id: 1,
        schema_id: 2,
        timestamp: Timestamp::ZERO,
        name: "_databases".to_string(),
        columns: vec![
            ColumnDescriptor {
                id: 1,
                name: "id".to_string(),
                nullable: false,
                serial: true,
                type_kind: ColumnTypeKind::Int64,
                type_declaration: None,
                default_value: None,
            },
            ColumnDescriptor {
                id: 2,
                name: "parent_id".to_string(),
                nullable: false,
                serial: false,
                type_kind: ColumnTypeKind::Int64,
                type_declaration: None,
                default_value: None,
            },
            ColumnDescriptor {
                id: 3,
                name: "name".to_string(),
                nullable: false,
                serial: false,
                type_kind: ColumnTypeKind::String,
                type_declaration: None,
                default_value: None,
            },
            ColumnDescriptor {
                id: 4,
                name: "descriptor".to_string(),
                nullable: true,
                serial: false,
                type_kind: ColumnTypeKind::Bytes,
                type_declaration: None,
                default_value: None,
            },
        ],
        last_column_id: 4,
        indices: vec![
            IndexDescriptor {
                id: 1,
                name: "primary".to_string(),
                unique: true,
                column_ids: vec![1],
                storing_column_ids: vec![2, 3, 4],
            },
            IndexDescriptor {
                id: 2,
                name: "naming".to_string(),
                unique: true,
                column_ids: vec![2, 3],
                storing_column_ids: vec![1, 4],
            },
        ],
        last_index_id: 2,
    }
}

// FIXME:
fn decode_serial_primary_value(
    table: &TableDescriptor,
    column: &ColumnDescriptor,
    value: &Value,
) -> Result<u64, SqlError> {
    let Value::Bytes(bytes) = value else {
        panic!("column {}.{}expect bytes, get {:?}", table.name, column.name, value)
    };
    let mut bytes = bytes.as_slice();
    let column_id = bytes.get_u32();
    if column_id != column.id {
        panic!("expect column {}.{}(id:{}), but get {}", table.name, column.name, column.id, column_id)
    }
    match ColumnValue::decode(&mut bytes) {
        None => panic!("no column value"),
        Some(ColumnValue::Int16(i)) => Ok(i as u64),
        Some(ColumnValue::Int32(i)) => Ok(i as u64),
        Some(ColumnValue::Int64(i)) => Ok(i as u64),
        Some(value) => panic!("get value with type {:?}", value.type_kind()),
    }
}
