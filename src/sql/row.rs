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

use bytes::BufMut;

use crate::protos::{Column, ColumnDescriptor, ColumnValue, IndexDescriptor, IndexKind, KeyRange, TableDescriptor};

#[derive(Debug, Clone, Default)]
pub struct Row {
    columns: Vec<Column>,
}

impl Row {
    pub fn clear(&mut self) {
        self.columns.clear();
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    pub fn serial_column_value(&self, descriptor: &ColumnDescriptor) -> u64 {
        let Some(column) = self.find_column(descriptor.id) else {
            panic!("row does not have column {}", descriptor.name)
        };
        match column.value.as_ref() {
            Some(ColumnValue::Int16(i)) => *i as u64,
            Some(ColumnValue::Int32(i)) => *i as u64,
            Some(ColumnValue::Int64(i)) => *i as u64,
            _ => panic!("row does not have serial column {}, value {:?}", descriptor.name, column.value),
        }
    }

    pub fn find_column(&self, id: u32) -> Option<&Column> {
        self.columns.iter().find(|c| c.id == id)
    }

    pub fn take_column(&mut self, id: u32) -> Option<ColumnValue> {
        match self.columns.iter().position(|c| c.id == id) {
            None => None,
            Some(i) => {
                let column = self.columns.remove(i);
                column.value
            },
        }
    }

    pub fn has_null(&self) -> bool {
        self.columns.iter().any(Column::is_null)
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

    pub fn index_row_key(&self, table: &TableDescriptor, index: &IndexDescriptor) -> (Vec<u8>, bool) {
        let mut key = table.index_prefix(index);
        let mut has_null_key = false;
        for id in index.column_ids.iter().copied() {
            let column = self.find_column(id).unwrap();
            column.encode_as_key(&mut key);
            has_null_key = column.is_null();
        }
        let mut primary_keys_included = index.kind == IndexKind::PrimaryKey;
        if index.kind == IndexKind::NotUnique || (index.kind == IndexKind::UniqueNullsDistinct && has_null_key) {
            let primary_index = table.primary_index();
            for id in primary_index.column_ids.iter().copied().filter(|column_id| !index.column_ids.contains(column_id))
            {
                let column = self.find_column(id).unwrap();
                column.encode_as_key(&mut key);
            }
            primary_keys_included = true;
        }
        key.put_u32(0);
        (key, primary_keys_included)
    }

    pub fn index_key(&self, table: &TableDescriptor, index: &IndexDescriptor) -> Vec<u8> {
        self.index_row_key(table, index).0
    }

    pub fn index_kv(&self, table: &TableDescriptor, index: &IndexDescriptor) -> (Vec<u8>, Vec<u8>) {
        let (key, primary_keys_included) = self.index_row_key(table, index);
        let primary_index = table.primary_index();
        let mut value_bytes = Vec::new();
        for id in index.storing_column_ids.iter().copied().filter(|id| match primary_keys_included {
            false => true,
            true => !primary_index.column_ids.contains(id),
        }) {
            let column = self.find_column(id).unwrap_or_else(|| panic!("no column {id} for index {}", index.id));
            column.encode_as_value(&mut value_bytes);
        }
        if !primary_keys_included {
            for id in primary_index
                .column_ids
                .iter()
                .copied()
                .filter(|id| !index.storing_column_ids.contains(id))
                .filter(|id| !index.column_ids.contains(id))
            {
                let column = self.find_column(id).unwrap_or_else(|| panic!("no column {id} for index {}", index.id));
                column.encode_as_value(&mut value_bytes);
            }
        }
        value_bytes.put_u32(0);
        (key, value_bytes)
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }
}
