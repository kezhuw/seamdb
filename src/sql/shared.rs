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

use std::fmt::{self, Display, Formatter};

use super::error::SqlError;
use super::traits::*;
use crate::protos::{ColumnDescriptor, ColumnTypeDeclaration, ColumnTypeKind, IndexDescriptor, TableDescriptor};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexKind {
    Index,
    Unique,
    Primary,
}

impl Display for IndexKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index => f.write_str("index"),
            Self::Unique => f.write_str("unique index"),
            Self::Primary => f.write_str("primary index"),
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct IndexMeta {
    kind: IndexKind,
    name: Option<String>,
    column_names: Vec<String>,
    column_ids: Vec<u32>,
}

impl IndexMeta {
    fn new(
        kind: IndexKind,
        name: Option<String>,
        column_names: Vec<String>,
        descriptor: &TableDescriptor,
    ) -> Result<IndexMeta, SqlError> {
        let column_ids = column_names
            .iter()
            .map(|column_name| match descriptor.find_column(column_name.as_ref()) {
                None => Err(SqlError::invalid(format!(
                    "table {} has no column in defining {} {:?}",
                    descriptor.name, kind, name
                ))),
                Some(column) => Ok(column.id),
            })
            .collect::<Result<Vec<_>, SqlError>>()?;
        Ok(Self { kind, name, column_names, column_ids })
    }
}

pub struct TableDescriptorBuilder {
    descriptor: TableDescriptor,
    primary_index: Option<IndexMeta>,
    unique_indices: Vec<IndexMeta>,
    indices: Vec<IndexMeta>,
}

pub struct ColumnDescriptorBuilder<'a> {
    builder: &'a mut TableDescriptorBuilder,
}

impl ColumnDescriptorBuilder<'_> {
    fn column(&mut self) -> &mut ColumnDescriptor {
        self.builder.descriptor.columns.last_mut().unwrap()
    }

    pub fn set_nullable(&mut self, nullable: bool) {
        self.column().nullable = nullable;
    }

    pub fn set_serial(&mut self, serial: bool) {
        let column = self.column();
        column.nullable = column.nullable && !serial;
        column.serial = serial;
    }

    pub fn add_unique_constraint(&mut self, primary: bool, name: Option<String>) -> Result<(), SqlError> {
        let column_names = vec![self.column().name.clone()];
        match primary {
            true => self.builder.add_primary_index(name, column_names),
            false => self.builder.add_unique_index(name, column_names),
        }
    }
}

impl TableDescriptorBuilder {
    pub fn new() -> Self {
        Self { descriptor: TableDescriptor::default(), primary_index: None, unique_indices: vec![], indices: vec![] }
    }

    pub fn add_column(
        &mut self,
        name: String,
        type_kind: ColumnTypeKind,
        type_declaration: Option<ColumnTypeDeclaration>,
    ) -> Result<ColumnDescriptorBuilder<'_>, SqlError> {
        if self.descriptor.find_column(&name).is_some() {
            return Err(SqlError::invalid(format!("multiple columns named {}", name)));
        }
        self.descriptor.add_column(ColumnDescriptor {
            id: 0,
            name,
            nullable: true,
            type_kind,
            type_declaration,
            ..Default::default()
        });
        Ok(ColumnDescriptorBuilder { builder: self })
    }

    pub fn add_unique_index(&mut self, name: Option<String>, column_names: Vec<String>) -> Result<(), SqlError> {
        let index_meta = IndexMeta::new(IndexKind::Unique, name, column_names, &self.descriptor)?;
        self.unique_indices.push(index_meta);
        Ok(())
    }

    pub fn add_primary_index(&mut self, name: Option<String>, column_names: Vec<String>) -> Result<(), SqlError> {
        if let Some(index) = self.primary_index.as_ref() {
            return Err(SqlError::invalid(format!(
                "multiple primary indices: name {:?}, columns {:?} and name {:?}, columns {:?}",
                index.name, index.column_names, name, column_names
            )));
        }
        let index_meta = IndexMeta::new(IndexKind::Primary, name, column_names, &self.descriptor)?;
        self.primary_index = Some(index_meta);
        Ok(())
    }

    pub fn add_index(&mut self, name: Option<String>, column_names: Vec<String>) -> Result<(), SqlError> {
        let index_meta = IndexMeta::new(IndexKind::Index, name, column_names, &self.descriptor)?;
        self.indices.push(index_meta);
        Ok(())
    }

    pub fn build(mut self) -> Result<TableDescriptor, SqlError> {
        let Some(primary_index) = self.primary_index else {
            return Err(SqlError::invalid(format!("table {} defines no index", self.descriptor.name)));
        };
        self.descriptor.add_index(IndexDescriptor {
            id: 0,
            name: primary_index.name.unwrap_or_default(),
            column_ids: primary_index.column_ids,
            unique: true,
            storing_column_ids: vec![],
        });
        for unique_index in self.unique_indices.drain(0..) {
            self.descriptor.add_index(IndexDescriptor {
                id: 0,
                name: unique_index.name.unwrap_or_default(),
                column_ids: unique_index.column_ids,
                unique: true,
                storing_column_ids: vec![],
            });
        }
        for index in self.indices.drain(0..) {
            self.descriptor.add_index(IndexDescriptor {
                id: 0,
                name: index.name.unwrap_or_default(),
                column_ids: index.column_ids,
                unique: true,
                storing_column_ids: vec![],
            });
        }
        Ok(self.descriptor)
    }
}
