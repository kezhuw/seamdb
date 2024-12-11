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

use datafusion::common::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use thiserror::Error;

use crate::kv::KvError;
use crate::protos::ColumnTypeKind;
use crate::tablet::TabletClientError;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("no statement")]
    NoStatement,
    #[error("multiple statements")]
    MultipleStatements,
    #[error("missing column {0}")]
    MissingColumn(String),
    #[error("database {0} not exists")]
    DatabaseNotExists(String),
    #[error("database {0} already exists")]
    DatabaseAlreadyExists(String),
    #[error("schema {0} not exists")]
    SchemaNotExists(String),
    #[error("schema {0} already exists")]
    SchemaAlreadyExists(String),
    #[error("table {0} already exists")]
    TableAlreadyExists(String),
    #[error("{0}")]
    TabletClientError(#[from] TabletClientError),
    #[error("{0}")]
    KvError(#[from] KvError),
    #[error("{table}.{column} is not nullable")]
    NotNullableColumn { table: String, column: String },
    #[error("{table}.{column} expect type {expect:?} but get {actual:?}")]
    MismatchColumnType { table: String, column: String, expect: ColumnTypeKind, actual: ColumnTypeKind },
    #[error("index already exist")]
    UniqueIndexAlreadyExists,
    #[error("{0}")]
    ExecutorError(#[from] DataFusionError),
    #[error("{0} unimplemented")]
    Unimplemented(Cow<'static, str>),
    #[error("invalid sql: {0}")]
    Invalid(Cow<'static, str>),
    #[error("{0} unsupported")]
    Unsupported(Cow<'static, str>),
    #[error("unexpected error: {0}")]
    Unexpected(Cow<'static, str>),
}

impl SqlError {
    pub fn unexpected(msg: impl Into<Cow<'static, str>>) -> Self {
        Self::Unexpected(msg.into())
    }

    pub fn unsupported(feature: impl Into<Cow<'static, str>>) -> Self {
        Self::Unsupported(feature.into())
    }

    pub fn unimplemented(msg: impl Into<Cow<'static, str>>) -> Self {
        Self::Unimplemented(msg.into())
    }

    pub fn invalid(msg: impl Into<Cow<'static, str>>) -> Self {
        Self::Invalid(msg.into())
    }

    pub fn is_retriable(&self) -> bool {
        // FIXME
        false
    }

    pub fn identity_already_exists(kind: &str, name: String) -> Self {
        match kind {
            "table" => Self::TableAlreadyExists(name),
            "schema" => Self::SchemaAlreadyExists(name),
            "database" => Self::DatabaseAlreadyExists(name),
            _ => Self::unexpected(format!("{kind} {name} already exists")),
        }
    }
}

impl From<ParserError> for SqlError {
    fn from(err: ParserError) -> SqlError {
        SqlError::from(DataFusionError::from(err))
    }
}

impl From<SqlError> for DataFusionError {
    fn from(err: SqlError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}
