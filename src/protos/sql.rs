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

use std::hash::{Hash, Hasher};

use bytes::{Buf, BufMut};
use prost::Message;

use crate::protos::{
    ColumnDescriptor,
    ColumnTypeKind,
    ColumnValue,
    DatabaseDescriptor,
    DescriptorMeta,
    IndexDescriptor,
    SchemaDescriptor,
    StoringFloat32,
    StoringFloat64,
    TableDescriptor,
};

pub trait NamespaceDescriptor {
    fn kind() -> &'static str;

    fn id(&self) -> u64;
    fn name(&self) -> &str;

    fn from_meta(meta: DescriptorMeta) -> Self;
}

impl NamespaceDescriptor for DatabaseDescriptor {
    fn kind() -> &'static str {
        "database"
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn from_meta(meta: DescriptorMeta) -> Self {
        Self { id: meta.id, name: meta.name, timestamp: meta.timestamp }
    }
}

impl NamespaceDescriptor for SchemaDescriptor {
    fn kind() -> &'static str {
        "schema"
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn from_meta(meta: DescriptorMeta) -> Self {
        Self { id: meta.id, name: meta.name, database_id: meta.parent_id, timestamp: meta.timestamp }
    }
}

impl NamespaceDescriptor for TableDescriptor {
    fn kind() -> &'static str {
        "table"
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn from_meta(meta: DescriptorMeta) -> Self {
        let mut descriptor = TableDescriptor::decode(meta.blob.as_slice()).unwrap();
        descriptor.id = meta.id;
        descriptor.name = meta.name;
        descriptor.database_id = meta.database_id;
        descriptor.schema_id = meta.parent_id;
        descriptor.timestamp = meta.timestamp;
        descriptor
    }
}

impl TableDescriptor {
    pub const POSTGRESQL_DIALECT_PREFIX: &[u8] = b"dus0";

    pub fn serial_key(&self, column: &ColumnDescriptor) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(b't');
        key.put_u64(self.id);
        key.push(b'c');
        key.put_u32(column.id);
        key
    }

    pub fn primary_index(&self) -> &IndexDescriptor {
        for index in self.indices.iter() {
            if index.id == 1 {
                return index;
            }
        }
        panic!("table {}(id={}) has no primary index", self.name, self.id)
    }

    pub fn table_prefix(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(b't');
        key.put_u64(self.id);
        key
    }

    pub fn index_prefix(&self, index: &IndexDescriptor) -> Vec<u8> {
        let mut key = self.table_prefix();
        key.push(b'i');
        key.put_u32(index.id);
        key
    }

    pub fn find_column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub fn column(&self, id: u32) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|column| column.id == id)
    }

    pub fn decode_storing_columns(&self, index: &IndexDescriptor, mut bytes: &[u8]) -> Vec<Option<ColumnValue>> {
        let mut values = Vec::with_capacity(index.storing_column_ids.len());
        loop {
            let id = bytes.get_u32();
            if id == 0 {
                break;
            }
            let value = ColumnValue::decode(&mut bytes);
            let i = values.len();
            if i >= index.storing_column_ids.len() {
                values.push(value);
                panic!("index key has more columns than descriptor {index:?}: {values:?}")
            }
            if id != index.storing_column_ids[i] {
                panic!("index descriptor {index:?} does not have column id {id}")
            }
            let column = self.column(id).unwrap();
            match value.as_ref().map(|v| v.type_kind()) {
                None => {
                    if !column.nullable {
                        panic!("column {} expect value, but get null", column.name)
                    }
                },
                Some(type_kind) => {
                    if type_kind != column.type_kind {
                        panic!("index {index:?} column {column:?} mismatch column value {value:?}")
                    }
                },
            }
            values.push(value);
        }
        if values.len() != index.storing_column_ids.len() {
            panic!("index {:?} get mismatching values {:?}", index, values)
        }
        values
    }

    pub fn decode_index_key(&self, index: &IndexDescriptor, mut bytes: &[u8]) -> Vec<ColumnValue> {
        let prefix = self.index_prefix(index);
        bytes.advance(prefix.len());
        let mut values = Vec::with_capacity(index.column_ids.len());
        loop {
            let id = bytes.get_u32();
            if id == 0 {
                break;
            }
            let value = ColumnValue::decode_from_key(&mut bytes);
            let i = values.len();
            if i >= index.column_ids.len() {
                values.push(value);
                panic!("index key has more columns than descriptor {index:?}: {values:?}")
            }
            if id != index.column_ids[i] {
                panic!("index descriptor {index:?} does not have column id {id}")
            }
            let column = self.column(id).unwrap();
            if value.type_kind() != column.type_kind {
                panic!("index {index:?} column {column:?} mismatch column value {value:?}")
            }
            values.push(value);
        }
        if values.len() != index.column_ids.len() {
            panic!("index {:?} get mismatching keys {:?}", index, values)
        }
        values
    }
}

impl IndexDescriptor {
    pub fn is_primary(&self) -> bool {
        self.id == 1
    }

    pub fn sole_column(&self) -> Option<u32> {
        if self.column_ids.len() == 1 {
            return Some(self.column_ids[0]);
        }
        None
    }
}

impl ColumnValue {
    pub fn minimum_of(type_kind: ColumnTypeKind) -> Self {
        match type_kind {
            ColumnTypeKind::Boolean => Self::Boolean(false),
            ColumnTypeKind::Int16 => Self::Int16(i16::MIN.into()),
            ColumnTypeKind::Int32 => Self::Int32(i32::MIN),
            ColumnTypeKind::Int64 => Self::Int64(i64::MIN),
            ColumnTypeKind::Float32 => Self::Float32(f32::MIN.into()),
            ColumnTypeKind::Float64 => Self::Float64(f64::MIN.into()),
            ColumnTypeKind::Bytes => Self::Bytes(Default::default()),
            ColumnTypeKind::String => Self::String(Default::default()),
        }
    }

    pub fn type_kind(&self) -> ColumnTypeKind {
        match self {
            Self::Boolean(_) => ColumnTypeKind::Boolean,
            Self::Int16(_) => ColumnTypeKind::Int16,
            Self::Int32(_) => ColumnTypeKind::Int32,
            Self::Int64(_) => ColumnTypeKind::Int64,
            Self::Float32(_) => ColumnTypeKind::Float32,
            Self::Float64(_) => ColumnTypeKind::Float64,
            Self::Bytes(_) => ColumnTypeKind::Bytes,
            Self::String(_) => ColumnTypeKind::String,
        }
    }

    pub fn into_u64(self) -> u64 {
        match self {
            Self::Int16(i) => i as u64,
            Self::Int32(i) => i as u64,
            Self::Int64(i) => i as u64,
            _ => panic!("expect int, but get{self:?}"),
        }
    }

    pub fn into_string(self) -> String {
        match self {
            Self::String(s) => s,
            _ => panic!("expect string, but get{self:?}"),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Bytes(bytes) => bytes,
            _ => panic!("expect bytes, but get{self:?}"),
        }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.type_kind() as i32);
        match &self {
            Self::Boolean(v) => buf.put_u8(if *v { 1 } else { 0 }),
            Self::Int16(v) => buf.put_i16(*v as i16),
            Self::Int32(v) => buf.put_i32(*v),
            Self::Int64(v) => buf.put_i64(*v),
            Self::Float32(v) => buf.put_f32(v.value),
            Self::Float64(v) => buf.put_f64(v.value),
            Self::Bytes(bytes) => {
                buf.put_u32(bytes.len() as u32);
                buf.put(bytes.as_slice())
            },
            Self::String(string) => {
                buf.put_u32(string.len() as u32);
                buf.put(string.as_bytes())
            },
        }
    }

    pub fn encode_as_key(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.type_kind() as i32);
        match self {
            Self::Boolean(v) => buf.put_u8(if *v { 1 } else { 0 }),
            Self::Int16(v) => buf.put_i16(*v as i16),
            Self::Int32(v) => buf.put_i32(*v),
            Self::Int64(v) => buf.put_i64(*v),
            Self::Float32(v) => buf.put_f32(v.value),
            Self::Float64(v) => buf.put_f64(v.value),
            Self::Bytes(bytes) => Self::encode_escaped(buf, bytes),
            Self::String(string) => Self::encode_escaped(buf, string.as_bytes()),
        }
    }

    pub fn decode_from_key(buf: &mut impl Buf) -> Self {
        let type_kind = buf.get_i32();
        match ColumnTypeKind::try_from(type_kind).unwrap() {
            ColumnTypeKind::Boolean => {
                let v = buf.get_u8() == 1;
                ColumnValue::Boolean(v)
            },
            ColumnTypeKind::Int16 => {
                let i = buf.get_i16();
                ColumnValue::Int16(i.into())
            },
            ColumnTypeKind::Int32 => {
                let i = buf.get_i32();
                ColumnValue::Int32(i)
            },
            ColumnTypeKind::Int64 => {
                let i = buf.get_i64();
                ColumnValue::Int64(i)
            },
            ColumnTypeKind::Float32 => {
                let v = buf.get_f32();
                ColumnValue::Float32(v.into())
            },
            ColumnTypeKind::Float64 => {
                let v = buf.get_f64();
                ColumnValue::Float64(v.into())
            },
            ColumnTypeKind::Bytes => {
                let bytes = ColumnValue::decode_escaped(buf);
                ColumnValue::Bytes(bytes)
            },
            ColumnTypeKind::String => {
                let bytes = ColumnValue::decode_escaped(buf);
                let string = String::from_utf8(bytes).unwrap();
                ColumnValue::String(string)
            },
        }
    }

    // Cockroach style encoding.
    //
    // Alternative: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    fn encode_escaped(buf: &mut impl BufMut, mut bytes: &[u8]) {
        loop {
            let Some(i) = bytes.iter().position(|b| *b == 0) else {
                break;
            };
            let (precedings, remainings) = unsafe { (bytes.get_unchecked(0..i), bytes.get_unchecked(i + 1..)) };
            buf.put_slice(precedings);
            buf.put_u8(0x00);
            buf.put_u8(0xff);
            bytes = remainings;
        }
        buf.put_slice(bytes);
        buf.put_u8(0x00);
        buf.put_u8(0x01);
    }

    fn decode_escaped(buf: &mut dyn Buf) -> Vec<u8> {
        let mut bytes = Vec::new();
        let chunk = buf.chunk();
        let mut remainings = chunk;
        loop {
            let Some(n) = remainings.iter().position(|b| *b == 0) else {
                panic!("no terminal in escaped bytes: {remainings:?}")
            };
            if n + 1 >= remainings.len() {
                panic!("invalid escaped bytes: {remainings:?}")
            }
            bytes.extend_from_slice(&remainings[0..n]);
            let escaped = remainings[n + 1];
            remainings = &remainings[n + 2..];
            match escaped {
                0x01 => break,
                0xff => bytes.push(0x00),
                _ => panic!("invalid escaped bytes: {chunk:?}"),
            };
        }
        buf.advance(chunk.len() - remainings.len());
        bytes
    }

    #[allow(clippy::uninit_vec)]
    pub fn decode(buf: &mut dyn Buf) -> Option<Self> {
        let type_kind = buf.get_i32();
        if type_kind == -1 {
            return None;
        }
        match ColumnTypeKind::try_from(type_kind).unwrap() {
            ColumnTypeKind::Boolean => {
                let v = buf.get_u8() == 1;
                Some(ColumnValue::Boolean(v))
            },
            ColumnTypeKind::Int16 => {
                let i = buf.get_i16();
                Some(ColumnValue::Int16(i.into()))
            },
            ColumnTypeKind::Int32 => {
                let i = buf.get_i32();
                Some(ColumnValue::Int32(i))
            },
            ColumnTypeKind::Int64 => {
                let i = buf.get_i64();
                Some(ColumnValue::Int64(i))
            },
            ColumnTypeKind::Float32 => {
                let v = buf.get_f32();
                Some(ColumnValue::Float32(v.into()))
            },
            ColumnTypeKind::Float64 => {
                let v = buf.get_f64();
                Some(ColumnValue::Float64(v.into()))
            },
            ColumnTypeKind::Bytes => {
                let n = buf.get_u32() as usize;
                let mut bytes = Vec::with_capacity(n);
                unsafe {
                    bytes.set_len(n);
                }
                buf.copy_to_slice(&mut bytes);
                Some(ColumnValue::Bytes(bytes))
            },
            ColumnTypeKind::String => {
                let n = buf.get_u32() as usize;
                let mut bytes = Vec::with_capacity(n);
                unsafe {
                    bytes.set_len(n);
                }
                buf.copy_to_slice(&mut bytes);
                Some(ColumnValue::String(String::from_utf8(bytes).unwrap()))
            },
        }
    }
}

impl Eq for StoringFloat32 {}

impl Hash for StoringFloat32 {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher, {
        state.write_u32(self.value.to_bits())
    }
}

impl From<f32> for StoringFloat32 {
    fn from(value: f32) -> Self {
        Self { value }
    }
}

impl From<StoringFloat32> for f32 {
    fn from(value: StoringFloat32) -> Self {
        value.value
    }
}

impl Eq for StoringFloat64 {}

impl Hash for StoringFloat64 {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher, {
        state.write_u64(self.value.to_bits())
    }
}

impl From<f64> for StoringFloat64 {
    fn from(value: f64) -> Self {
        Self { value }
    }
}

impl From<StoringFloat64> for f64 {
    fn from(value: StoringFloat64) -> Self {
        value.value
    }
}
