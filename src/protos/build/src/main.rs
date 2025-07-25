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

use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

fn main() {
    let pwd = env::var("CARGO_MANIFEST_DIR").unwrap();
    let parent = Path::new(&pwd).parent().unwrap();
    let protos_dir = parent.join("protos");
    let protos: Vec<_> = protos_dir
        .read_dir()
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().ends_with(".proto"))
        .collect();

    let outdir = parent.join("generated");

    let _ = fs::remove_dir_all(&outdir);
    fs::create_dir(&outdir).unwrap();

    let mut config = prost_build::Config::new();
    config
        .skip_debug(std::iter::once("Uuid"))
        .type_attribute("Timestamp", "#[derive(Eq, Hash, PartialOrd, Ord)]")
        .type_attribute("MessageId", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("KeySpan", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("KeyRange", "#[derive(Eq, PartialOrd, Ord)]")
        .type_attribute("Uuid", "#[derive(Eq, Hash, PartialOrd, Ord)]")
        .type_attribute("ColumnDescriptor", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("IndexDescriptor", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("TableDescriptor", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("ColumnValue", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("ColumnTypeDeclaration", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("NumericTypeDeclaration", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("CharacterTypeDeclaration", "#[derive(Eq, Hash, PartialOrd)]")
        .type_attribute("StoringFloat32", "#[derive(PartialOrd)]")
        .type_attribute("StoringFloat64", "#[derive(PartialOrd)]")
        .require_field("DescriptorMeta.timestamp")
        .require_field("TableDescriptor.timestamp")
        .require_field("SchemaDescriptor.timestamp")
        .require_field("DatabaseDescriptor.timestamp")
        .oneof_enum("ColumnTypeDeclaration")
        .oneof_enum("Value")
        .oneof_enum("Temporal")
        .oneof_enum("Transient")
        .oneof_enum("DataRequest")
        .oneof_enum("ColumnValue")
        .oneof_enum("DataResponse")
        .oneof_enum("DataError")
        .enumerate_field(".seamdb")
        .enumerate_field(".sql")
        .require_field(".seamdb.TabletWatermark")
        .require_field("RefreshReadError.temporal")
        .require_field("ConflictWriteError.transient")
        .require_field("TimestampMismatchError.actual")
        .require_field("BatchError.error")
        .require_field("ShardRequest.request")
        .require_field("ShardResponse.response")
        .require_field("BatchRequest.temporal")
        .require_field("BatchResponse.temporal")
        .require_field("ParticipateTxnRequest.txn")
        .require_field("ParticipateTxnResponse.txn")
        .require_field("DataMessage.temporal")
        .require_field("Transaction.meta")
        .require_field("Transaction.heartbeat_ts")
        .require_field("TxnMeta.id")
        .require_field("TxnMeta.start_ts")
        .require_field("TxnMeta.commit_ts")
        .require_field("TxnIntent.txn")
        .require_field("RefreshReadRequest.span")
        .require_field("RefreshReadRequest.from")
        .require_field("TimestampedValue.value")
        .require_field("TimestampedValue.timestamp")
        .require_field("TimestampedKeyValue.value")
        .require_field("TimestampedKeyValue.timestamp")
        .require_field("ScanRequest.range")
        .require_field("PutResponse.write_ts")
        .require_field("LocateResponse.shard")
        .require_field("LocateResponse.deployment")
        .require_field("ClusterDescriptor.timestamp")
        .require_field("ShardDescriptor.range")
        .require_field("ShardDescription.range")
        .require_field("TabletManifest.tablet")
        .require_field("TabletManifest.watermark")
        .require_field("TabletManifest.compaction")
        .require_field("TabletCompaction.accumulated_cursor")
        .require_field("TabletDeployRequest.deployment");

    tonic_build::configure().out_dir(&outdir).compile_with_config(config, &protos, &[protos_dir]).unwrap();

    let mut file = File::create(outdir.join("mod.rs")).unwrap();

    file.write_all(b"#![allow(clippy::all)]\n").unwrap();
    file.write_all(b"\n").unwrap();

    let modules = protos.iter().map(|p| Path::new(p).file_stem().unwrap().to_str().unwrap());
    for (i, module) in modules.enumerate() {
        if i != 0 {
            file.write_all(b"\n").unwrap();
        }
        file.write_all(b"#[rustfmt::skip]\n").unwrap();
        writeln!(&mut file, "mod {};", module).unwrap();
        writeln!(&mut file, "pub use self::{}::*;", module).unwrap();
    }
}
