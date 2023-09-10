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
        .into_iter()
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().ends_with(".proto"))
        .collect();

    let outdir = parent.join("generated");

    let _ = fs::remove_dir_all(&outdir);
    fs::create_dir(&outdir).unwrap();

    let mut config = prost_build::Config::new();
    config
        .type_attribute("Timestamp", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        .type_attribute("MessageId", "#[derive(Copy, Eq, PartialOrd, Ord)]")
        .type_attribute("TabletWatermark", "#[derive(Copy)]")
        .type_attribute("TxnStatus", "#[derive(::num_enum::TryFromPrimitive)]")
        .oneof_enum("Value")
        .oneof_enum("Temporal")
        .oneof_enum("DataRequest")
        .oneof_enum("DataResponse")
        .enumerate_field(".seamdb")
        .require_field(".seamdb.TabletWatermark")
        .require_field("TxnMeta.create_ts")
        .require_field("TxnRecord.create_ts")
        .require_field("Transaction.create_ts")
        .require_field("TimestampedValue.value")
        .require_field("TimestampedValue.timestamp")
        .require_field("PutResponse.write_ts")
        .require_field("LocateResponse.deployment")
        .require_field("TabletDepot.range")
        .require_field("TabletManifest.tablet")
        .require_field("TabletManifest.watermark")
        .require_field("TabletDescriptor.range")
        .require_field("TabletDeployment.tablet")
        .require_field("TabletListRequest.range")
        .require_field("TabletDeployRequest.deployment")
        .require_field("TabletDescription.depot");

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
        write!(&mut file, "mod {};\n", module).unwrap();
        write!(&mut file, "pub use self::{}::*;\n", module).unwrap();
    }
}
