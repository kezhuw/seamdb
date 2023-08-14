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
        .type_attribute("TxnStatus", "#[derive(::num_enum::TryFromPrimitive)]")
        .oneof_enum("Value")
        .enumerate_field(".seamdb")
        .require_field("TimestampedValue.value")
        .require_field("TimestampedValue.timestamp");

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