check: check_fmt lint

verify: check build test

fmt:
	cargo +nightly fmt --all

check_fmt:
	cargo +nightly fmt --all -- --check

protos:
	cargo run --manifest-path src/protos/build/Cargo.toml

lint:
	cargo clippy --no-deps -- -D clippy::all

build:
	cargo build

test:
	cargo test
