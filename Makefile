# Copyright 2023 The SeamDB Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

check: check_fmt license lint

verify: check build test

fmt:
	cargo +nightly fmt --all

check_fmt:
	cargo +nightly fmt --all -- --check

license: FORCE
	docker run -it --rm -v "$$(pwd):/github/workspace" -u "$$(id -u):$$(id -g)" ghcr.io/korandoru/hawkeye-native:v3 check

# https://www.gnu.org/software/make/manual/make.html#Force-Targets
FORCE:

protos:
	cargo run --manifest-path src/protos/build/Cargo.toml

lint:
	cargo clippy --no-deps -- -D clippy::all

build:
	cargo build --tests --bins
release:
	cargo build --tests --bins --release

test:
	cargo test
clean:
	cargo clean
