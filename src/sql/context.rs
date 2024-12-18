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

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use derive_where::derive_where;

use crate::tablet::TabletClient;

pub trait SqlClientInfo {
    fn database(&self) -> Option<&str>;

    fn user(&self) -> &str;

    fn socket_addr(&self) -> SocketAddr;
}

#[derive_where(Debug)]
pub struct SqlContext {
    database: Option<String>,
    user: String,
    #[derive_where(skip(Debug))]
    client: TabletClient,
    addr: SocketAddr,
}

impl SqlContext {
    pub fn current_catalog(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn current_user(&self) -> &str {
        &self.user
    }

    pub fn current_schema(&self) -> &str {
        "public"
    }

    pub fn client(&self) -> &TabletClient {
        &self.client
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn new(db: TabletClient, client: &dyn SqlClientInfo) -> Self {
        Self {
            database: client.database().map(|s| s.to_string()),
            user: client.user().to_string(),
            client: db,
            addr: client.socket_addr(),
        }
    }

    pub fn new_unconnected(client: TabletClient, database: Option<String>, user: String) -> Self {
        Self { database, client, user, addr: SocketAddrV4::new(Ipv4Addr::from_bits(0), 0).into() }
    }
}
