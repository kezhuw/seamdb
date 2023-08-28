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

use std::time::Duration;

use anyhow::{bail, Result};
use compact_str::ToCompactString;
use etcd_client::{Client, ConnectOptions, LeaseClient, LeaseKeepAliveStream, LeaseKeeper};
use ignore_result::Ignore;
use tokio::select;

use crate::endpoint::{Endpoint, Params};
use crate::utils::{self, DropOwner, DropWatcher};

pub const NODE_LEASE_DURATION: Duration = Duration::from_secs(20);

pub struct EtcdLease {
    id: i64,
    ttl: Duration,
    keep_alive: DropOwner,
}

impl EtcdLease {
    pub fn id(&self) -> i64 {
        self.id
    }

    #[allow(dead_code)]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn into_keep_alive(self) -> DropOwner {
        self.keep_alive
    }
}

pub(super) enum EtcdHelper {}

impl EtcdHelper {
    pub async fn connect(endpoint: Endpoint<'_>, params: &Params) -> Result<Client> {
        let scheme = match endpoint.scheme() {
            "etcd" => "http",
            "etcd+tls" => "https",
            _ => bail!("unsupported etcd endpoint scheme: {endpoint}"),
        };
        let endpoints: Vec<_> = endpoint.split_with_scheme(scheme).map(|s| s.to_compact_string()).collect();
        let mut options = ConnectOptions::default();
        if let (Some(username), Some(password)) = (params.query("username"), params.query("password")) {
            options = options.with_user(username, password);
        }
        Ok(Client::connect(&endpoints, Some(options)).await?)
    }

    // TODO: This method could leak lease during cancellation.
    pub async fn grant_lease(client: &mut Client, ttl: Option<Duration>) -> Result<EtcdLease> {
        let response = client.lease_grant(ttl.unwrap_or(NODE_LEASE_DURATION).as_secs() as i64, None).await.unwrap();
        let lease_id = response.id();
        let ttl = Duration::from_secs(response.ttl() as u64);
        let keep_alive = Self::keep_alive(client.lease_client(), lease_id, ttl).await?;
        Ok(EtcdLease { id: lease_id, ttl, keep_alive })
    }

    async fn heartbeat_lease(
        mut client: LeaseClient,
        mut keeper: LeaseKeeper,
        mut alive_stream: LeaseKeepAliveStream,
        mut watcher: DropWatcher,
        ttl: Duration,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(ttl / 2);
        loop {
            select! {
                _ = interval.tick() => {
                    keeper.keep_alive().await.ignore();
                },
                r = alive_stream.message() => match r? {
                    None => break,
                    Some(_) => continue,
                },
                _ = watcher.dropped() => {
                    client.revoke(keeper.id()).await?;
                    break;
                },
            }
        }
        Ok(())
    }

    pub async fn keep_alive(mut client: LeaseClient, lease_id: i64, ttl: Duration) -> Result<DropOwner> {
        let (keeper, alive_stream) = client.keep_alive(lease_id).await?;
        let (owner, watcher) = utils::drop_watcher();
        tokio::spawn(Self::heartbeat_lease(client, keeper, alive_stream, watcher, ttl));
        Ok(owner)
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use assertor::*;
    use etcd_client::{EventType, PutOptions};
    use testcontainers::clients::Cli as DockerCli;
    use testcontainers::core::{Container, WaitFor};
    use testcontainers::images::generic::GenericImage;

    use super::EtcdHelper;
    use crate::endpoint::ServiceUri;

    pub struct EtcdContainer {
        container: Container<'static, GenericImage>,
    }

    impl EtcdContainer {
        pub fn uri(&self) -> ServiceUri {
            let cluster = format!("etcd://127.0.0.1:{}", self.container.get_host_port_ipv4(2379));
            cluster.parse().unwrap()
        }
    }

    fn etcd_image() -> GenericImage {
        GenericImage::new("bitnami/etcd", "3.5.9")
            .with_env_var("ALLOW_NONE_AUTHENTICATION", "yes")
            .with_wait_for(WaitFor::StdErrMessage { message: "ready to serve client requests".to_string() })
    }

    pub fn etcd_container() -> EtcdContainer {
        let docker = DockerCli::default();
        let etcd = docker.run(etcd_image());
        EtcdContainer { container: unsafe { std::mem::transmute(etcd) } }
    }

    #[tokio::test]
    async fn test_etcd_lease() {
        let etcd = etcd_container();
        let uri = etcd.uri();

        // given: granted lease and its ttl
        let mut client = EtcdHelper::connect(uri.endpoint(), uri.params()).await.unwrap();
        let lease = EtcdHelper::grant_lease(&mut client, Some(Duration::from_secs(2))).await.unwrap();
        let ttl = lease.ttl();
        println!("lease ttl: {:?}", ttl);

        // when: sleep more than ttl
        tokio::time::sleep(ttl + Duration::from_secs(2)).await;

        // then: lease still hold
        let key = "/key1";
        client.put(key, vec![], Some(PutOptions::new().with_lease(lease.id()))).await.unwrap();

        // given: watch to leased key
        let (_watcher, mut stream) = client.watch(key, None).await.unwrap();

        // when: drop the lease
        drop(lease);

        // then: leased key deleted finally
        let message = stream.message().await.unwrap().unwrap();
        let event = message.events().last().unwrap();
        assert_that!(event.event_type()).is_equal_to(EventType::Delete);
        assert_that!(event.kv().unwrap().key()).is_equal_to(key.as_bytes());
    }
}
