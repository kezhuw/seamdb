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

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Result};
use clap::Parser;
use pgwire::tokio::process_socket;
use seamdb::cluster::{ClusterEnv, EtcdClusterMetaDaemon, EtcdNodeRegistry, NodeId};
use seamdb::endpoint::{Endpoint, ServiceUri};
use seamdb::log::{KafkaLogFactory, LogManager, MemoryLogFactory};
use seamdb::protos::TableDescriptor;
use seamdb::sql::postgres::PostgresqlHandlerFactory;
use seamdb::tablet::{TabletClient, TabletNode};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, instrument};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

async fn new_log_manager(uri: ServiceUri<'_>) -> Result<LogManager> {
    match uri.scheme() {
        "memory" => LogManager::new(MemoryLogFactory::new(), &uri).await,
        "kafka" => LogManager::new(KafkaLogFactory {}, &uri).await,
        scheme => Err(anyhow!("unsupported log schema: {}, supported: memory, kafka", scheme)),
    }
}

#[instrument(skip_all, fields(addr = %addr))]
async fn serve_connection(factory: PostgresqlHandlerFactory, stream: TcpStream, addr: SocketAddr) {
    match process_socket(stream, None, factory).await {
        Ok(_) => info!("connection terminated"),
        Err(err) => info!("connection terminated: {err}"),
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Meta cluster uri to store cluster wide metadata, e.g. etcd://etcd-cluster/scope.
    #[arg(long = "cluster.uri")]
    cluster_uri: String,
    /// Cluster name.
    #[arg(long = "cluster.name", default_value = "seamdb")]
    cluster_name: String,
    /// Log cluster uri to store WAL logs, e.g. kafka://kafka-cluster.
    #[arg(long = "log.uri")]
    log_uri: String,
    /// Port to serve PostgreSQL compatible SQL statements.
    #[arg(long = "sql.postgresql.port", default_value_t = 5432)]
    pgsql_port: u16,
}

#[tokio::main]
async fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());

    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(non_blocking).with_level(true).with_file(true).with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let cluster_uri = ServiceUri::parse(&args.cluster_uri).unwrap();
    let log_uri = ServiceUri::parse(&args.log_uri).unwrap();

    let node_id = NodeId::new_random();
    info!("Starting node {node_id}");

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let endpoint = Endpoint::try_from(address.as_str()).unwrap();
    let (nodes, lease) =
        EtcdNodeRegistry::join(cluster_uri.clone(), node_id.clone(), Some(endpoint.to_owned())).await.unwrap();
    let log_manager = new_log_manager(log_uri).await.unwrap();
    let cluster_env = ClusterEnv::new(log_manager.into(), nodes).with_replicas(1);
    let mut cluster_meta_handle =
        EtcdClusterMetaDaemon::start(args.cluster_name, cluster_uri.clone(), cluster_env.clone()).await.unwrap();
    let descriptor_watcher = cluster_meta_handle.watch_descriptor(None).await.unwrap();
    let deployment_watcher = cluster_meta_handle.watch_deployment(None).await.unwrap();
    let cluster_env = cluster_env.with_descriptor(descriptor_watcher).with_deployment(deployment_watcher.monitor());
    let _node = TabletNode::start(node_id, listener, lease, cluster_env.clone());
    let client = TabletClient::new(cluster_env).scope(TableDescriptor::POSTGRESQL_DIALECT_PREFIX);
    tokio::time::sleep(Duration::from_secs(20)).await;

    let factory = PostgresqlHandlerFactory::new(client);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", args.pgsql_port)).await.unwrap();
    info!("Listening on {} ...", listener.local_addr().unwrap());
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        tokio::spawn(serve_connection(factory.clone(), stream, addr));
    }
}
