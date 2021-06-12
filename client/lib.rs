#![feature(test)]
extern crate bytes;
extern crate futures;
extern crate test;
#[macro_use]
extern crate log;
extern crate fnv;
extern crate grpcio;
extern crate pin_project;
extern crate protobuf;
extern crate rand;
extern crate tokio;

mod append;
mod protocol;

use bytes::Bytes;
use futures::join;
use grpcio::{ChannelBuilder, EnvBuilder, Environment};
use protocol::*;
use std::cmp::min;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub use protocol::{AppendSentFuture, LatestOffsetFuture, QueryFuture, Reply, ReplyStream};

// TODO: repoll configuration from the management server
pub struct Connection {
    req_mgr: append::RequestManager,
    head_conn: LogStorageClient,
    tail_conn: LogStorageClient,
}

impl Connection {
    pub async fn append(&mut self, body: Bytes) -> Result<(), io::Error> {
        let (client_request_id, response) = self.req_mgr.push_req().await;

        let mut append_req = AppendRequest::new();
        append_req.set_payload(body);
        append_req.set_client_id(self.req_mgr.client_id());
        append_req.set_client_request_id(client_request_id);

        AppendSentFuture::new(self.head_conn.append_async(&append_req)).await?;
        response.await.map_err(|e| {
            error!("Error appending: {:?}", e);
            io::Error::new(io::ErrorKind::Other, "Error appending")
        })
    }

    pub async fn raw_append(
        &mut self,
        client_id: u64,
        client_request_id: u64,
        payload: Bytes,
    ) -> Result<(), io::Error> {
        let mut append_req = AppendRequest::new();
        append_req.set_payload(payload);
        append_req.set_client_id(client_id);
        append_req.set_client_request_id(client_request_id);

        AppendSentFuture::new(self.head_conn.append_async(&append_req)).await
    }

    pub fn raw_replies(&mut self, client_id: u64) -> ReplyStream {
        let mut reply_req = ReplyRequest::new();
        reply_req.set_client_id(client_id);
        self.tail_conn.replies(&reply_req).unwrap().into()
    }

    pub async fn read(
        &mut self,
        start_offset: u64,
        max_bytes: u32,
    ) -> Result<Vec<(u64, Bytes)>, io::Error> {
        let mut read_req = QueryRequest::new();
        read_req.set_start_offset(start_offset);
        read_req.set_max_bytes(max_bytes);
        QueryFuture::new(self.tail_conn.query_log_async(&read_req)).await
    }

    pub async fn latest_offset(&mut self) -> Result<Option<u64>, io::Error> {
        let query = LatestOffsetQuery::new();
        LatestOffsetFuture::new(self.tail_conn.latest_offset_async(&query)).await
    }
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct Configuration {
    management_server: SocketAddr,
    initial_delay: Duration,
    max_delay: Duration,
}

impl Default for Configuration {
    fn default() -> Configuration {
        Configuration {
            management_server: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000),
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl Configuration {
    pub fn management_server<A: ToSocketAddrs>(
        &mut self,
        addrs: A,
    ) -> Result<&mut Configuration, io::Error> {
        self.management_server = addrs
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No SocketAddress found"))?;
        Ok(self)
    }
}

fn connect(env: Arc<Environment>, addr: &str) -> LogStorageClient {
    let cb = ChannelBuilder::new(env)
        // TODO: compress?
        .default_compression_algorithm(grpcio::CompressionAlgorithms::GRPC_COMPRESS_NONE)
        .max_concurrent_stream(1000)
        .http2_bdp_probe(true);
    let conn = cb.connect(addr);
    LogStorageClient::new(conn)
}

fn connect_management_server(env: Arc<Environment>, addr: &SocketAddr) -> ConfigurationClient {
    let cb = ChannelBuilder::new(env)
        .default_compression_algorithm(grpcio::CompressionAlgorithms::GRPC_COMPRESS_STREAM_GZIP)
        .max_concurrent_stream(10)
        .http2_bdp_probe(true);
    let conn = cb.connect(&format!("{}:{}", addr.ip(), addr.port()));
    ConfigurationClient::new(conn)
}

pub struct LogServerClient {
    config: Configuration,
    env: Arc<Environment>,
}

impl LogServerClient {
    pub fn new(config: Configuration) -> LogServerClient {
        grpcio::redirect_log();
        LogServerClient {
            config,
            env: Arc::new(EnvBuilder::new().build()),
        }
    }

    pub async fn new_connection(&self) -> Result<Connection, io::Error> {
        let client = connect_management_server(self.env.clone(), &self.config.management_server);

        let mut delay = ConnectBackoff::new(&self.config);

        // grab the nodes from the management server (loop until there are at least 2)
        let snapshot = loop {
            debug!("Requesting configuration from management server");

            let snapshot_call = client
                .snapshot_async(&protocol::ClientNodeRequest::new())
                .map_err(|e| {
                    warn!("Error getting snapshot from management server: {:?}", e);
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid construction of snapshot call",
                    )
                })?;

            match snapshot_call.await {
                Ok(snapshot) if snapshot.nodes.len() >= 2 => break snapshot,
                Ok(snapshot) => warn!(
                    "Awaiting more nodes, current count = {}, must be at least 2",
                    snapshot.nodes.len()
                ),
                Err(e) => warn!("Error getting configuration, adding delay {:?}", e),
            };

            delay.backoff().await;
        };

        let head_addr = snapshot.nodes.first().unwrap().get_client_address();
        let tail_addr = snapshot.nodes.last().unwrap().get_client_address();
        debug!(
            "Configuration found from management server, head={}, tail={}",
            head_addr, tail_addr
        );

        let head_conn = connect(self.env.clone(), head_addr);
        let tail_conn = connect(self.env.clone(), tail_addr);

        // force connection open by querying for the latest offset
        loop {
            let head_latest = {
                let query = LatestOffsetQuery::new();
                LatestOffsetFuture::new(head_conn.latest_offset_async(&query))
            };

            let tail_latest = {
                let query = LatestOffsetQuery::new();
                LatestOffsetFuture::new(tail_conn.latest_offset_async(&query))
            };

            match join!(head_latest, tail_latest) {
                (Ok(_), Ok(_)) => {
                    let req_mgr = append::RequestManager::start(&tail_conn)?;
                    return Ok(Connection { req_mgr, head_conn, tail_conn });
                }
                (head_res, tail_res) => {
                    if let Err(e) = head_res {
                        warn!("Error connecting to the head: {:?}", e);
                    }

                    if let Err(e) = tail_res {
                        warn!("Error connecting to the tail: {:?}", e);
                    }

                    delay.backoff().await;
                }
            }
        }
    }
}

struct ConnectBackoff {
    delay: Option<Duration>,
    initial_duration: Duration,
    max_duration: Duration,
}

impl ConnectBackoff {
    fn new(config: &Configuration) -> ConnectBackoff {
        ConnectBackoff {
            delay: None,
            initial_duration: config.initial_delay,
            max_duration: config.max_delay,
        }
    }

    async fn backoff(&mut self) {
        let delay = min(
            self.max_duration,
            self.delay
                .map(|v| v + v)
                .unwrap_or_else(|| self.initial_duration),
        );
        self.delay = Some(delay);
        sleep(delay).await;
    }
}
