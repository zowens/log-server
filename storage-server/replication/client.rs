use super::protocol::{ClientProtocol, ReplicationRequest, ReplicationResponse};
use crate::retry::RetryBehavior;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

const CONNECT_BACKOFF_MS: u64 = 1000;
const MAX_CONNECT_BACKOFF_MS: u64 = 4000;

/// Connection to an upstream node that is idle
pub struct Connection(Framed<TcpStream, ClientProtocol>);

impl Connection {
    fn new(io: TcpStream) -> Connection {
        Connection(Framed::new(io, ClientProtocol))
    }

    /// Sends a replication request
    pub async fn send(&mut self, offset: u64) -> Result<ReplicationResponse, io::Error> {
        trace!("Requesting replication for offset {}", offset);
        let req = ReplicationRequest {
            starting_offset: offset,
        };
        self.0.send(req).await.map_err(|e| {
            error!("Error sending for replication: {:?}", e);
            io::Error::new(io::ErrorKind::Other, "Error on send")
        })?;
        self.0
            .next()
            .await
            .unwrap_or_else(|| Err(io::Error::new(io::ErrorKind::Other, "Connection closed")))
    }
}

/// Connects to an upstream node
pub async fn connect(addr: &SocketAddr) -> Connection {
    let connect_behavior = RetryBehavior::forever()
        .initial_backoff(Duration::from_millis(CONNECT_BACKOFF_MS))
        .max_backoff(Duration::from_millis(MAX_CONNECT_BACKOFF_MS))
        .disable_jitter();

    let addr = *addr;
    let tcp_conn = connect_behavior
        .retry(Box::new(move || TcpStream::connect(addr)))
        .await
        .unwrap();

    if let Err(e) = tcp_conn.set_nodelay(true) {
        warn!("Unable to set TCP nodelay: {:?}", e);
    }

    Connection::new(tcp_conn)
}
