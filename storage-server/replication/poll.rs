use super::client::connect;
use super::log_reader::FileSlice;
use crate::asynclog::Messages;
use crate::asynclog::ReplicatorAsyncLog;
use futures::future::join;
use std::io;
use std::net::SocketAddr;

/// Replication state machine that reads from an upstream server
pub struct UpstreamReplication<'a> {
    addr: SocketAddr,
    log: &'a mut ReplicatorAsyncLog<FileSlice>,
}

impl<'a> UpstreamReplication<'a> {
    /// Creates a replication state machine connecting to the upstream node
    pub fn new(
        addr: &SocketAddr,
        log: &'a mut ReplicatorAsyncLog<FileSlice>,
    ) -> UpstreamReplication<'a> {
        UpstreamReplication { addr: *addr, log }
    }

    pub async fn replicate(self) -> Result<(), io::Error> {
        loop {
            // Initial State, Concurrently:
            // * Connect to the upstream node
            // * Determine the latest log offset
            let (mut conn, latest_log_offset) =
                join(connect(&self.addr), self.log.last_offset()).await;

            // request the first batch of messages from the upstream node
            //
            // TODO: should we verify that each nodes have the same snapshot view of the log for
            // correctness?
            let first_missing_offset = latest_log_offset.unwrap().map(|off| off + 1).unwrap_or(0);
            let mut response = match conn.send(first_missing_offset).await {
                Ok(response) => response,
                Err(e) => {
                    error!("Error with replication, attempting reconnect: {}", e);
                    return Err(e);
                }
            };

            loop {
                let messages = Messages::parse(response.messages.freeze()).map_err(|e| {
                    error!("Error with upstream: {:?}", e);
                    io::Error::new(io::ErrorKind::Other, "Invalid messages")
                })?;

                let next_off = messages
                    .next_offset()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Zero messages"))?;

                // concurrently append the current batch of messages
                // and request the next batch of messages
                let (_, current_response) = join(
                    self.log.append_from_replication(messages),
                    conn.send(next_off),
                )
                .await;

                match current_response {
                    Ok(current_response) => {
                        response = current_response;
                    }
                    Err(e) => {
                        error!("Error with replication, attempting reconnect: {}", e);
                        break;
                    }
                }
            }
        }
    }
}
