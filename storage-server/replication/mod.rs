use crate::asynclog::ReplicatorAsyncLog;
use futures::{pin_mut, sink::SinkExt, stream::StreamExt};
use tokio::net::TcpListener;
use tokio::net::ToSocketAddrs;

mod client;
mod controller;
mod io;
mod poll;
mod protocol;

// TODO: remove pub
pub mod log_reader;

pub use self::controller::ReplicationController;
pub use self::log_reader::{FileSlice, FileSliceMessageReader};

pub async fn server<T: ToSocketAddrs>(
    addr: T,
    log: ReplicatorAsyncLog<FileSlice>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let socket = match listener.accept().await {
            Ok((socket, _)) => socket,
            Err(e) => {
                error!("accept failed = {:?}", e);
                continue;
            }
        };

        let mut log = log.clone();

        if let Err(e) = socket.set_nodelay(true) {
            warn!("Unable to set nodelay on socket: {}", e);
        }

        tokio::spawn(async move {
            let (mut request_stream, write_buf) = self::io::replication_framed(socket);
            pin_mut!(write_buf);
            loop {
                let offset = match request_stream.next().await {
                    Some(Ok(req)) => req.starting_offset,
                    None => {
                        trace!("Connection closed");
                        break;
                    }
                    Some(Err(e)) => {
                        error!("Connection error: {}", e);
                        break;
                    }
                };

                let output = match log.replicate_from(offset).await {
                    Ok(output) => output,
                    Err(_) => break,
                };

                if let Err(e) = write_buf.send(output).await {
                    error!("Connection error: {}", e);
                    break;
                }
            }
        });
    }
}
