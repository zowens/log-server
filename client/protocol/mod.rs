#![allow(dead_code)]
mod manage;
mod manage_grpc;
mod storage;
mod storage_grpc;

pub use self::manage::*;
pub use self::manage_grpc::ConfigurationClient;
pub use self::storage::*;
pub use self::storage_grpc::LogStorageClient;
use bytes::Bytes;
use futures::{Future, Stream};
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

macro_rules! wrap_future {
    ($name:ident, $rpc_ty:ty, $result_ty:ty, $res_var:ident, $map:expr) => {
        pub struct $name(grpcio::Result<grpcio::ClientUnaryReceiver<$rpc_ty>>);

        impl $name {
            pub(crate) fn new(res: grpcio::Result<grpcio::ClientUnaryReceiver<$rpc_ty>>) -> $name {
                $name(res)
            }
        }

        impl Future for $name {
            type Output = Result<$result_ty, io::Error>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                match &mut self.get_mut().0 {
                    &mut Ok(ref mut rec) => {
                        let pin_receiver = unsafe { Pin::new_unchecked(rec) };
                        match pin_receiver.poll(cx) {
                            Poll::Ready(Ok($res_var)) => {
                                trace!("[response] {:?}", $res_var);
                                Poll::Ready(Ok($map))
                            }
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(Err(e)) => {
                                error!("Error with server: {:?}", e);
                                Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "Invalid payload",
                                )))
                            }
                        }
                    }
                    &mut Err(_) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid payload",
                    ))),
                }
            }
        }
    };
}

wrap_future!(
    LatestOffsetFuture,
    LatestOffsetResult,
    Option<u64>,
    res,
    res.latest_offset
        .map(|LatestOffsetResult_oneof_latest_offset::offset(v)| v)
);

wrap_future!(
    QueryFuture,
    QueryResult,
    Vec<(u64, Bytes)>,
    res,
    res.entries
        .into_vec()
        .into_iter()
        .map(
            |LogEntry {
                 offset, payload, ..
             }| (offset, payload)
        )
        .collect()
);

wrap_future!(AppendSentFuture, AppendAck, (), _res, ());

#[pin_project]
pub struct ReplyStream(#[pin] grpcio::ClientSStreamReceiver<Reply>);

impl Stream for ReplyStream {
    type Item = Result<Reply, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().0.poll_next(cx) {
            Poll::Ready(Some(Err(e))) => {
                error!("Error polling for reply stream value: {:?}", e);
                Poll::Ready(Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid payload",
                ))))
            }
            Poll::Ready(Some(Ok(v))) => Poll::Ready(Some(Ok(v))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl From<grpcio::ClientSStreamReceiver<Reply>> for ReplyStream {
    fn from(s: grpcio::ClientSStreamReceiver<Reply>) -> ReplyStream {
        ReplyStream(s)
    }
}

wrap_future!(
    ClientConfigurationFuture,
    ClientConfiguration,
    ClientConfiguration,
    res,
    res
);
