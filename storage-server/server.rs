use crate::asynclog::AsyncLog;
use crate::config::FrontendConfig;
use crate::protocol::*;
use crate::tail_reply::TailReplyRegistrar;
use bytes::Bytes;
use commitlog::{message::MessageSet, ReadLimit};
use futures::{
    future::{Future, TryFuture},
    ready,
    stream::StreamExt,
};
use grpcio::{
    self, Environment, RpcContext, Server as GrpcServer, ServerBuilder, ServerStreamingSink,
    UnarySink, WriteFlags,
};
use pin_project::pin_project;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[derive(Clone)]
struct Service(AsyncLog, TailReplyRegistrar);

impl LogStorage for Service {
    fn append(&mut self, ctx: RpcContext, req: AppendRequest, sink: UnarySink<AppendAck>) {
        self.0
            .append(req.client_id, req.client_request_id, req.payload);
        ctx.spawn(LogErr(sink.success(AppendAck::new())));
    }

    fn replies(&mut self, ctx: RpcContext, req: ReplyRequest, sink: ServerStreamingSink<Reply>) {
        let wf = WriteFlags::default()
            .force_no_compress(true)
            .buffer_hint(false);

        let stream = self
            .1
            .listen(req.client_id)
            .map(move |m| {
                let mut reply = Reply::new();
                reply.set_client_request_ids(m);
                (reply, wf)
            })
            .map(Ok);

        ctx.spawn(LogErr(stream.forward(sink)));
    }

    fn latest_offset(
        &mut self,
        ctx: RpcContext,
        _req: LatestOffsetQuery,
        sink: UnarySink<LatestOffsetResult>,
    ) {
        let last_off = self.0.last_offset();
        ctx.spawn(async move {
            let mut res = LatestOffsetResult::new();
            if let Some(off) = last_off.await.unwrap() {
                res.set_offset(off);
            }
            LogErr(sink.success(res)).await;
        });
    }

    fn query_log(&mut self, ctx: RpcContext, req: QueryRequest, sink: UnarySink<QueryResult>) {
        trace!("Query log: {:?}", req);

        let read_limit = ReadLimit::max_bytes(req.max_bytes as usize);
        let read = self.0.read(req.start_offset, read_limit);
        ctx.spawn(async move {
            let mut res = QueryResult::new();
            let messages = read.await.unwrap();
            for m in messages.iter() {
                let mut entry = LogEntry::new();
                entry.set_offset(m.offset());
                entry.set_payload(Bytes::copy_from_slice(m.payload()));
                res.mut_entries().push(entry);
            }

            trace!("Query log done");
            LogErr(sink.success(res)).await;
        });
    }
}

pub async fn server(cfg: FrontendConfig, log: AsyncLog, tail: TailReplyRegistrar) {
    grpcio::redirect_log();

    let service = create_log_storage(Service(log, tail));
    let env = Arc::new(Environment::new(1));

    let host = cfg.server_addr.ip().to_string();
    let port = cfg.server_addr.port();

    info!("STARTING GRPC SERVER: {}:{}", host, port);

    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(host, port)
        .build()
        .unwrap();
    server.start();

    for (ref host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }

    WaitFuture(server).await
}

struct WaitFuture(GrpcServer);

impl Future for WaitFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<()> {
        Poll::Pending
    }
}

#[pin_project]
struct LogErr<F>(#[pin] F);
impl<F> Future for LogErr<F>
where
    F: TryFuture,
    F::Error: Debug,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        if let Err(e) = ready!(self.project().0.try_poll(cx)) {
            error!("{:?}", e);
        }
        Poll::Ready(())
    }
}
