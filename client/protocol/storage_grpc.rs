// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_LOG_STORAGE_APPEND: ::grpcio::Method<super::storage::AppendRequest, super::storage::AppendAck> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/chainreplication.LogStorage/Append",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LOG_STORAGE_REPLIES: ::grpcio::Method<super::storage::ReplyRequest, super::storage::Reply> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/chainreplication.LogStorage/Replies",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LOG_STORAGE_LATEST_OFFSET: ::grpcio::Method<super::storage::LatestOffsetQuery, super::storage::LatestOffsetResult> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/chainreplication.LogStorage/LatestOffset",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LOG_STORAGE_QUERY_LOG: ::grpcio::Method<super::storage::QueryRequest, super::storage::QueryResult> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/chainreplication.LogStorage/QueryLog",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct LogStorageClient {
    client: ::grpcio::Client,
}

impl LogStorageClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        LogStorageClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn append_opt(&self, req: &super::storage::AppendRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::storage::AppendAck> {
        self.client.unary_call(&METHOD_LOG_STORAGE_APPEND, req, opt)
    }

    pub fn append(&self, req: &super::storage::AppendRequest) -> ::grpcio::Result<super::storage::AppendAck> {
        self.append_opt(req, ::grpcio::CallOption::default())
    }

    pub fn append_async_opt(&self, req: &super::storage::AppendRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::storage::AppendAck>> {
        self.client.unary_call_async(&METHOD_LOG_STORAGE_APPEND, req, opt)
    }

    pub fn append_async(&self, req: &super::storage::AppendRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::storage::AppendAck>> {
        self.append_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn replies_opt(&self, req: &super::storage::ReplyRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::storage::Reply>> {
        self.client.server_streaming(&METHOD_LOG_STORAGE_REPLIES, req, opt)
    }

    pub fn replies(&self, req: &super::storage::ReplyRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::storage::Reply>> {
        self.replies_opt(req, ::grpcio::CallOption::default())
    }

    pub fn latest_offset_opt(&self, req: &super::storage::LatestOffsetQuery, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::storage::LatestOffsetResult> {
        self.client.unary_call(&METHOD_LOG_STORAGE_LATEST_OFFSET, req, opt)
    }

    pub fn latest_offset(&self, req: &super::storage::LatestOffsetQuery) -> ::grpcio::Result<super::storage::LatestOffsetResult> {
        self.latest_offset_opt(req, ::grpcio::CallOption::default())
    }

    pub fn latest_offset_async_opt(&self, req: &super::storage::LatestOffsetQuery, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::storage::LatestOffsetResult>> {
        self.client.unary_call_async(&METHOD_LOG_STORAGE_LATEST_OFFSET, req, opt)
    }

    pub fn latest_offset_async(&self, req: &super::storage::LatestOffsetQuery) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::storage::LatestOffsetResult>> {
        self.latest_offset_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn query_log_opt(&self, req: &super::storage::QueryRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::storage::QueryResult> {
        self.client.unary_call(&METHOD_LOG_STORAGE_QUERY_LOG, req, opt)
    }

    pub fn query_log(&self, req: &super::storage::QueryRequest) -> ::grpcio::Result<super::storage::QueryResult> {
        self.query_log_opt(req, ::grpcio::CallOption::default())
    }

    pub fn query_log_async_opt(&self, req: &super::storage::QueryRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::storage::QueryResult>> {
        self.client.unary_call_async(&METHOD_LOG_STORAGE_QUERY_LOG, req, opt)
    }

    pub fn query_log_async(&self, req: &super::storage::QueryRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::storage::QueryResult>> {
        self.query_log_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Output = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait LogStorage {
    fn append(&mut self, ctx: ::grpcio::RpcContext, req: super::storage::AppendRequest, sink: ::grpcio::UnarySink<super::storage::AppendAck>);
    fn replies(&mut self, ctx: ::grpcio::RpcContext, req: super::storage::ReplyRequest, sink: ::grpcio::ServerStreamingSink<super::storage::Reply>);
    fn latest_offset(&mut self, ctx: ::grpcio::RpcContext, req: super::storage::LatestOffsetQuery, sink: ::grpcio::UnarySink<super::storage::LatestOffsetResult>);
    fn query_log(&mut self, ctx: ::grpcio::RpcContext, req: super::storage::QueryRequest, sink: ::grpcio::UnarySink<super::storage::QueryResult>);
}

pub fn create_log_storage<S: LogStorage + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LOG_STORAGE_APPEND, move |ctx, req, resp| {
        instance.append(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_LOG_STORAGE_REPLIES, move |ctx, req, resp| {
        instance.replies(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LOG_STORAGE_LATEST_OFFSET, move |ctx, req, resp| {
        instance.latest_offset(ctx, req, resp)
    });
    let mut instance = s;
    builder = builder.add_unary_handler(&METHOD_LOG_STORAGE_QUERY_LOG, move |ctx, req, resp| {
        instance.query_log(ctx, req, resp)
    });
    builder.build()
}
