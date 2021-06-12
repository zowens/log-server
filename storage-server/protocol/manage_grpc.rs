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

const METHOD_CONFIGURATION_JOIN: ::grpcio::Method<super::manage::JoinRequest, super::manage::NodeConfiguration> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/chainreplication.Configuration/Join",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_CONFIGURATION_POLL: ::grpcio::Method<super::manage::PollRequest, super::manage::NodeConfiguration> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/chainreplication.Configuration/Poll",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_CONFIGURATION_SNAPSHOT: ::grpcio::Method<super::manage::ClientNodeRequest, super::manage::ClientConfiguration> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/chainreplication.Configuration/Snapshot",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct ConfigurationClient {
    client: ::grpcio::Client,
}

impl ConfigurationClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ConfigurationClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn join_opt(&self, req: &super::manage::JoinRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::manage::NodeConfiguration> {
        self.client.unary_call(&METHOD_CONFIGURATION_JOIN, req, opt)
    }

    pub fn join(&self, req: &super::manage::JoinRequest) -> ::grpcio::Result<super::manage::NodeConfiguration> {
        self.join_opt(req, ::grpcio::CallOption::default())
    }

    pub fn join_async_opt(&self, req: &super::manage::JoinRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::manage::NodeConfiguration>> {
        self.client.unary_call_async(&METHOD_CONFIGURATION_JOIN, req, opt)
    }

    pub fn join_async(&self, req: &super::manage::JoinRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::manage::NodeConfiguration>> {
        self.join_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn poll_opt(&self, req: &super::manage::PollRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::manage::NodeConfiguration> {
        self.client.unary_call(&METHOD_CONFIGURATION_POLL, req, opt)
    }

    pub fn poll(&self, req: &super::manage::PollRequest) -> ::grpcio::Result<super::manage::NodeConfiguration> {
        self.poll_opt(req, ::grpcio::CallOption::default())
    }

    pub fn poll_async_opt(&self, req: &super::manage::PollRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::manage::NodeConfiguration>> {
        self.client.unary_call_async(&METHOD_CONFIGURATION_POLL, req, opt)
    }

    pub fn poll_async(&self, req: &super::manage::PollRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::manage::NodeConfiguration>> {
        self.poll_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn snapshot_opt(&self, req: &super::manage::ClientNodeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::manage::ClientConfiguration> {
        self.client.unary_call(&METHOD_CONFIGURATION_SNAPSHOT, req, opt)
    }

    pub fn snapshot(&self, req: &super::manage::ClientNodeRequest) -> ::grpcio::Result<super::manage::ClientConfiguration> {
        self.snapshot_opt(req, ::grpcio::CallOption::default())
    }

    pub fn snapshot_async_opt(&self, req: &super::manage::ClientNodeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::manage::ClientConfiguration>> {
        self.client.unary_call_async(&METHOD_CONFIGURATION_SNAPSHOT, req, opt)
    }

    pub fn snapshot_async(&self, req: &super::manage::ClientNodeRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::manage::ClientConfiguration>> {
        self.snapshot_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Output = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Configuration {
    fn join(&mut self, ctx: ::grpcio::RpcContext, req: super::manage::JoinRequest, sink: ::grpcio::UnarySink<super::manage::NodeConfiguration>);
    fn poll(&mut self, ctx: ::grpcio::RpcContext, req: super::manage::PollRequest, sink: ::grpcio::UnarySink<super::manage::NodeConfiguration>);
    fn snapshot(&mut self, ctx: ::grpcio::RpcContext, req: super::manage::ClientNodeRequest, sink: ::grpcio::UnarySink<super::manage::ClientConfiguration>);
}

pub fn create_configuration<S: Configuration + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_CONFIGURATION_JOIN, move |ctx, req, resp| {
        instance.join(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_CONFIGURATION_POLL, move |ctx, req, resp| {
        instance.poll(ctx, req, resp)
    });
    let mut instance = s;
    builder = builder.add_unary_handler(&METHOD_CONFIGURATION_SNAPSHOT, move |ctx, req, resp| {
        instance.snapshot(ctx, req, resp)
    });
    builder.build()
}
