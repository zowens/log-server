#![feature(core_intrinsics, test)]
extern crate bytes;
extern crate commitlog;
extern crate futures;
extern crate test;
#[macro_use]
extern crate prometheus;
extern crate byteorder;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate either;
extern crate fnv;
extern crate http;
extern crate hyper;
extern crate libc;
extern crate nix;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate serde_derive;
extern crate env_logger;
extern crate grpcio;
extern crate pin_project;
extern crate protobuf;
extern crate rand;
extern crate tokio_stream;
extern crate toml;

// mod admin_server;

#[macro_use]
mod macros;
mod asynclog;
mod config;
mod configuration;
mod protocol;
mod replication;
mod retry;
mod server;
mod tail_reply;

use std::io::Read;
use std::process::exit;
use std::{env, fs, str};
use tokio::task::LocalSet;

fn config() -> config::Config {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} [config_file]", args[0]);
        exit(1);
    }

    let config: config::Config = {
        let mut f = fs::File::open(&args[1]).expect("Unable to open config file");
        let mut bytes = vec![];
        f.read_to_end(&mut bytes)
            .expect("Unable to read config file");
        let cfg = str::from_utf8(&bytes).expect("Invalid UTF-8");
        toml::from_str(cfg).expect("Unable to parse TOML")
    };

    info!("Starting with configuration {:?}", config);
    config
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = config();
    let (listener, register) = tail_reply::new();
    let lr = replication::log_reader::FileSliceMessageReader;

    let tasks = LocalSet::new();
    let (log, r_log) = asynclog::open(&config.log, listener, lr, &tasks);

    tasks.spawn_local(replication::server(
        config.replication.server_addr,
        r_log.clone(),
    ));

    // TODO: re-enable the admin server
    /*if let Some(ref admin) = config.admin {
        spawn_local(admin_server::server(&admin.server_addr));
    }*/

    tasks.spawn_local(server::server(config.frontend.clone(), log, register));

    tasks
        .run_until(async {
            let node_mgr = configuration::ClusterJoin::new(&config).await;
            replication::ReplicationController::new(node_mgr, r_log)
                .run()
                .await
        })
        .await;
}
