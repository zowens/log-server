extern crate futures;
extern crate grpcio;
extern crate protobuf;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate env_logger;
extern crate tokio;
extern crate toml;

mod chain;
mod config;
mod handle;
mod protocol;

use config::Config;
use futures::FutureExt;
use grpcio::{Environment, ServerBuilder};
use std::io::Read;
use std::process::exit;
use std::sync::Arc;
use std::{env, fs, str};
use tokio::signal;

fn load_config() -> Config {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} [config_file]", args[0]);
        exit(1);
    }

    let config: Config = {
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

    let Config {
        server_addr,
        failure_detection,
    } = load_config();
    let chain = chain::Chain::new(failure_detection);
    let service = handle::ManagementService(chain.clone());

    let env = Arc::new(Environment::new(1));

    let host = server_addr.ip().to_string();
    let port = server_addr.port();

    let mut server = ServerBuilder::new(env)
        .register_service(protocol::create_configuration(service))
        .bind(host, port)
        .build()
        .unwrap();
    server.start();

    for (host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }

    // wait for sigint and run failure detection on this thread
    let ctrl_c = signal::ctrl_c().map(|_| ());

    let failure_detector = chain.spawn_failure_detector();
    ctrl_c.await;
    server.shutdown().await.unwrap();
    failure_detector.abort();
}
