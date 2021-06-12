#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
extern crate futures;
extern crate getopts;
extern crate histogram;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate rand;
extern crate tokio;
extern crate tokio_stream;

use bytes::Bytes;
use client::{Configuration, Connection, LogServerClient};
use futures::stream::StreamExt;
use getopts::Options;
use rand::{distributions::Alphanumeric, rngs::SmallRng, Rng, SeedableRng};
use std::env;
use std::process::exit;
use std::time::{Duration, Instant};
use tokio::time::{interval, sleep};
use tokio::{runtime::Runtime, spawn};
use tokio_stream::wrappers::IntervalStream;

macro_rules! to_ms {
    ($e:expr) => {
        (($e as f32) / 1_000_000f32)
    };
}

struct RandomSource {
    chars: usize,
    rand: SmallRng,
}

impl RandomSource {
    fn new(chars: usize) -> RandomSource {
        RandomSource {
            chars,
            rand: SmallRng::from_entropy(),
        }
    }

    fn random_chars(&mut self) -> Vec<u8> {
        (&mut self.rand)
            .sample_iter(Alphanumeric)
            .map(|c| c as u8)
            .take(self.chars)
            .collect()
    }
}

#[allow(dead_code)]
struct Metrics {
    state: histogram::Histogram,
    conn: Connection,
    msg_size: usize,
}

impl Metrics {
    pub async fn spawn(mut conn: Connection, start_instant: Instant, msg_size: usize) {
        let replies = conn.raw_replies(0);
        tokio::pin!(replies);

        let periodic_report = IntervalStream::new(interval(Duration::from_secs(10)));
        tokio::pin!(periodic_report);

        let mut metrics = Metrics {
            state: histogram::Histogram::default(),
            conn,
            msg_size,
        };

        loop {
            tokio::select! {
                _ = &mut periodic_report.next() => {
                    metrics.snapshot();
                },
                reply = &mut replies.next() => {
                    let since_start = (Instant::now() - start_instant).as_nanos() as u64;
                    match reply {
                        Some(Ok(reply)) => {
                            // We take the two deltas since the start
                            // and capture the difference to get the
                            // amount of time it took end-to-end
                            reply.client_request_ids.iter().for_each(|t| metrics.incr(since_start - t));
                        },
                        Some(Err(e)) => error!("Error processing reply: {:?}", e),
                        None => warn!("Reply stream comopleted"),
                    }
                },
            }
        }
    }

    fn incr(&mut self, nanos: u64) {
        self.state.increment(nanos).unwrap();
    }

    fn snapshot(&mut self) {
        let (requests, p95, p99, p999, max) = {
            let v = (
                self.state.entries(),
                self.state.percentile(95.0).unwrap_or(0),
                self.state.percentile(99.0).unwrap_or(0),
                self.state.percentile(99.9).unwrap_or(0),
                self.state.maximum().unwrap_or(0),
            );
            self.state.clear();
            v
        };

        let req_per_sec = (requests as f32) / 10f32;
        let mb_per_sec = req_per_sec * (self.msg_size as f32) / 1_000_000f32;
        info!("AVG REQ/s :: {}, {} MB/s", req_per_sec, mb_per_sec);
        info!(
            "LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
            to_ms!(p95),
            to_ms!(p99),
            to_ms!(p999),
            to_ms!(max)
        );
    }
}

struct BenchOptions {
    management_server_addr: String,
    throughput: u32,
    bytes: usize,
}

impl BenchOptions {
    fn parse() -> BenchOptions {
        // TODO: add multi-threading, add batching
        let args: Vec<String> = env::args().collect();
        let program = args[0].clone();

        let mut opts = Options::new();
        opts.optopt(
            "a",
            "management-address",
            "address of the management server",
            "HOST:PORT",
        );
        opts.optopt("t", "throughput", "number of connections per second", "N");
        opts.optopt("b", "bytes", "number of bytes per message", "N");
        opts.optflag("h", "help", "print this help menu");

        let matches = match opts.parse(&args[1..]) {
            Ok(m) => m,
            Err(f) => panic!("{}", f.to_string()),
        };

        if matches.opt_present("h") {
            let brief = format!("Usage: {} [options]", program);
            print!("{}", opts.usage(&brief));
            exit(1);
        }

        let mgmt_addr = matches
            .opt_str("a")
            .unwrap_or_else(|| "127.0.0.1:5000".to_string());

        let throughput = matches.opt_str("t").unwrap_or_else(|| "10".to_string());
        let throughput = throughput.as_str().parse::<u32>().unwrap();

        let bytes = matches.opt_str("b").unwrap_or_else(|| "100".to_string());
        let bytes = bytes.as_str().parse::<u32>().unwrap() as usize;

        BenchOptions {
            management_server_addr: mgmt_addr,
            throughput,
            bytes,
        }
    }
}

async fn run_appender(mut conn: Connection, rand: Bytes, wait: Duration, start_instant: Instant) {
    loop {
        let since_start = Instant::now() - start_instant;
        let req_id = since_start.as_nanos() as u64;
        // IMPORTANT!!!!!
        // To benchmark, we've selected the request ID to be the
        // nanoseconds since the start in relative terms. When
        // the reply is captured, the histogram is incremented
        // with the time delta.
        conn.raw_append(0, req_id, rand.clone()).await.unwrap();
        sleep(wait).await;
    }
}

pub fn main() {
    env_logger::init();

    let opts = BenchOptions::parse();

    let mut client_config = Configuration::default();
    client_config
        .management_server(&opts.management_server_addr)
        .unwrap();

    let rt = Runtime::new().unwrap();
    rt.block_on(async move {
        let start_instant = Instant::now();
        let client = LogServerClient::new(client_config);
        let mut throughput = opts.throughput;
        let mut rand = RandomSource::new(opts.bytes);
        // spawn connections that run ever 1ms
        while throughput > 1000 {
            throughput -= 1000;

            let rand: Bytes = rand.random_chars().into();
            let conn = client.new_connection().await.unwrap();
            spawn(run_appender(
                conn,
                rand,
                Duration::from_millis(1),
                start_instant,
            ));
        }

        if throughput > 0 {
            let wait = Duration::from_millis((1000 / throughput).into());
            let rand: Bytes = rand.random_chars().into();
            let conn = client.new_connection().await.unwrap();
            spawn(run_appender(conn, rand, wait, start_instant));
        }

        let msg_size = opts.bytes;
        let metrics_conn = client.new_connection().await.unwrap();
        Metrics::spawn(metrics_conn, start_instant, msg_size).await;
    });
}
