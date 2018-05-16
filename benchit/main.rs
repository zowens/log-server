#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
#[macro_use]
extern crate futures;
extern crate getopts;
extern crate histogram;
#[macro_use]
extern crate log;
extern crate rand;
extern crate tokio;

use client::{AppendFuture, Configuration, Connection, LogServerClient};
use futures::{Future, Poll};
use getopts::Options;
use rand::{Rng, XorShiftRng};
use std::cell::RefCell;
use std::env;
use std::io;
use std::process::exit;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use tokio::executor::current_thread::spawn;
use tokio::runtime::current_thread::Runtime;

macro_rules! to_ms {
    ($e:expr) => {
        (($e as f32) / 1_000_000f32)
    };
}

struct RandomSource {
    chars: usize,
    rand: XorShiftRng,
}

impl RandomSource {
    fn new(chars: usize) -> RandomSource {
        RandomSource {
            chars,
            rand: XorShiftRng::new_unseeded(),
        }
    }

    fn random_chars(&mut self) -> Vec<u8> {
        self.rand
            .gen_ascii_chars()
            .take(self.chars)
            .map(|c| c as u8)
            .collect()
    }
}

#[derive(Clone)]
struct Metrics {
    state: Arc<Mutex<(u32, histogram::Histogram)>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        let metrics = Metrics {
            state: Arc::new(Mutex::new((0, histogram::Histogram::new()))),
        };

        {
            let metrics = metrics.clone();
            thread::spawn(move || {
                let mut last_report = time::Instant::now();
                loop {
                    thread::sleep(time::Duration::from_secs(10));
                    let now = time::Instant::now();
                    metrics
                        .snapshot(now.duration_since(last_report))
                        .unwrap_or_else(|e| {
                            error!("Error writing metrics: {}", e);
                            ()
                        });
                    last_report = now;
                }
            });
        }

        metrics
    }

    pub fn incr(&self, duration: time::Duration) {
        if duration.as_secs() > 0 {
            println!("WARN: {}s latency", duration.as_secs());
            return;
        }

        let nanos = duration.subsec_nanos() as u64;
        let mut data = self.state.lock().unwrap();
        data.0 += 1;
        data.1.increment(nanos).unwrap();
    }

    pub fn snapshot(&self, since_last: time::Duration) -> Result<(), &str> {
        let (requests, p95, p99, p999, max) = {
            let mut data = self.state.lock().unwrap();
            let v = (
                data.0,
                data.1.percentile(95.0)?,
                data.1.percentile(99.0)?,
                data.1.percentile(99.9)?,
                data.1.maximum()?,
            );
            data.0 = 0;
            data.1.clear();
            v
        };
        println!(
            "AVG REQ/s :: {}",
            (requests as f32)
                / (since_last.as_secs() as f32
                    + (since_last.subsec_nanos() as f32 / 1_000_000_000f32))
        );

        println!(
            "LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
            to_ms!(p95),
            to_ms!(p99),
            to_ms!(p999),
            to_ms!(max)
        );

        Ok(())
    }
}

#[allow(or_fun_call)]
fn parse_opts() -> (String, u32, u32, usize) {
    // TODO: add multi-threading, add batching

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "address of the server", "HOST:PORT");
    opts.optopt("c", "connections", "number of connections", "N");
    opts.optopt(
        "r",
        "concurrent-requests",
        "number of concurrent requests",
        "N",
    );
    opts.optopt("b", "bytes", "number of bytes per message", "N");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options]", program);
        print!("{}", opts.usage(&brief));
        exit(1);
    }

    let addr = matches.opt_str("a").unwrap_or("127.0.0.1:4000".to_string());

    let conns = matches.opt_str("c").unwrap_or("1".to_string());
    let conns = u32::from_str_radix(conns.as_str(), 10).unwrap();

    let concurrent = matches.opt_str("r").unwrap_or("2".to_string());
    let concurrent = u32::from_str_radix(concurrent.as_str(), 10).unwrap();

    let bytes = matches.opt_str("b").unwrap_or("100".to_string());
    let bytes = u32::from_str_radix(bytes.as_str(), 10).unwrap() as usize;

    (addr, conns, concurrent, bytes)
}

struct TrackedRequest {
    client: Rc<RefCell<Connection>>,
    rand: RandomSource,
    f: AppendFuture,
    metrics: Metrics,
    start: time::Instant,
}

impl TrackedRequest {
    fn new(metrics: Metrics, conn: Rc<RefCell<Connection>>, chars: usize) -> TrackedRequest {
        let mut rand = RandomSource::new(chars);
        let f = { conn.borrow_mut().append(rand.random_chars()) };
        TrackedRequest {
            client: conn,
            metrics: metrics,
            start: time::Instant::now(),
            rand,
            f,
        }
    }
}

impl Future for TrackedRequest {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            try_ready!(self.f.poll());
            let stop = time::Instant::now();
            self.metrics.incr(stop.duration_since(self.start));
            self.f = self.client.borrow_mut().append(self.rand.random_chars());
            self.start = stop;
        }
    }
}

pub fn main() {
    env_logger::init().unwrap();

    let (addr, connections, concurrent, bytes) = parse_opts();

    let metrics = Metrics::new();

    let mut rt = Runtime::new().unwrap();

    let mut client_config = Configuration::default();
    client_config.head(addr).unwrap();
    let client = LogServerClient::new(client_config);

    for _ in 0..connections {
        let m = metrics.clone();
        rt.spawn(
            client
                .new_connection()
                .map(move |conn| {
                    let conn = Rc::new(RefCell::new(conn));

                    for _ in 0..concurrent {
                        spawn(
                            TrackedRequest::new(m.clone(), conn.clone(), bytes).map_err(|e| {
                                error!("I/O Error for request: {}", e);
                            }),
                        );
                    }

                    ()
                })
                .map_err(|e| {
                    error!("I/O Error for connection: {}", e);
                    ()
                }),
        );
    }

    rt.run().unwrap();
}
