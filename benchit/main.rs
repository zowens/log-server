#![feature(core_intrinsics)]
extern crate rand;
extern crate histogram;
extern crate getopts;
#[macro_use]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate byteorder;

use std::io::{self, Error};
use std::time;
use std::net::{ToSocketAddrs, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::env;
use rand::Rng;
use getopts::Options;
use std::process::exit;
use futures::{Future, Async, Poll};
use futures::future::BoxFuture;
use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_proto::multiplex::{Multiplex, ClientService, ClientProto, RequestId};
use tokio_proto::{TcpClient, Connect};
use tokio_service::Service;
use byteorder::{ByteOrder, LittleEndian};

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            std::intrinsics::unlikely($e)
        }
    )
}

macro_rules! to_ms {
    ($e:expr) => (
        (($e as f32) / 1000000f32)
    )
}


#[derive(Default)]
struct Request;
struct Response(u64);
struct Protocol(rand::StdRng);

impl Codec for Protocol {
    /// The type of decoded frames.
    type In = (RequestId, Response);

    /// The type of frames to be encoded.
    type Out = (RequestId, Request);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        trace!("Decode, size={}", buf.len());
        if probably_not!(buf.len() < 21) {
            return Ok(None);
        }

        let buf = buf.drain_to(21);
        let response = buf.as_slice();
        assert_eq!(21u32, LittleEndian::read_u32(&response[0..4]));
        assert_eq!(0u8, response[12]);
        let reqid = LittleEndian::read_u64(&response[4..12]);
        let offset = LittleEndian::read_u64(&response[13..21]);
        Ok(Some((reqid, Response(offset))))
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        trace!("Writing request");

        let mut wbuf = [0u8; 12];
        LittleEndian::write_u32(&mut wbuf[0..4], 113);
        LittleEndian::write_u64(&mut wbuf[4..12], msg.0);

        // add length and request ID
        buf.extend_from_slice(&wbuf);

        // add op code
        buf.push(0u8);

        let s: String = self.0.gen_ascii_chars().take(100).collect();
        buf.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

#[derive(Default)]
struct LogProto;
impl ClientProto<TcpStream> for LogProto {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<TcpStream, Protocol>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: TcpStream) -> Self::BindTransport {
        trace!("Bind transport");
        try!(io.set_nodelay(true));
        trace!("Setting up protocol");
        Ok(io.framed(Protocol(rand::StdRng::new()?)))
    }
}

#[derive(Clone)]
struct Metrics {
    state: Arc<Mutex<(u32, histogram::Histogram)>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        let metrics = Metrics { state: Arc::new(Mutex::new((0, histogram::Histogram::new()))) };

        {
            let metrics = metrics.clone();
            thread::spawn(move || {
                let mut last_report = time::Instant::now();
                loop {
                    thread::sleep(time::Duration::from_secs(10));
                    let now = time::Instant::now();
                    metrics.snapshot(now.duration_since(last_report))
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
            let reqs = data.0;
            data.0 = 0;
            (reqs,
             data.1.percentile(95.0)?,
             data.1.percentile(99.0)?,
             data.1.percentile(99.9)?,
             data.1.maximum()?)
        };
        println!("AVG REQ/s :: {}",
                 (requests as f32) /
                 (since_last.as_secs() as f32 +
                  (since_last.subsec_nanos() as f32 / 1000000000f32)));

        println!("LATENCY(ms) :: p95: {}, p99: {}, p999: {}, max: {}",
                 to_ms!(p95),
                 to_ms!(p99),
                 to_ms!(p999),
                 to_ms!(max));

        Ok(())
    }
}

fn parse_opts() -> (SocketAddr, u32, u32) {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("a", "address", "address of the server", "HOST:PORT");
    opts.optopt("w", "threads", "number of connections", "N");
    opts.optopt("c", "concurrent-requests", "number of concurrent requests", "N");
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

    let threads = matches.opt_str("w").unwrap_or("1".to_string());
    let threads = u32::from_str_radix(threads.as_str(), 10).unwrap();

    let concurrent = matches.opt_str("c").unwrap_or("2".to_string());
    let concurrent = u32::from_str_radix(concurrent.as_str(), 10).unwrap();

    (addr.to_socket_addrs().unwrap().next().unwrap(), threads, concurrent)
}

struct TrackedRequest {
    f: BoxFuture<Response, Error>,
    metrics: Metrics,
    start: time::Instant,
}

impl TrackedRequest {
    fn new(metrics: Metrics, f: BoxFuture<Response, Error>) -> TrackedRequest {
        TrackedRequest {
            f: f,
            metrics: metrics,
            start: time::Instant::now(),
        }
    }

    #[inline]
    fn reset(&mut self, f: BoxFuture<Response, Error>) {
        self.f = f;
        self.start = time::Instant::now();
    }
}

impl Future for TrackedRequest {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.f.poll());
        let stop = time::Instant::now();
        self.metrics.incr(stop.duration_since(self.start));
        Ok(Async::Ready(()))
    }
}


struct RunFuture {
    client: ClientService<TcpStream, LogProto>,
    reqs: Vec<TrackedRequest>,
}

impl RunFuture {
    fn spawn(metrics: Metrics, client: ClientService<TcpStream, LogProto>, n: u32) -> RunFuture {
        debug!("Spawning request");

        let mut reqs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            reqs.push(TrackedRequest::new(metrics.clone(), client.call(Request).boxed()));
        }
        RunFuture {
            client: client,
            reqs: reqs,
        }
    }
}

impl Future for RunFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("Run future poll");
        loop {
            let mut not_ready = 0;
            // TODO: prevent unnecessary polling by spawning separate futures
            for req in &mut self.reqs {
                let poll_res = req.poll();
                match poll_res {
                    Ok(Async::Ready(())) => {
                        req.reset(self.client.call(Request).boxed());
                    },
                    Ok(Async::NotReady) => {
                        not_ready += 1;
                    },
                    Err(e) => return Err(e)
                }
            }

            if not_ready == self.reqs.len() {
                return Ok(Async::NotReady);
            }
        }
    }
}

enum ConnectionState {
    Connect(Metrics, u32, Connect<Multiplex, LogProto>),
    Run(RunFuture),
}

impl Future for ConnectionState {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (m, concurrent, conn) = match *self {
            ConnectionState::Connect(ref metrics, concurrent, ref mut f) => {
                let conn = try_ready!(f.poll());
                debug!("Connected");
                (metrics.clone(), concurrent, conn)
            }
            ConnectionState::Run(ref mut f) => {
                return f.poll();
            }
        };

        *self = ConnectionState::Run(RunFuture::spawn(m, conn, concurrent));
        self.poll()
    }
}

pub fn main() {
    env_logger::init().unwrap();

    let (addr, threads, concurrent) = parse_opts();

    let metrics = Metrics::new();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let client = TcpClient::new(LogProto);
    core.run(futures::future::join_all((0..threads)
            .map(|_| ConnectionState::Connect(metrics.clone(), concurrent, client.connect(&addr, &handle)))))
        .unwrap();
}
