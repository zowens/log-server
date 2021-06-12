use crate::config::LogConfig;
use bytes::Bytes;
use commitlog::message::{set_offsets, MessageBuf, MessageSet};
use commitlog::reader::LogSliceReader;
use commitlog::{CommitLog, LogOptions, Offset, OffsetRange, ReadError, ReadLimit};
use futures::{pin_mut, stream::StreamExt};
use prometheus::{exponential_buckets, linear_buckets, Gauge, Histogram};
use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::LocalSet;

mod batch;
mod bufpool;
mod messages;
mod sync;

use self::batch::BatchMessageStream;
use self::bufpool::BytesPool;
pub use self::messages::{Messages, MessagesMut, SingleMessage};
pub use self::sync::LogFuture;
use self::sync::{channel, LogSender};

pub enum ReplicationSource<R> {
    InMemory {
        messages: Messages,
        latest_log_offset: u64,
    },

    LogRead {
        messages: R,
        latest_log_offset: u64,
    },
}

impl<R> ReplicationSource<R> {
    /// Last offset appended to the log.
    ///
    /// The last offset may be later than the values contained in the messages
    /// for the replication response. This value is used to determine if
    /// the node is caught up to the latest entries.
    ///
    /// TODO: is this still needed???
    #[allow(dead_code)]
    fn latest_log_offset(&self) -> u64 {
        match self {
            ReplicationSource::InMemory {
                latest_log_offset, ..
            } => *latest_log_offset,
            ReplicationSource::LogRead {
                latest_log_offset, ..
            } => *latest_log_offset,
        }
    }
}

lazy_static! {
    static ref LOG_LATEST_OFFSET: Gauge = register_gauge!(opts!(
        "log_last_offset",
        "The log offset of the last entry to the log.",
        labels! {"mod" => "log",}
    ))
    .unwrap();
    static ref APPEND_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "log_append_count",
        "Number of messages appended",
        linear_buckets(0f64, 2f64, 20usize).unwrap()
    )
    .unwrap();
    static ref APPEND_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "log_append_bytes",
        "Number of bytes appended",
        exponential_buckets(500f64, 2f64, 10usize).unwrap()
    )
    .unwrap();
    static ref REPLICATION_APPEND_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "replication_log_append_count",
        "Number of messages appended",
        linear_buckets(0f64, 2f64, 20usize).unwrap()
    )
    .unwrap();
    static ref APPEND_TIME_HISTOGRAM: Histogram = register_histogram!(
        "log_append_ns",
        "Nanos to append to log",
        linear_buckets(20_000f64, 1.5f64, 20usize).unwrap()
    )
    .unwrap();
    static ref FLUSH_TIME_HISTOGRAM: Histogram = register_histogram!(
        "log_flush_ns",
        "Nanos to flugh to disk",
        linear_buckets(20_000f64, 1.5f64, 20usize).unwrap()
    )
    .unwrap();
}

enum ClientRequest {
    Append(MessagesMut),
    LastOffset(LogSender<Option<Offset>>),
    Read(Offset, ReadLimit, LogSender<MessageBuf>),
}

// TODO: remove this
enum ReplicaRequest<R> {
    Replicate(Offset, LogSender<ReplicationSource<R>>),
    AppendFromReplication(Messages, LogSender<OffsetRange>),
}

/// Request sent through the `Sink` for the log
enum LogRequest<R> {
    Replica(ReplicaRequest<R>),
    Client(ClientRequest),
}

/// `Sink` that executes commands on the log during the `start_send` phase
/// and attempts to flush the log on the `poll_complete` phase
struct LogSink<L: AppendListener, R: LogSliceReader> {
    log: CommitLog,
    last_flush: Instant,
    dirty: bool,
    listener: L,
    log_slice_reader: R,
    parked_replication: Option<(Offset, LogSender<ReplicationSource<R::Result>>)>,
    replication_max_bytes: usize,
}

impl<L, R> LogSink<L, R>
where
    L: AppendListener,
    R: LogSliceReader,
{
    fn new(
        log: CommitLog,
        replication_max_bytes: usize,
        listener: L,
        reader: R,
    ) -> LogSink<L, R> {
        LogSink {
            log,
            last_flush: Instant::now(),
            dirty: false,
            listener,
            log_slice_reader: reader,
            parked_replication: None,
            replication_max_bytes,
        }
    }

    /// Trys to replicate via a log read, parking if the offset has not yet been appended.
    fn try_replicate(&mut self, offset: Offset, res: LogSender<ReplicationSource<R::Result>>) {
        let read_res = self.log.reader(
            &mut self.log_slice_reader,
            offset,
            ReadLimit::max_bytes(self.replication_max_bytes),
        );
        match read_res {
            Ok(Some(fs)) => {
                let latest_log_offset = self
                    .log
                    .last_offset()
                    .expect("Unexpected empty last offset value");
                res.send(ReplicationSource::LogRead {
                    messages: fs,
                    latest_log_offset,
                });
            }
            Ok(None) => {
                debug!("Parking replication, no offset {}", offset);
                self.parked_replication = Some((offset, res));
            }
            Err(ReadError::Io(e)) => res.send_err(e),
            Err(ReadError::CorruptLog) => {
                res.send_err_with(ErrorKind::Other, "Corrupt log detected");
            }
            Err(ReadError::NoSuchSegment) => {
                res.send_err_with(ErrorKind::Other, "read error");
            }
        }
    }

    fn log_append(&mut self, ms: &mut Messages) -> Result<OffsetRange, Error> {
        let num_bytes = ms.bytes().len() as f64;

        let start = Instant::now();
        let range = self.log.append_with_offsets(ms).map_err(|e| {
            error!("Unable to append to the log {}", e);
            Error::new(ErrorKind::Other, "append error")
        })?;
        let elapsed = start.elapsed().subsec_nanos() as f64;
        APPEND_TIME_HISTOGRAM.observe(elapsed);

        self.dirty = true;

        let latest_offset = range.iter().next_back().unwrap();

        APPEND_BYTES_HISTOGRAM.observe(num_bytes);
        LOG_LATEST_OFFSET.set(latest_offset as f64);
        APPEND_COUNT_HISTOGRAM.observe(range.len() as f64);

        // TODO: figure out whether the listener should be notified via roles/config
        if let Some((offset, res)) = self.parked_replication.take() {
            debug!("Sending messages to parked replication request");
            if offset == range.first() {
                trace!("Sending in memory replication");
                let latest_log_offset = self
                    .log
                    .last_offset()
                    .expect("Unexpected empty latest log offset");
                res.send(ReplicationSource::InMemory {
                    messages: ms.clone(),
                    latest_log_offset,
                });
            } else {
                warn!("Invalid append, offset {} != {}", offset, range.first());
                self.try_replicate(offset, res);
            }
        } else {
            self.listener.notify_append(ms);
        }

        Ok(range)
    }

    fn handle_client_request(&mut self, request: ClientRequest) {
        match request {
            ClientRequest::Append(mut ms) => {
                set_offsets(&mut ms, self.log.next_offset());
                let mut ms = ms.freeze();
                self.log_append(&mut ms).map(|_| ()).unwrap_or_default();
                // self.pool.push(ms.clone().into_inner());
            }
            ClientRequest::LastOffset(res) => {
                res.send(self.log.last_offset());
            }
            ClientRequest::Read(pos, lim, res) => {
                // TODO: allow file slice to be sent (zero copy all the things!)
                match self.log.read(pos, lim) {
                    Ok(v) => res.send(v),
                    Err(_) => res.send_err_with(ErrorKind::Other, "read error"),
                }
            }
        }
    }

    fn handle_replica_request(&mut self, request: ReplicaRequest<R::Result>) {
        match request {
            ReplicaRequest::AppendFromReplication(mut ms, res) => {
                // assert that the upstream server replicated the correct offset and
                // that the message hash values match the payloads
                {
                    assert!(ms.len() > 0);
                    let first_msg = ms.iter().next().unwrap();
                    let expected_offset = self.log.last_offset().map(|v| v + 1).unwrap_or(0);
                    if rare!(expected_offset != first_msg.offset()) {
                        res.send_err_with(
                            ErrorKind::InvalidInput,
                            "Expected append from replication to be in sequence",
                        );
                        return;
                    }
                }

                trace!("Initiating log append");
                match self.log_append(&mut ms) {
                    Ok(appended_range) => {
                        trace!("DONE APPENDING");
                        // extra tracking of metrics for appends
                        let num_msgs = appended_range.len();
                        REPLICATION_APPEND_COUNT_HISTOGRAM.observe(num_msgs as f64);

                        let start_offset = appended_range.first();
                        let next_offset = appended_range.iter().next_back().unwrap() + 1;
                        trace!(
                            "Replicated to log, starting at {}, next offset is {}",
                            start_offset,
                            next_offset
                        );
                        res.send(appended_range);
                        trace!("Full append finish");
                    }
                    Err(e) => {
                        res.send_err(e);
                    }
                }
            }
            ReplicaRequest::Replicate(offset, res) => {
                self.try_replicate(offset, res);
            }
        }
    }

    fn flush(&mut self) {
        if self.dirty {
            trace!("Log poll_complete, flushing");
            let now = Instant::now();
            if (now - self.last_flush) > Duration::from_secs(1) {
                trace!("Attempting flush");

                match self.log.flush() {
                    Err(e) => {
                        error!("Log flush error: {}", e);
                    }
                    _ => {
                        self.last_flush = now;
                        self.dirty = false;
                        trace!("Flushed");
                    }
                };

                let elapsed = now.elapsed().subsec_nanos() as f64;
                FLUSH_TIME_HISTOGRAM.observe(elapsed);
            }
        }
    }
}

/// `AsyncLog` allows asynchronous operations against the `CommitLog`.
#[derive(Clone)]
pub struct AsyncLog {
    req_sink: mpsc::UnboundedSender<ClientRequest>,
    append_sink: mpsc::UnboundedSender<SingleMessage>,
}

pub fn open<L, R>(
    cfg: &LogConfig,
    listener: L,
    reader: R,
    local_task_set: &LocalSet,
) -> (AsyncLog, ReplicatorAsyncLog<R::Result>)
where
    L: AppendListener + Send + 'static,
    R: LogSliceReader + Send + 'static,
    R::Result: Send + 'static,
{
    // TODO: bounded channels
    let (client_req_sink, mut client_req_stream) = mpsc::unbounded_channel::<ClientRequest>();
    let (repl_req_sink, mut repl_req_stream) = mpsc::unbounded_channel::<LogRequest<R::Result>>();
    let (append_sink, append_stream) = mpsc::unbounded_channel::<SingleMessage>();

    let log = {
        let mut opts = LogOptions::new(&cfg.dir);
        opts.message_max_bytes(cfg.message_max_bytes);
        opts.index_max_items(cfg.index_max_items);
        opts.segment_max_bytes(cfg.segment_max_bytes);
        CommitLog::new(opts).expect("Unable to open log")
    };

    // start the metric for latest offset, if not already appended
    if let Some(off) = log.last_offset() {
        LOG_LATEST_OFFSET.set(off as f64);
    }

    trace!("Spawning log sink...");

    // TODO: rewrite this :tableflip:
    let message_buffer_bytes = cfg.message_max_bytes;
    let replication_max_bytes = cfg.replication_max_bytes;
    local_task_set.spawn_local(async move {
        let pool = BytesPool::new(message_buffer_bytes);
        let append_stream = BatchMessageStream::new(append_stream, pool.clone()).map(ClientRequest::Append);
        pin_mut!(append_stream);
        let mut log_sink = LogSink::new(log, replication_max_bytes, listener, reader);

        loop {
            // TODO: handle NONE for other streams
            tokio::select! {
                req = client_req_stream.recv() => {
                    log_sink.handle_client_request(req.unwrap());
                },
                req = append_stream.next() => {
                    log_sink.handle_client_request(req.unwrap());
                },
                req = repl_req_stream.recv() => {
                    match req.unwrap() {
                        LogRequest::Replica(replica_req) => log_sink.handle_replica_request(replica_req),
                        LogRequest::Client(client_req) => log_sink.handle_client_request(client_req),
                    }
                }
            }

            log_sink.flush();
        }
    });

    (
        AsyncLog {
            req_sink: client_req_sink,
            append_sink,
        },
        ReplicatorAsyncLog {
            req_sink: repl_req_sink,
        },
    )
}

impl AsyncLog {
    pub fn append(&mut self, client_id: u64, client_req_id: u64, payload: Bytes) {
        self.append_sink
            .send((client_id, client_req_id, payload))
            .map_err(|_| ())
            .expect("unable to append to the log");
    }

    pub fn last_offset(&mut self) -> LogFuture<Option<Offset>> {
        let (snd, f) = channel::<Option<Offset>>();
        self.req_sink
            .send(ClientRequest::LastOffset(snd))
            .map_err(|_| ())
            .expect("unable to read latest offset from the log");
        f
    }

    pub fn read(&mut self, position: Offset, limit: ReadLimit) -> LogFuture<MessageBuf> {
        let (snd, f) = channel::<MessageBuf>();
        self.req_sink
            .send(ClientRequest::Read(position, limit, snd))
            .map_err(|_| ())
            .expect("unable to read from the log");
        f
    }
}

// TODO: remove replication-specific logic
pub struct ReplicatorAsyncLog<R> {
    req_sink: mpsc::UnboundedSender<LogRequest<R>>,
}

impl<R> Clone for ReplicatorAsyncLog<R> {
    fn clone(&self) -> ReplicatorAsyncLog<R> {
        ReplicatorAsyncLog {
            req_sink: self.req_sink.clone(),
        }
    }
}

impl<R> ReplicatorAsyncLog<R> {
    pub fn replicate_from(&mut self, offset: Offset) -> LogFuture<ReplicationSource<R>> {
        let (snd, f) = channel::<ReplicationSource<R>>();
        self.req_sink
            .send(LogRequest::Replica(ReplicaRequest::Replicate(offset, snd)))
            .map_err(|_| ())
            .expect("cannot sent replicate_from to the log");
        f
    }

    pub fn append_from_replication(&mut self, buf: Messages) -> LogFuture<OffsetRange> {
        let (snd, f) = channel::<OffsetRange>();
        self.req_sink
            .send(LogRequest::Replica(ReplicaRequest::AppendFromReplication(
                buf, snd,
            )))
            .map_err(|_| ())
            .expect("cannot sent append_from_replication to the log");
        f
    }

    pub fn last_offset(&mut self) -> LogFuture<Option<Offset>> {
        let (snd, f) = channel::<Option<Offset>>();
        self.req_sink
            .send(LogRequest::Client(ClientRequest::LastOffset(snd)))
            .map_err(|_| ())
            .expect("cannot send last_offset to the log");
        f
    }
}

/// Notifies a listener of log append operations.
pub trait AppendListener {
    /// Notifies the listener that the log has been mutated with the
    /// offset range specified.
    fn notify_append(&mut self, appended: &Messages);
}
