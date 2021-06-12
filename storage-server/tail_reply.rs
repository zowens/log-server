use crate::asynclog::AppendListener;
use crate::asynclog::Messages;
use byteorder::{ByteOrder, LittleEndian};
use commitlog::message::MessageSet;
use fnv::FnvHashMap;
use futures::{ready, Future, Stream};
use std::collections::hash_map;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::sync::mpsc;

// TODO: bound sending
type ReplySender = mpsc::UnboundedSender<Vec<u64>>;

/// Stream of replies for a single client
pub struct ReplyStream(mpsc::UnboundedReceiver<Vec<u64>>);

impl Stream for ReplyStream {
    type Item = Vec<u64>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Vec<u64>>> {
        self.0.poll_recv(cx)
    }
}

/// Listener for `AsyncLog` appends
pub struct TailReplyListener {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl AppendListener for TailReplyListener {
    fn notify_append(&mut self, append: &Messages) {
        self.sender
            .send(TailReplyMsg::Notify(append.clone()))
            .unwrap_or_default();
    }
}

/// Registrar for client for notifications
#[derive(Clone)]
pub struct TailReplyRegistrar {
    sender: mpsc::UnboundedSender<TailReplyMsg>,
}

impl TailReplyRegistrar {
    /// Listens for for client changes
    pub fn listen(&self, client_id: u64) -> ReplyStream {
        let (snd, recv) = mpsc::unbounded_channel();
        self.sender
            .send(TailReplyMsg::Register(client_id, snd))
            .unwrap_or_default();
        ReplyStream(recv)
    }
}

enum TailReplyMsg {
    Register(u64, ReplySender),
    Notify(Messages),
}

/// Opens a listener and tail reply pair
pub fn new() -> (TailReplyListener, TailReplyRegistrar) {
    let (sender, receiver) = mpsc::unbounded_channel();

    spawn(TailReplySender {
        receiver,
        registered: FnvHashMap::default(),
    });

    let listener = TailReplyListener {
        sender: sender.clone(),
    };
    let registrar = TailReplyRegistrar { sender };
    (listener, registrar)
}

struct TailReplySender {
    receiver: mpsc::UnboundedReceiver<TailReplyMsg>,
    registered: FnvHashMap<u64, ReplySender>,
}

impl TailReplySender {
    fn notify_clients(&mut self, append_set: Messages) {
        let mut req_batches: FnvHashMap<u64, Vec<u64>> = FnvHashMap::default();

        // batch by client_id
        for msg in append_set.iter() {
            let bytes = msg.metadata();
            if bytes.len() != 16 {
                warn!("Invalid log entry appended");
                continue;
            }

            let client_id = LittleEndian::read_u64(&bytes[0..8]);
            if self.registered.contains_key(&client_id) {
                let client_req_id = LittleEndian::read_u64(&bytes[8..16]);
                let reqs = req_batches.entry(client_id).or_insert_with(Vec::new);
                reqs.push(client_req_id);
            }
        }

        // notify the clients
        for (client_id, client_req_ids) in req_batches {
            if let hash_map::Entry::Occupied(mut entry) = self.registered.entry(client_id) {
                let send_res = entry.get_mut().send(client_req_ids);
                match send_res {
                    Ok(_) => {
                        trace!("Tail reply sent to client {}", client_id);
                    }
                    Err(_) => {
                        trace!("Tail dropped");
                        entry.remove();
                    }
                }
            }
        }
    }
}

impl Future for TailReplySender {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match ready!(self.receiver.poll_recv(cx)) {
                Some(TailReplyMsg::Register(client_id, listener)) => {
                    trace!("Registered client {}", client_id);
                    self.registered.insert(client_id, listener);
                }
                Some(TailReplyMsg::Notify(append_set)) => {
                    self.notify_clients(append_set);
                }
                None => {
                    warn!("Tail reply stream completed");
                    return Poll::Ready(());
                }
            }
        }
    }
}
