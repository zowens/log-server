use super::{
    bufpool::BytesPool,
    messages::{MessagesMut, SingleMessage},
};
use commitlog::message::MessageSet;
use futures::{ready, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

pub struct BatchMessageStream {
    receiver: mpsc::UnboundedReceiver<SingleMessage>,
    buf_pool: BytesPool,
    blocked_message: Option<SingleMessage>,
}

impl BatchMessageStream {
    pub fn new(
        receiver: mpsc::UnboundedReceiver<SingleMessage>,
        buf_pool: BytesPool,
    ) -> BatchMessageStream {
        BatchMessageStream {
            receiver,
            buf_pool,
            blocked_message: None,
        }
    }
}

impl Stream for BatchMessageStream {
    type Item = MessagesMut;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // utilize a buffered message or pull one from the stream
        let (client, req, payload) = if let Some(m) = this.blocked_message.take() {
            m
        } else if let Some(m) = ready!(this.receiver.poll_recv(cx)) {
            m
        } else {
            return Poll::Ready(None);
        };

        // initialize a buffer from the byte pool
        let (mut buf, capacity) = {
            let msgs: MessagesMut = this.buf_pool.take().into();
            (msgs, this.buf_pool.buffer_capacity())
        };

        // try to push the first message
        if rare!(buf.push(client, req, &payload).is_err()) {
            warn!(
                "Ignoring message clientId={}, reqId={} due to size {} > buffer capacity {}",
                client,
                req,
                payload.len(),
                capacity
            );
        }

        // add more messages to the buffer, up to the capacity
        while let Poll::Ready(Some((client, req, payload))) = this.receiver.poll_recv(cx) {
            if rare!(payload.len() > capacity) {
                warn!(
                    "Ignoring message clientId={}, reqId={} due to size {} > buffer capacity {}",
                    client,
                    req,
                    payload.len(),
                    capacity
                );
                continue;
            }

            // try to push the message, if there is no capacity
            // save the message for another round of poll
            if rare!(buf.push(client, req, &payload).is_err()) {
                debug!(
                    "Buffer is full, corking message clientId={}, reqId={}",
                    client, req
                );

                assert!(this.blocked_message.is_none());
                self.blocked_message.replace((client, req, payload));
                break;
            }
        }
        debug!("Done batching messages");

        if rare!(buf.bytes().is_empty()) {
            debug!("No messages in the buffer, returning to the pool");
            self.buf_pool.push(buf.into_inner());
            Poll::Pending
        } else {
            Poll::Ready(Some(buf))
        }
    }
}
