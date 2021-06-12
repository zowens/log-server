use futures::Future;
use pin_project::pin_project;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub struct LogSender<T> {
    s: oneshot::Sender<Result<T, Error>>,
}

impl<T> LogSender<T> {
    #[inline]
    pub fn send(self, res: T) {
        self.s.send(Ok(res)).unwrap_or_default();
    }

    #[inline]
    pub fn send_err(self, e: Error) {
        self.s.send(Err(e)).unwrap_or_default();
    }

    #[inline]
    pub fn send_err_with(self, k: ErrorKind, e: &'static str) {
        self.s.send(Err(Error::new(k, e))).unwrap_or_default();
    }
}

/// `LogFuture` waits for a response from the `CommitLog`.
#[pin_project]
pub struct LogFuture<R> {
    #[pin]
    f: oneshot::Receiver<Result<R, Error>>,
}

impl<R> Future for LogFuture<R> {
    type Output = Result<R, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().f.poll(cx) {
            Poll::Ready(Ok(v)) => Poll::Ready(v),
            Poll::Ready(Err(e)) => {
                error!("Inner error {}", e);
                Poll::Ready(Err(Error::new(ErrorKind::Other, "Cancelled")))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn channel<T>() -> (LogSender<T>, LogFuture<T>) {
    let (s, f) = oneshot::channel::<Result<T, Error>>();
    (LogSender { s }, LogFuture { f })
}
