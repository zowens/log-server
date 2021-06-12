use futures::{future::TryFuture, ready};
use pin_project::pin_project;
use rand::{distributions::Uniform, thread_rng, Rng};
use std::cmp::min;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::usize;
use tokio::time::{sleep, Sleep};

#[derive(Clone)]
pub struct RetryBehavior {
    max_retries: usize,
    jitter: bool,
    wait_duration: Duration,
    max_backoff: Duration,
    retry: usize,
}

impl RetryBehavior {
    pub const fn forever() -> RetryBehavior {
        RetryBehavior {
            max_retries: usize::MAX,
            retry: 0,
            jitter: true,
            wait_duration: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }

    #[allow(dead_code)]
    pub fn limited(retries: usize) -> RetryBehavior {
        RetryBehavior { max_retries: retries, ..Default::default() }
    }

    pub fn disable_jitter(mut self) -> RetryBehavior {
        self.jitter = false;
        self
    }

    pub fn max_backoff(mut self, max_backoff: Duration) -> RetryBehavior {
        self.max_backoff = max_backoff;
        self.wait_duration = min(self.wait_duration, max_backoff);
        self
    }

    pub fn initial_backoff(mut self, initial_backoff: Duration) -> RetryBehavior {
        self.wait_duration = min(initial_backoff, self.max_backoff);
        self
    }

    pub fn retry<F>(&self, supplier: Box<dyn Fn() -> F>) -> Retry<F>
    where
        F: TryFuture,
    {
        let future = supplier();
        Retry {
            behavior: self.clone(),
            supplier,
            state: RetryState::Running(future),
        }
    }

    fn take(&mut self) -> Option<Duration> {
        if self.retry >= self.max_retries {
            return None;
        }

        self.retry += 1;
        let mut duration = self.wait_duration;
        self.wait_duration = min(self.wait_duration * 2, self.max_backoff);

        if self.jitter {
            duration = thread_rng().sample(Uniform::new(Duration::from_millis(0), duration));
        }

        Some(duration)
    }
}

impl Default for RetryBehavior {
    fn default() -> RetryBehavior {
        RetryBehavior {
            max_retries: 3usize,
            retry: 0,
            jitter: true,
            wait_duration: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }
}

#[pin_project(project = RetryStatePinned, project_replace = RetryStateOwned)]
enum RetryState<F: TryFuture> {
    Running(#[pin] F),
    Waiting(#[pin] Sleep),
}

#[pin_project]
pub struct Retry<F: TryFuture> {
    behavior: RetryBehavior,
    supplier: Box<dyn Fn() -> F>,
    #[pin]
    state: RetryState<F>,
}

impl<F: Future> Future for Retry<F>
where
    F: TryFuture,
{
    // TODO: possible output an error stack??
    type Output = Result<F::Ok, F::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        let mut state = this.state;
        let next_state = match state.as_mut().project() {
            RetryStatePinned::Running(f) => {
                let e = match ready!(f.try_poll(cx)) {
                    Ok(v) => return Poll::Ready(Ok(v)),
                    Err(e) => e,
                };

                // do another retry, if possible
                if let Some(delay) = this.behavior.take() {
                    // trigger a rewake
                    cx.waker().wake_by_ref();

                    RetryState::Waiting(sleep(delay))
                } else {
                    return Poll::Ready(Err(e));
                }
            }
            RetryStatePinned::Waiting(f) => {
                ready!(f.poll(cx));
                RetryState::Running((this.supplier)())
            }
        };

        cx.waker().wake_by_ref();
        state.set(next_state);
        Poll::Pending
    }
}
