use super::log_reader::FileSlice;
use super::poll::UpstreamReplication;
use crate::asynclog::ReplicatorAsyncLog;
use crate::configuration::{NodeConfigFuture, NodeManager};
use futures::ready;
use futures::{pin_mut, stream::StreamExt, Future, Stream};
use pin_project::pin_project;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{sleep, Sleep};

pub struct ReplicationController {
    manager: NodeManager,
    log: ReplicatorAsyncLog<FileSlice>,
}

impl ReplicationController {
    pub fn new(manager: NodeManager, log: ReplicatorAsyncLog<FileSlice>) -> ReplicationController {
        ReplicationController { manager, log }
    }

    pub async fn run(self) {
        let reassignment_stream = Repoll::new(&self.manager);
        pin_mut!(reassignment_stream);

        let mut log = self.log;

        loop {
            // wait for an assignment for the upstream node
            //
            // A head node will continuously poll until there is a node
            // that is upstream of this node
            while self.manager.current().upstream_addr().is_none() {
                reassignment_stream.next().await;
            }

            // primary loop
            let upstream_addr = self.manager.current().upstream_addr().unwrap();

            let replication_future = UpstreamReplication::new(&upstream_addr, &mut log).replicate();
            pin_mut!(replication_future);
            tokio::select! {
                // check for reassignment
                _ = reassignment_stream.next() => {
                    info!("Replication changed");
                }
                // drive the replication
                _ = &mut replication_future => {
                    panic!("Unreachable");
                }
            }
        }
    }
}

#[pin_project]
struct Repoll {
    manager: NodeManager,
    prev_upstream: Option<SocketAddr>,

    #[pin]
    state: RepollState,
}

#[pin_project(project = RepollStateProj, project_replace = RepollStateProjOwned)]
enum RepollState {
    Delay(#[pin] Sleep),
    RequestConfig(#[pin] NodeConfigFuture),
}

impl Repoll {
    fn new(manager: &NodeManager) -> Repoll {
        let delay = manager.current().wait_duration();
        Repoll {
            manager: manager.clone(),
            prev_upstream: None,
            state: RepollState::Delay(sleep(delay)),
        }
    }

    fn poll_next_state(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<(RepollState, bool)> {
        let this = self.as_mut().project();
        let state = match this.state.project() {
            RepollStateProj::Delay(sleep) => {
                ready!(sleep.poll(cx));
                (RepollState::RequestConfig(this.manager.repoll()), false)
            }
            RepollStateProj::RequestConfig(config_future) => {
                ready!(config_future.poll(cx));
                let prev_upstream = *this.prev_upstream;
                *this.prev_upstream = this.manager.current().upstream_addr();
                (
                    RepollState::Delay(sleep(this.manager.current().wait_duration())),
                    prev_upstream == *this.prev_upstream,
                )
            }
        };
        Poll::Ready(state)
    }
}

impl Stream for Repoll {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        let (next_state, reconfigured) = {
            let this = self.as_mut();
            ready!(this.poll_next_state(cx))
        };
        self.as_mut().project().state.set(next_state);

        // only send an update when the upstream changes
        if reconfigured {
            Poll::Ready(Some(()))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
