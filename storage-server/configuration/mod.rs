use crate::config::Config;
use crate::protocol;
use crate::retry::{Retry, RetryBehavior};
use futures::{ready, Future};
use grpcio::{ChannelBuilder, EnvBuilder};
use pin_project::pin_project;
use rand::{thread_rng, Rng};
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::task::{Context, Poll};
use std::time::Duration;

const DEFAULT_WAIT_DURATION: Duration = Duration::from_secs(5);

pub struct NodeConfiguration<'a> {
    config: RwLockReadGuard<'a, protocol::NodeConfiguration>,
}

impl<'a> NodeConfiguration<'a> {
    #[allow(dead_code)]
    pub fn is_head(&self) -> bool {
        assert!(self.config.quorum > 0);
        assert!(self.config.has_self_node());

        let id = self.config.get_self_node().id;
        self.config.active_chain.len() >= self.config.quorum as usize
            && id == self.config.active_chain.iter().next().unwrap().id
    }

    #[allow(dead_code)]
    pub fn is_tail(&self) -> bool {
        assert!(self.config.quorum > 0);
        assert!(self.config.has_self_node());

        let id = self.config.get_self_node().id;
        self.config.active_chain.len() >= self.config.quorum as usize
            && id == self.config.active_chain.iter().last().unwrap().id
    }

    pub fn upstream_addr(&self) -> Option<SocketAddr> {
        assert!(self.config.has_self_node());
        let id = self.config.get_self_node().id;
        let index = self
            .config
            .active_chain
            .iter()
            .enumerate()
            .filter_map(|(ind, n)| if n.id == id { Some(ind) } else { None })
            .next();

        match index {
            Some(0) => None,
            None => None,
            Some(i) => {
                assert!(i > 0);
                let upstream_node = &self.config.active_chain[i - 1];
                upstream_node
                    .replication_address
                    .as_str()
                    .to_socket_addrs()
                    .ok()
                    .and_then(|sock_addrs| sock_addrs.into_iter().next())
            }
        }
    }

    pub fn wait_duration(&self) -> Duration {
        if self.config.has_poll_wait() {
            let poll = self.config.get_poll_wait();
            Duration::new(poll.seconds as u64, poll.nanos as u32)
        } else {
            DEFAULT_WAIT_DURATION
        }
    }
}

#[derive(Clone)]
pub struct NodeManager {
    current_config: Arc<RwLock<protocol::NodeConfiguration>>,
    client: protocol::ConfigurationClient,
}

fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<NodeManager>();
}

impl NodeManager {
    fn new(
        config: protocol::NodeConfiguration,
        client: protocol::ConfigurationClient,
    ) -> NodeManager {
        NodeManager {
            current_config: Arc::new(RwLock::new(config)),
            client,
        }
    }

    pub fn current(&self) -> NodeConfiguration {
        NodeConfiguration {
            config: self.current_config.read().unwrap(),
        }
    }

    pub fn repoll(&self) -> NodeConfigFuture {
        let node_id = {
            let config = self.current_config.read().unwrap();
            config.get_self_node().get_id()
        };

        let client = self.client.clone();
        NodeConfigFuture {
            current_config: self.current_config.clone(),
            future: RetryBehavior::forever().retry(Box::new(move || {
                let mut poll_req = protocol::PollRequest::new();
                poll_req.set_node_id(node_id);
                client.poll_async(&poll_req).unwrap()
            })),
        }
    }
}

#[pin_project]
pub struct NodeConfigFuture {
    current_config: Arc<RwLock<protocol::NodeConfiguration>>,
    #[pin]
    future: Retry<grpcio::ClientUnaryReceiver<protocol::NodeConfiguration>>,
}

impl Future for NodeConfigFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        match ready!(this.future.poll(cx)) {
            Ok(config) => {
                let mut v = this.current_config.write().unwrap();
                *v = config;
                Poll::Ready(())
            }
            Err(_) => unreachable!("This future is retried forever"),
        }
    }
}

#[pin_project(project_replace)]
pub struct ClusterJoin {
    #[pin]
    future: Retry<grpcio::ClientUnaryReceiver<protocol::NodeConfiguration>>,
    client: protocol::ConfigurationClient,
}

impl ClusterJoin {
    pub fn new(config: &Config) -> ClusterJoin {
        let config = config.clone();

        let cb = ChannelBuilder::new(Arc::new(EnvBuilder::new().build()))
            .default_compression_algorithm(grpcio::CompressionAlgorithms::GRPC_COMPRESS_GZIP)
            .max_concurrent_stream(100)
            .http2_bdp_probe(true);
        let conn = cb.connect(&config.management.management_server_addr);

        let client = protocol::ConfigurationClient::new(conn);
        let retry_behavior = RetryBehavior::forever()
            .max_backoff(Duration::from_secs(4))
            .initial_backoff(Duration::from_millis(500))
            .disable_jitter();

        ClusterJoin {
            client: client.clone(),
            future: retry_behavior.retry(Box::new(move || {
                let mut join_req = protocol::JoinRequest::new();
                join_req.set_replication_address(config.replication.server_addr.to_string());
                join_req.set_client_address(config.frontend.server_addr.to_string());

                // pick a random node on every retry to prevent duplicates
                join_req.set_node_id(thread_rng().gen());

                client.join_async(&join_req).unwrap()
            })),
        }
    }
}

impl Future for ClusterJoin {
    type Output = NodeManager;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<NodeManager> {
        let this = self.project();
        match ready!(this.future.poll(cx)) {
            Ok(config) => {
                info!("Joined cluster. id={}", config.get_self_node().get_id());
                Poll::Ready(NodeManager::new(config, this.client.clone()))
            }
            Err(_) => unreachable!("Configured for forever retries"),
        }
    }
}
