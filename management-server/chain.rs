use crate::config::FailureDetection;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::{spawn, task::JoinHandle, time::sleep};

/// Default number of nodes to be considered available
const DEFAULT_QUORUM: u32 = 2;

const WAIT_DURATION: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq)]
/// Metadata for a storage server node
pub struct Node {
    /// Identifier for the node
    pub id: u64,

    /// Internal address used for replication
    pub replication_address: String,

    /// Socket address for the client to storage server requests
    pub client_address: String,

    /// State of the node as determined by failure detection
    pub state: NodeState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeState {
    Joining { last_poll: Instant },
    Active { last_poll: Instant },
}

impl NodeState {
    fn is_joining(&self) -> bool {
        matches!(*self, NodeState::Joining { .. })
    }

    #[allow(unused)]
    fn is_active(&self) -> bool {
        matches!(*self, NodeState::Active { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChainState {
    // nodes that are known to be up-to-date
    active_nodes: Vec<Node>,
    quorum: u32,
    failure_detection: FailureDetection,
}

/// Result of a poll operation
#[derive(Debug, PartialEq, Eq)]
pub struct PollState {
    /// View on the current chain
    pub chain: ChainView,

    /// Duration the storage server is expected to wait
    pub repoll_duration_sec: i64,

    /// ID of the current node
    pub id: u64,
}

impl PollState {
    pub fn self_node(&self) -> &Node {
        let id = self.id;
        self.chain
            .active_nodes()
            .iter()
            .find(|n| n.id == id)
            .expect("Poll result contained inactive node")
    }
}

/// Readonly view of the chain state
#[derive(Debug, PartialEq, Eq)]
pub struct ChainView(ChainState);

impl ChainView {
    /// Flag indicating whether the chain has reached a quorum and
    /// can accept requests.
    pub fn has_active_chain(&self) -> bool {
        self.0.active_nodes.len() >= (self.0.quorum as usize)
    }

    /// Nodes in the active state
    pub fn active_nodes(&self) -> &[Node] {
        &self.0.active_nodes
    }

    /// Size of the chain to be considered in sync
    pub fn quorum(&self) -> u32 {
        self.0.quorum
    }
}

#[derive(PartialEq, Debug)]
pub enum PollError {
    NoNodeFound,
}

#[derive(PartialEq, Debug)]
pub enum JoinError {
    DuplicateNodeId,
}

#[derive(Clone)]
pub struct Chain {
    state: Arc<RwLock<ChainState>>,
}

impl Chain {
    /// Creates a new chain
    pub fn new(failure_detection: FailureDetection) -> Chain {
        Chain {
            state: Arc::new(RwLock::new(ChainState {
                active_nodes: Vec::new(),
                quorum: DEFAULT_QUORUM,
                failure_detection,
            })),
        }
    }

    pub fn spawn_failure_detector(&self) -> JoinHandle<()> {
        let chain = self.clone();
        spawn(async move {
            loop {
                sleep(WAIT_DURATION).await;
                chain.node_reaper();
            }
        })
    }

    /// Join a node to the cluster
    pub fn join(&self, mut node: Node) -> Result<PollState, JoinError> {
        let mut state = self.state.write().unwrap();

        let id = node.id;

        // find a server that matches join request
        // TODO: refactor this to account for non-active nodes
        let existing_server = state.active_nodes.iter().find_map(|n| {
            let rep_match = n.replication_address == node.replication_address;
            let id_match = n.id == node.id;
            if rep_match && id_match {
                Some(Ok(()))
            } else if id_match {
                Some(Err(JoinError::DuplicateNodeId))
            } else {
                None
            }
        });

        match existing_server {
            Some(Ok(_)) => {
                info!("Node {} has already joined", id);
            }
            None => {
                info!("Server joined {:?}", node);
                // TODO: add this node to the recovery state

                // figure out state of the node
                // - force into active state if no other nodes
                // - force into joining state if other nodes AND not already in joining state
                if state.active_nodes.is_empty() {
                    node.state = NodeState::Active {
                        last_poll: Instant::now(),
                    };
                } else if !node.state.is_joining() {
                    node.state = NodeState::Joining {
                        last_poll: Instant::now(),
                    }
                }
                state.active_nodes.push(node);
            }
            Some(Err(e)) => return Err(e),
        };

        let poll_duration = state.failure_detection.poll_duration_seconds as i64;
        Ok(PollState {
            chain: ChainView(state.clone()),
            id,
            repoll_duration_sec: poll_duration,
        })
    }

    /// Refreshes a nodes state within the cluster
    pub fn poll(&self, id: u64) -> Result<PollState, PollError> {
        debug!("Server poll {}", id);
        let mut state = self.state.write().unwrap();

        // TODO: add catchup mechanism where node reports whether
        // it is caughtup
        {
            // ensure node exists
            let node = state
                .active_nodes
                .iter_mut()
                .find(|state| state.id == id)
                .ok_or(PollError::NoNodeFound)?;

            node.state = NodeState::Active {
                last_poll: Instant::now(),
            };
        }

        let state = state.clone();
        let poll_duration = state.failure_detection.poll_duration_seconds as i64;
        Ok(PollState {
            chain: ChainView(state),
            id,
            repoll_duration_sec: poll_duration,
        })
    }

    /// Reads the current state of the chain
    pub fn read(&self) -> ChainView {
        ChainView(self.state.read().unwrap().clone())
    }

    /// Detects downed nodes
    pub fn node_reaper(&self) -> usize {
        let now = Instant::now();
        let mut state = self.state.write().unwrap();
        let grace_duration = Duration::from_secs(
            state.failure_detection.poll_duration_seconds
                + state.failure_detection.failed_poll_duration_seconds,
        );

        let mut failed_nodes = state
            .active_nodes
            .iter()
            .enumerate()
            .filter_map(|(i, n)| {
                // TODO: don't nuke joining nodes??
                match n.state {
                    NodeState::Joining { last_poll } if last_poll + grace_duration < now => Some(i),
                    NodeState::Active { last_poll } if last_poll + grace_duration < now => Some(i),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        failed_nodes.reverse();

        // check to see if we're over the threshold for number of downed nodes
        let fail_factor = 0.01f32 * state.failure_detection.percent_maximum_failed_nodes as f32;
        if failed_nodes.len() as f32 > fail_factor * state.active_nodes.len() as f32 {
            info!("{} nodes have failed, which is over the percentage threshold for maximum failing nodes",
                  failed_nodes.len());
            return 0;
        }

        for i in &failed_nodes {
            let node = state.active_nodes.remove(*i);
            info!("Node {:?} failed", node);
        }
        failed_nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLL_DURATION_SECONDS: i64 = 2;
    const FAILURE_DETECTION: FailureDetection = FailureDetection {
        poll_duration_seconds: 2,
        failed_poll_duration_seconds: 3,
        percent_maximum_failed_nodes: 50,
    };

    fn chain_order(chain: &ChainView) -> Vec<u64> {
        chain.active_nodes().iter().map(|n| n.id).collect()
    }

    #[test]
    fn test_join() {
        let chain = Chain::new(FAILURE_DETECTION.clone());

        // join first node
        {
            let config = chain.join(join_req("a", 0)).unwrap();
            assert_eq!(DEFAULT_QUORUM, config.chain.quorum());
            assert_eq!(1, config.chain.active_nodes().len());
            assert_eq!(POLL_DURATION_SECONDS, config.repoll_duration_sec);
            assert_eq!(0, config.self_node().id);
            assert_eq!("a", config.self_node().replication_address);
            assert_eq!(0, config.chain.active_nodes()[0].id);
            assert!(config.self_node().state.is_active())
        }

        // join second node
        {
            let config = chain.join(join_req("b", 1)).unwrap();
            assert_eq!(DEFAULT_QUORUM, config.chain.quorum());
            assert_eq!(2, config.chain.active_nodes().len());
            assert_eq!(POLL_DURATION_SECONDS, config.repoll_duration_sec);
            assert_eq!(1, config.self_node().id);
            assert_eq!("b", config.self_node().replication_address);
            assert_eq!(0, config.chain.active_nodes()[0].id);
            assert_eq!(1, config.chain.active_nodes()[1].id);
            assert!(config.self_node().state.is_joining());
            assert!(config.chain.has_active_chain());
        }
    }

    #[test]
    fn test_double_join() {
        let chain = Chain::new(FAILURE_DETECTION.clone());
        let config = chain.join(join_req("a", 0)).unwrap();
        let config2 = chain.join(join_req("a", 0)).unwrap();
        // TODO: assert epoch same
        assert_eq!(0, config.self_node().id);
        assert_eq!(0, config2.self_node().id);
        assert_eq!(config, config2);
    }

    #[test]
    fn test_duplicate_id_join() {
        let chain = Chain::new(FAILURE_DETECTION.clone());
        let config = chain.join(join_req("a", 0)).unwrap();
        assert_eq!(0, config.self_node().id);
        let config2 = chain.join(join_req("b", 0));
        assert_eq!(Some(JoinError::DuplicateNodeId), config2.err());
    }

    #[test]
    fn test_poll_unknown_node() {
        let chain = Chain::new(FAILURE_DETECTION.clone());
        assert_eq!(Some(PollError::NoNodeFound), chain.poll(123).err());
    }

    #[test]
    fn test_poll_nodes() {
        let chain = Chain::new(FAILURE_DETECTION.clone());
        // join 3 nodes
        let a = chain.join(join_req("a", 0)).unwrap();
        assert_eq!(0, a.self_node().id);
        assert_eq!(1, a.chain.active_nodes().len());
        let b = chain.join(join_req("b", 1)).unwrap();
        assert_eq!(1, b.self_node().id);
        assert_eq!(2, b.chain.active_nodes().len());
        let c = chain.join(join_req("c", 2)).unwrap();
        assert_eq!(2, c.self_node().id);
        assert_eq!(3, c.chain.active_nodes().len());

        let final_chain = c.chain;

        {
            let c_poll = chain.poll(2).unwrap();
            assert_eq!(2, c_poll.self_node().id);
            assert_eq!("c", c_poll.self_node().replication_address);
            assert_eq!(chain_order(&c_poll.chain), chain_order(&final_chain));
            assert_eq!(POLL_DURATION_SECONDS, c_poll.repoll_duration_sec);
        }

        {
            let b_poll = chain.poll(1).unwrap();
            assert_eq!(1, b_poll.self_node().id);
            assert_eq!("b", b_poll.self_node().replication_address);
            assert_eq!(chain_order(&b_poll.chain), chain_order(&final_chain));
            assert_eq!(POLL_DURATION_SECONDS, b_poll.repoll_duration_sec);
        }

        {
            let a_poll = chain.poll(0).unwrap();
            assert_eq!(0, a_poll.self_node().id);
            assert_eq!("a", a_poll.self_node().replication_address);
            assert_eq!(chain_order(&a_poll.chain), chain_order(&final_chain));
            assert_eq!(POLL_DURATION_SECONDS, a_poll.repoll_duration_sec);
        }
    }

    #[test]
    fn test_snapshot() {
        let chain = Chain::new(FAILURE_DETECTION.clone());

        // no head/tail
        let snapshot = chain.read();
        assert_eq!(0, snapshot.active_nodes().len());
        assert!(!snapshot.has_active_chain());

        chain.join(join_req("a", 0)).unwrap();
        let snapshot = chain.read();
        assert_eq!(1, snapshot.active_nodes().len());
        assert!(!snapshot.has_active_chain());

        chain.join(join_req("b", 1)).unwrap();
        chain.join(join_req("c", 2)).unwrap();

        // configuration = a -> b -> c
        let snapshot = chain.read();
        assert!(snapshot.has_active_chain());
        assert_eq!(3, snapshot.active_nodes().len());
        assert_eq!(0, snapshot.active_nodes()[0].id);
        assert_eq!("a", snapshot.active_nodes()[0].client_address);
        assert_eq!(1, snapshot.active_nodes()[1].id);
        assert_eq!("b", snapshot.active_nodes()[1].client_address);
        assert_eq!(2, snapshot.active_nodes()[2].id);
        assert_eq!("c", snapshot.active_nodes()[2].client_address);
    }

    #[test]
    fn test_reap_no_connections() {
        let chain = Chain::new(FAILURE_DETECTION.clone());

        // join the nodes
        for i in 0u64..3u64 {
            chain.join(join_req(&format!("n{}", i), i)).unwrap();
            chain.poll(i).unwrap();
        }

        let snapshot = chain.read();
        assert_eq!(vec![0, 1, 2], chain_order(&snapshot));

        // reap the connections
        assert_eq!(0, chain.node_reaper());

        let snapshot = chain.read();
        assert_eq!(vec![0, 1, 2], chain_order(&snapshot));
    }

    #[test]
    fn test_reap_failed_connection() {
        let chain = Chain::new(FAILURE_DETECTION.clone());
        let now = Instant::now();

        // join the nodes
        for i in 0u64..3u64 {
            chain.join(join_req(&format!("n{}", i), i)).unwrap();
            chain.poll(i).unwrap();
        }

        let snapshot = chain.read();
        assert_eq!(vec![0, 1, 2], chain_order(&snapshot));

        // back-date the node 1's poll time
        {
            let mut state = chain.state.write().unwrap();
            state.active_nodes[1].state = NodeState::Active {
                last_poll: now - Duration::from_secs(7),
            };
        }

        // reap the connections
        assert_eq!(1, chain.node_reaper());

        let snapshot = chain.read();
        assert_eq!(vec![0, 2], chain_order(&snapshot));
    }

    #[test]
    fn test_reap_prevent_total_failure() {
        let chain = Chain::new(FAILURE_DETECTION.clone());
        let now = Instant::now();

        // join the nodes
        for i in 0u64..3u64 {
            chain.join(join_req(&format!("n{}", i), i)).unwrap();
            chain.poll(i).unwrap();
        }

        let snapshot = chain.read();
        assert_eq!(vec![0, 1, 2], chain_order(&snapshot));

        // back-date the first 2 node's poll time
        // this is > 50% threashold for failures
        {
            let mut state = chain.state.write().unwrap();
            for i in 0usize..2 {
                state.active_nodes[i].state = NodeState::Active {
                    last_poll: now - Duration::from_secs(7),
                };
            }
        }

        // reap the connections
        assert_eq!(0, chain.node_reaper());

        let snapshot = chain.read();
        assert_eq!(vec![0, 1, 2], chain_order(&snapshot));
    }

    fn join_req(addr: &str, id: u64) -> Node {
        Node {
            id,
            replication_address: addr.to_string(),
            client_address: addr.to_string(),
            state: NodeState::Joining {
                last_poll: Instant::now(),
            },
        }
    }
}
