use crate::chain::{Chain, ChainView, JoinError, Node, NodeState, PollError, PollState};
use crate::protocol;
use futures::FutureExt;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use protobuf::well_known_types::Duration;
use std::time::Instant;

#[derive(Clone)]
pub struct ManagementService(pub Chain);

fn map_client_config(view: ChainView) -> protocol::ClientConfiguration {
    trace!("Snapshot called");
    let mut config = protocol::ClientConfiguration::new();
    if view.has_active_chain() {
        config.set_nodes(
            view.active_nodes()
                .iter()
                .map(|n| {
                    let mut client_node = protocol::ClientNode::new();
                    client_node.set_id(n.id);
                    client_node.set_client_address(n.client_address.clone());
                    client_node
                })
                .collect(),
        );
    }

    config
}

fn to_node_configuration(poll_result: PollState) -> protocol::NodeConfiguration {
    let mut config = protocol::NodeConfiguration::new();
    config.set_quorum(poll_result.chain.quorum());

    {
        let self_node = poll_result.self_node();
        let node = config.mut_self_node();
        node.set_id(self_node.id);
        node.set_replication_address(self_node.replication_address.clone());
    }

    config.set_active_chain(
        poll_result
            .chain
            .active_nodes()
            .iter()
            .map(|chain_node| {
                let mut n = protocol::Node::new();
                n.set_id(chain_node.id);
                n.set_replication_address(chain_node.replication_address.clone());
                n
            })
            .collect(),
    );

    let mut poll_duration = Duration::new();
    poll_duration.set_seconds(poll_result.repoll_duration_sec);
    config.set_poll_wait(poll_duration);

    config
}

impl protocol::Configuration for ManagementService {
    fn join(
        &mut self,
        ctx: RpcContext,
        mut req: protocol::JoinRequest,
        sink: UnarySink<protocol::NodeConfiguration>,
    ) {
        let node = Node {
            id: req.get_node_id(),
            replication_address: req.take_replication_address(),
            client_address: req.take_client_address(),
            state: NodeState::Joining {
                last_poll: Instant::now(),
            },
        };
        match self.0.join(node).map(to_node_configuration) {
            Ok(config) => ctx.spawn(sink.success(config).map(|_| ())),
            Err(JoinError::DuplicateNodeId) => {
                let status = RpcStatus::new(
                    RpcStatusCode::ALREADY_EXISTS,
                    Some("Server ID duplicated".to_string()),
                );
                ctx.spawn(sink.fail(status).map(|_| ()));
            }
        }
    }

    fn poll(
        &mut self,
        ctx: RpcContext,
        req: protocol::PollRequest,
        sink: UnarySink<protocol::NodeConfiguration>,
    ) {
        match self.0.poll(req.node_id).map(to_node_configuration) {
            Ok(config) => {
                ctx.spawn(sink.success(config).map(|_| ()));
            }
            Err(PollError::NoNodeFound) => {
                let status = RpcStatus::new(
                    RpcStatusCode::INVALID_ARGUMENT,
                    Some("Server ID not found".to_string()),
                );
                ctx.spawn(sink.fail(status).map(|_| ()));
            }
        }
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext,
        _req: protocol::ClientNodeRequest,
        sink: UnarySink<protocol::ClientConfiguration>,
    ) {
        ctx.spawn(sink.success(map_client_config(self.0.read())).map(|_| ()));
    }
}
