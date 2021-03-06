# Chain Replication

*Development Status*: Experimental

[Chain replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf) is a replication protocol designed for high throughput, 
fault tolerant systems to achieve both fast
replication as well as, in some implementations, strong consistency. A chain is able to recover quickly from failure
by bypassing failed nodes in the chain, and new nodes can be added with minimal disruption.

This implementation is an experiment to build chain replication with Rust and [Tokio](https://tokio.rs). The goal is to build
an underlying primitive on which specific systems can be built (e.g. key value store, search engine, state machine replication).

## Building/Running

Must be using Rust nightly.

```shell
cargo build --release

# start the management server
./target/release/management-server config/management.toml &

# start a head node
./target/release/storage-server config/head.toml &

# start a replica node
./target/release/storage-server config/middle.toml &

# start a tail node
./target/release/storage-serer config/tail.toml &

# start the CLI
./target/release/cli
```

The *benchit* tool allows a quick way to benchmark log insertion using multiple clients.

```shell
# Start 2 clients, 50 requests in flight per client
./target/release/benchit -c 2 -r 50
```


## Progress

- [ ] Chain Replication
    - [X] Replicated log
    - [X] Chained replication
    - [X] Multiplexed command protocol
    - [X] Replication protocol
    - [X] Tail replies
    - [ ] Tail queries
- [ ] Reconfiguration
    - [X] Modes
        - [X] Head node failure
        - [X] Tail node failure
        - [X] Middle node failure
    - [ ] Master Node
        - [X] Failure detector
        - [ ] Reconfiguration Protocol
        - [ ] Partitioning
        - [X] Chain reconfiguration
        - [ ] Backup Master Nodes (Requires consensus protocol)
- [ ] Framework Elements
    - [ ] Custom Commands
    - [ ] Tail node queries
- [ ] Optimizations
    - [X] Zero-Copy Transfer
- [ ] Other ideas
    - [ ] Kubernetes Operator
    - [ ] Complex chains
        - [ ] [Replex](https://www.cs.princeton.edu/~mfreed/docs/replex-atc16.pdf)
        - [ ] [HyperDex](https://www.cs.cornell.edu/people/egs/papers/hyperdex-sigcomm.pdf)
        - [ ] Multiple listeners
    - [ ] Multi-Data Center topology
    - [ ] Integration with non-replicated systems or poorly replicated systems
    - [ ] IoT offline mode (coalescing chains)
