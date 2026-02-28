# ADR-005: Raft Consensus via openraft

- **Status**: Accepted
- **Date**: 2024-08-20
- **Authors**: FalconDB Core Team

## Context

FalconDB requires a consensus protocol for leader election and log replication
in multi-node deployments. The primary alternative to WAL-shipping replication
is a Raft-based approach that provides automatic leader election and failover
without external coordination.

### Options Considered

| Option | Pros | Cons |
|--------|------|------|
| **openraft** (0.9+) | Pure-logic Raft, async-native, no runtime coupling, MIT license, active maintenance | Complex generics; less battle-tested than etcd/raft |
| **raft-rs** (tikv) | Battle-tested in TiKV at massive scale | C-style API, sync-only, requires significant adaptation for async Rust |
| **Custom Raft** | Full control over wire format and optimizations | High engineering cost; correctness risk; maintenance burden |
| **etcd (external)** | Proven distributed coordination | External dependency; operational complexity; latency for metadata operations |

## Decision

Adopt **openraft** as the Raft implementation:

1. **Pure-logic design**: openraft separates the Raft state machine from I/O,
   making it straightforward to integrate with our async (tokio) runtime and
   custom storage engine.

2. **Trait-based integration**: we implement `RaftStorage` and `RaftNetwork`
   traits. The `RaftShardCoordinator` bridges Raft commits to
   `StorageEngine::apply()`.

3. **Transport**: gRPC via tonic (`GrpcNetworkFactory` / `RaftTransportService`)
   for multi-node clusters; in-process channels for unit tests.

4. **Dual mode**: openraft is optional — nodes can run in WAL-shipping mode
   (`role = primary/replica`) or Raft mode (`role = raft_member`). The
   `SingleNodeConsensus` stub provides a no-op default for standalone deployments.

## Consequences

### Positive

- Automatic leader election eliminates manual failover for Raft-mode clusters.
- `RaftFailoverWatcher` detects leader loss and triggers promotion automatically.
- Raft log ordering guarantees linearizable writes within a shard.
- Unit-testable: `InProcessTransport` enables deterministic Raft tests.

### Negative

- Two replication modes (WAL-shipping and Raft) increase configuration surface.
- openraft's generic-heavy API requires careful type wiring (`RaftGroup<C>`).
- Raft adds ~1 RTT latency vs async WAL-shipping for write commits.

### Mitigations

- Clear documentation: `role = raft_member` vs `role = primary/replica` in
  config guides and `falcon.toml` comments.
- Abstraction: `Consensus` trait unifies both modes; planner/executor are
  mode-agnostic.
- Performance: single-shard writes still use the fast path; Raft overhead only
  applies to the replication layer.

## References

- [openraft documentation](https://docs.rs/openraft)
- ARCHITECTURE.md §2.7 (Raft module)
- `crates/falcon_raft/` — implementation
- `crates/falcon_cluster/src/ha.rs` — RaftShardCoordinator integration
