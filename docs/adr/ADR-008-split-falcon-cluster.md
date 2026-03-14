# ADR-008: Split falcon_cluster into Focused Crates

## Status: **Proposed**

## Context

`falcon_cluster` is the largest crate in the workspace (~1.4 MB source, 43+ modules).
It mixes HA/failover, sharding/2PC, replication transport, enterprise ops, and
distributed query execution. This slows compilation, increases coupling, and makes
ownership boundaries unclear.

## Decision

Split `falcon_cluster` into four focused crates:

### 1. `falcon_cluster_ha` — HA, failover, safety hardening

| Current module | Bytes |
|----------------|-------|
| `ha.rs` | 47 KB |
| `failover_txn_hardening.rs` | 55 KB |
| `indoubt_resolver.rs` | 34 KB |
| `dist_hardening.rs` | 49 KB |
| `stability_hardening.rs` | 56 KB |
| `determinism_hardening.rs` | 47 KB |
| `circuit_breaker.rs` | 16 KB |
| `fault_injection.rs` | 33 KB |

### 2. `falcon_cluster_sharding` — Sharding, 2PC, distributed execution

| Current module | Bytes |
|----------------|-------|
| `sharding.rs` | 24 KB |
| `sharded_engine.rs` | 5 KB |
| `two_phase.rs` | 23 KB |
| `deterministic_2pc.rs` | 34 KB |
| `cross_shard.rs` | 67 KB |
| `distributed_exec/` | ~120 KB |
| `query_engine.rs` | 18 KB |
| `rebalancer.rs` | 55 KB |
| `raft_rebalance.rs` | 20 KB |
| `routing/` | ~30 KB |

### 3. `falcon_cluster_replication` — WAL shipping, gRPC transport, Raft bridge

| Current module | Bytes |
|----------------|-------|
| `replication/` | ~80 KB |
| `grpc_transport.rs` | 34 KB |
| `raft_integration.rs` | 34 KB |
| `segment_streaming.rs` | 54 KB |

### 4. `falcon_cluster_ops` — Enterprise ops, self-healing, gateway

| Current module | Bytes |
|----------------|-------|
| `self_healing.rs` | 82 KB |
| `smart_gateway.rs` | 66 KB |
| `client_discovery.rs` | 51 KB |
| `cluster_lifecycle.rs` | 20 KB |
| `cluster_ops.rs` | 32 KB |
| `cost_capacity.rs` | 52 KB |
| `ga_hardening.rs` | 55 KB |
| `sla_admission.rs` | 37 KB |
| `distributed_enhancements.rs` | 66 KB |
| `admission.rs` | 25 KB |
| `token_bucket.rs` | 15 KB |
| `bg_supervisor.rs` | 10 KB |
| `security_hardening.rs` | 36 KB |
| `cluster/` (membership) | ~30 KB |
| `gateway.rs` | 11 KB |

## Migration Strategy

1. **Phase A** (non-breaking): Reorganize modules into sub-directories matching
   the target crate boundaries. Keep everything in `falcon_cluster`.
2. **Phase B**: Extract `falcon_cluster_replication` first (fewest reverse deps).
3. **Phase C**: Extract `falcon_cluster_ha` (depends on replication types).
4. **Phase D**: Extract `falcon_cluster_sharding`.
5. **Phase E**: Rename residual `falcon_cluster` → `falcon_cluster_ops`.

Each phase is a separate PR with passing CI.

## Dependency Graph (target)

```text
falcon_cluster_ops
  ├── falcon_cluster_ha
  │     └── falcon_cluster_replication
  └── falcon_cluster_sharding
        └── falcon_cluster_replication
```

## Consequences

- **Faster incremental builds** — changing HA code doesn't recompile sharding.
- **Clearer CODEOWNERS** — each crate maps to a team.
- **Smaller review surface** — PRs touch one crate, not a 1.4 MB monolith.
- **Migration risk** — re-export façade in `falcon_cluster` during transition
  keeps downstream breakage minimal.
