# Sharding & Distributed Execution Design

## Overview

FalconDB distributes data across multiple shards using hash-based partitioning on
the primary key. Each shard is a self-contained unit of storage, transaction
management, and replication. The distributed layer routes queries to the correct
shard(s) and coordinates cross-shard transactions.

## Goals

1. **Horizontal scalability**: add nodes to increase capacity linearly.
2. **Single-shard fast path**: avoid 2PC overhead for the common case.
3. **Cross-shard correctness**: XA-2PC for multi-shard transactions.
4. **Transparent routing**: clients connect to any node; the planner routes.
5. **Bounded disruption**: shard migration does not block unrelated queries.

## Shard Model

### Hash Partitioning

```
shard_id = hash(pk_bytes) % num_shards
```

- **Hash function**: xxHash3 (xxhash-rust) — fast, well-distributed.
- **Partition key**: the table's primary key columns, serialized via `encode_pk`.
- **Deterministic**: same PK always maps to the same shard, regardless of node.

### Shard Map

The `ShardMap` is the authoritative mapping from shard IDs to node assignments:

```
ShardMap {
    num_shards: 16,          // power of 2 for bitmask optimization
    assignments: [
        Shard 0  → Node A (primary), Node B (replica)
        Shard 1  → Node A (primary), Node C (replica)
        Shard 2  → Node B (primary), Node A (replica)
        ...
    ],
    version: 42,             // monotonically increasing epoch
}
```

- **Epoch versioning**: every shard map change increments the epoch.
- **Epoch fencing**: stale shard map operations are rejected with `EpochFence` error.
- **Dissemination**: gossip-style push to all nodes on change.

### Shard Replica Group

Each shard has a `ShardReplicaGroup`:

| Role | Count | Responsibilities |
|------|-------|-----------------|
| Primary | 1 | Read + write; WAL durable commit |
| Replica | 0–N | Read-only; WAL apply; failover candidate |

## Query Routing

### Planner Integration

The query planner annotates each physical plan node with a `ShardTarget`:

```
PhysicalPlan::SeqScan {
    table: "orders",
    shard_target: ShardTarget::Single(3),  // PK filter resolved to shard 3
}
```

For queries without a PK filter, the target is `ShardTarget::All`.

### Single-Shard Fast Path

When the planner determines all accessed shards are the same:

```
Client → PG Wire → SQL Frontend → Planner
    → ShardTarget::Single(N)
    → LocalTxn (no 2PC)
    → Direct MemTable access on shard N
    → Commit (WAL fsync only)
    → ACK
```

- **TxnPath**: `Fast`
- **Latency**: ~1 WAL fsync round-trip
- **No coordination overhead**

### Cross-Shard Slow Path

When multiple shards are involved:

```
Client → PG Wire → SQL Frontend → Planner
    → ShardTarget::Multi([2, 5, 11])
    → GlobalTxn (XA-2PC)
    → Phase 1 (Prepare): parallel prepare on shards 2, 5, 11
    → Phase 2 (Commit): parallel commit on all prepared shards
    → ACK
```

- **TxnPath**: `Slow`
- **Latency**: 2× WAL fsync round-trips (prepare + commit)
- **Atomicity**: if any shard fails to prepare, all are aborted

### Automatic Path Classification

The planner emits routing hints. The transaction manager observes actual shard
involvement at runtime:

```rust
txn_context.observe_involved_shards(&shard_set);
// If shard_set.len() > 1, auto-upgrade LocalTxn → GlobalTxn
```

Hard invariant: `TxnContext::validate_commit_invariants()` returns
`InvariantViolation` (not debug_assert) if a LocalTxn touches multiple shards.

## Distributed Query Execution

### Scatter-Gather

For cross-shard reads (SELECT without PK filter):

```
Coordinator
  ├── Scatter: send sub-query to shard 0, 1, 2, ..., N-1
  │     (parallel, via DistributedQueryEngine)
  ├── Gather: collect partial results from all shards
  │     └── Merge / sort / aggregate as needed
  └── Return merged result to client
```

### Failure Policy

| Policy | Behavior |
|--------|----------|
| `Strict` | Fail entire query if any shard is unreachable |
| `BestEffort` | Return partial results; mark missing shards in response |

### Memory Limits

`GatherLimits` caps the total memory used by a single scatter-gather operation
to prevent OOM from large cross-shard queries.

## Replication per Shard

Each shard's primary streams WAL records to its replicas independently:

```
Shard 0 Primary (Node A) ──WAL──▶ Shard 0 Replica (Node B)
Shard 1 Primary (Node A) ──WAL──▶ Shard 1 Replica (Node C)
Shard 2 Primary (Node B) ──WAL──▶ Shard 2 Replica (Node A)
```

Replication modes (per shard):
- **WAL-shipping** (`role = primary/replica`): async or sync quorum ack.
- **Raft** (`role = raft_member`): automatic leader election and failover.

## Failover

### Fencing Protocol (5 steps)

```
1. FENCE:    Block writes on old primary for this shard
2. CATCH-UP: Replica applies remaining WAL to reach primary's LSN
3. SWAP:     Replica becomes new primary (shard map update)
4. UNFENCE:  New primary accepts writes
5. UPDATE:   Shard map epoch incremented; disseminated to all nodes
```

- **Epoch fencing**: prevents split-brain; stale-epoch writes are rejected.
- **Deterministic**: failover outcome is the same regardless of timing (DCG property).

### Admission Control

`AdmissionControl` manages per-node capacity during failover and normal operation:

| Permit Type | Purpose |
|-------------|---------|
| Read | Concurrent read query slots |
| Write | Concurrent write transaction slots |
| DDL | Exclusive DDL operation (serialized) |

### Node Operational Mode

| Mode | Reads | Writes | DDL |
|------|-------|--------|-----|
| Normal | ✅ | ✅ | ✅ |
| ReadOnly | ✅ | ❌ | ❌ |
| Drain | ❌ | ❌ | ❌ |

Used during rolling upgrades and planned maintenance.

## Shard Rebalancing

Currently **manual** (M1). The enterprise module (`falcon_enterprise`) provides:

- **Auto-rebalance**: move shards to balance load across nodes.
- **Bounded disruption**: migration proceeds shard-by-shard with fencing.
- **SLA forecasting**: predict when rebalancing is needed based on growth trends.

## Configuration

```toml
[cluster]
num_shards = 16
node_id = 1

[cluster.nodes]
1 = { addr = "node1:5433", role = "primary" }
2 = { addr = "node2:5433", role = "replica" }
3 = { addr = "node3:5433", role = "replica" }

[cluster.shard_map]
# Auto-generated or manually specified
```

## Invariants

1. `shard_id = hash(pk) % num_shards` is deterministic and stable.
2. Each shard has exactly one primary at any time (enforced by epoch fencing).
3. Single-shard transactions never use 2PC.
4. Cross-shard transactions always use 2PC; partial commits are impossible.
5. Shard map epoch is monotonically increasing; stale operations are rejected.
6. Failover preserves the Deterministic Commit Guarantee (no committed-but-invisible
   or visible-but-uncommitted transactions).
