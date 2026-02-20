# ADR-0004: Commit Semantics — Async Replication (M1 Freeze)

## Status
Accepted (M1) — Frozen for M1 release.

## Context
Falcon M1 needs a clear, documented commit ack policy that balances
latency against data loss risk. The choice directly affects RPO/RTO
and user expectations.

## Decision: Async Replication (Scheme A)

### Commit Ack Rule
A transaction is **committed** when the WAL record is **durable on the primary**.
The primary does **NOT** wait for any replica to acknowledge.

### Replication Flow
```
Client                Primary                    Replica
  │  BEGIN + DML         │                          │
  │─────────────────────▶│                          │
  │                      │ WAL append + fsync       │
  │                      │──────────────────┐       │
  │                      │                  │       │
  │  COMMIT OK           │◀─────────────────┘       │
  │◀─────────────────────│                          │
  │                      │  WAL stream (async)      │
  │                      │─────────────────────────▶│
  │                      │                          │ apply
  │                      │      ack(applied_lsn)    │
  │                      │◀─────────────────────────│
```

### RPO (Recovery Point Objective)
- **RPO > 0**: If the primary crashes after committing locally but before
  the replica receives the WAL chunk, those committed transactions are
  **lost on failover**.
- The RPO window is bounded by:
  - Network latency between primary and replica
  - WAL streaming batch size (`max_records_per_chunk`)
  - Replica apply throughput
- Typical RPO under normal operation: **sub-second** (WAL chunks are
  streamed continuously via gRPC server-streaming).

### RTO (Recovery Time Objective)
- **RTO = detect + promote + route-switch**
- Detection: external health check or manual trigger (M1 has no auto-failover).
- Promote: `ShardReplicaGroup::promote(replica_idx)` executes:
  1. Fence old primary (set `read_only = true`)
  2. Catch up replica to latest known WAL
  3. Swap roles (replica → primary)
  4. Unfence new primary
  5. Update `ShardMap` routing atomically
- Measured promote latency: **< 10ms** in-process (dominated by catch-up).
- Total RTO in production: **seconds** (dominated by detection latency).

## What M1 Does NOT Support
- **Synchronous replication**: no quorum commit, no sync-rep mode.
- **Automatic failover**: promote is manual (API call or script).
- **Multi-replica quorum**: only 1 primary + N replicas, no Raft consensus.
- **Zero-RPO guarantee**: by design, async replication has RPO > 0.

## Consequences
- **Low commit latency**: primary never waits for replica. Commit = local
  WAL fsync only.
- **Potential data loss**: transactions committed between last replica ack
  and primary crash are lost on failover.
- **Simple operational model**: one primary, one or more replicas, manual
  promote.
- **Clear upgrade path**: M2 can add sync-rep mode or quorum commit as
  a `ReplicationMode` enum without changing the core commit path.

## Observability
- `SHOW falcon.replication_stats`: promote_count, last_failover_time_ms
- `ReplicaRunnerMetrics`: applied_lsn, chunks_applied, records_applied,
  reconnect_count, connected
- `WalReplicationService::replication_lag()`: per-replica LSN lag

## Verification
- `tests/failover.rs`: end-to-end failover exercise
- `replica_runner_tests::test_replica_runner_e2e_cross_node`: real gRPC
  replication with data verification
- `falcon_bench --failover`: benchmark with failover mid-workload
