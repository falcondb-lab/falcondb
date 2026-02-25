# Failover × Network Partition × Write Conflict — Quantified SLA

> **This document is normative.** Every bound has a corresponding code reference,
> configuration parameter, or automated test. If docs and code disagree, it is a bug.

---

## 1. Scope

This document quantifies the **external SLA** for three interacting failure domains:

1. **Failover** — primary node crash or forced leader switch
2. **Network partition** — primary ↔ replica communication interrupted
3. **Write conflict** — OCC (optimistic concurrency control) conflicts during or after failover

It supersedes informal guarantees scattered across other docs and provides
**concrete numbers** that clients, SREs, and JDBC driver implementations can depend on.

---

## 2. System Model

```
┌──────────┐   WAL-shipping (gRPC)   ┌──────────┐
│ Primary  │ ──────────────────────→ │ Replica  │
│ (writes) │                          │ (standby)│
└──────────┘                          └──────────┘
     ↑                                     │
     │  Client connections                 │ Promote on failure
     │                                     ↓
┌──────────┐                          ┌──────────┐
│  Client  │ ──── reconnect ────────→│New Primary│
└──────────┘                          └──────────┘
```

- **Replication**: WAL-shipping over gRPC (see ARCHITECTURE.md §5.5)
- **Commit policies**: `LocalWalSync`, `PrimaryWalOnly`, `PrimaryPlusReplicaAck`
- **Concurrency control**: OCC with MVCC timestamps
- **Fencing**: Epoch-based; old primary rejected after promote (FAIL-3)

---

## 3. Failover Timing SLA

### 3.1 Detection → Promotion Latency

| Phase | Default | Config | Hard Bound |
|-------|---------|--------|------------|
| Heartbeat interval | 1 s | `falcon.cluster.heartbeat_interval_ms` | — |
| Failure detection | 5 s | `falcon.cluster.failure_timeout_ms` | ≥ 2× heartbeat |
| Drain active txns | 5 s | `falcon.server.shutdown_drain_timeout_secs` | Force-abort after timeout |
| Epoch bump + promote | < 50 ms | — | Bounded by WAL replay size |
| **Total worst-case** | **≤ 10.05 s** | — | Configurable down to ~2.1 s |

**Code**: `FailoverTxnCoordinator::begin_failover_drain()` → `complete_failover_drain()`
**Test**: `failover_determinism.rs` — all 9 matrix tests measure `failover_ms`

### 3.2 Post-Failover Availability

| Metric | SLA | Evidence |
|--------|-----|----------|
| New primary accepts writes | < 1 s after promote | Test: `ga_p0_3_new_leader_accepts_writes` |
| TPS convergence to baseline | ≤ 30 s | `FailoverTxnCoordinator::convergence_window` (default 30s) |
| p99 latency convergence | ≤ 30 s | `FailoverBlockedTxnGuard::is_converged()` |

---

## 4. Network Partition SLA

### 4.1 Partition Scenarios

| Scenario | Primary behavior | Replica behavior | Data risk |
|----------|-----------------|-----------------|-----------|
| **P1**: Primary ↔ Replica | Primary continues writes (no ACK) | Replica stale; may be promoted | In-doubt txns on old primary |
| **P2**: Client ↔ Primary | Client disconnected; txns time out | Replica unaffected | Client retries on reconnect |
| **P3**: Primary ↔ Coordinator | Coordinator cannot manage failover | System continues in last-known state | Manual intervention may be needed |

### 4.2 Partition Write Isolation Guarantee

> **SLA-P1**: Writes committed on the old primary during a network partition are
> **NEVER** visible on the new primary after promotion.

This is the **no split-brain** guarantee. It holds because:
1. Replica only sees WAL records that were shipped before partition
2. Promote makes the replica's WAL the new source of truth
3. Old primary is fenced (epoch check rejects stale writes)

**Test**: `fde3_partition_writes_invisible_after_promote` — 10 partition-era writes, 0 leaked

### 4.3 Partition Duration Bounds

| Metric | Bound | Enforcement |
|--------|-------|-------------|
| Max partition before forced failover | Configurable (default: `failure_timeout_ms` = 5 s) | Heartbeat timeout |
| Max in-doubt lifetime | 60 s (default) | `InDoubtTtlEnforcer` force-aborts after TTL |
| Max replication lag before write rejection | Configurable | `ReplicationLagExceeded` admission gate, SQLSTATE `57P03` |

### 4.4 In-Doubt Transaction Resolution

Transactions that were committed on the old primary but not yet shipped to the
replica at partition time enter **in-doubt** state.

| Property | Guarantee | Evidence |
|----------|-----------|----------|
| In-doubt explicitly tracked | Always | `InDoubtResolver::register_indoubt()` |
| In-doubt never silently committed | Always | `fde3_partition_writes_invisible_after_promote` |
| In-doubt resolution bounded | ≤ 60 s (default TTL) | `fde4_indoubt_ttl_bounded_resolution` |
| In-doubt queryable by admin | Always | `InDoubtResolver::admin_inspect()` |
| Admin force-commit/abort | Available | `admin_force_commit()` / `admin_force_abort()` |
| No in-doubt accumulation across cycles | Verified | `fde4` — 5 cycles × 10 txns, 0 remaining |

**Config**: `falcon.txn.indoubt_ttl_secs` (default 60)

---

## 5. Write Conflict SLA

### 5.1 OCC Conflict Model

FalconDB uses **optimistic concurrency control** with MVCC timestamps.
Write conflicts are detected at commit time, not at write time.

| Conflict type | Detection point | Error | SQLSTATE | Retryable |
|--------------|----------------|-------|----------|-----------|
| Write-write (same key) | `commit_txn()` | `WriteConflict` | `40001` | Yes |
| Serialization anomaly | `commit_txn()` | `SerializationConflict` | `40001` | Yes |
| Constraint violation | `commit_txn()` | `ConstraintViolation` | `23xxx` | No |

**Code**: `determinism_hardening.rs §2 classify_error()`

### 5.2 Write Conflict During Failover

| Scenario | Behavior | Evidence |
|----------|----------|----------|
| Two writers, same key, one committed + shipped, failover | Winner's value survives on new primary | `fde2_occ_write_conflict_during_failover` |
| Winner committed but NOT shipped, failover | Winner's value lost (WAL not on replica) | `fde2` — expected under async replication |
| Loser's commit attempt | `WriteConflict` or `Aborted` — deterministic error | `fde2` — no phantom, no duplication |
| Duplicate WAL replay after failover | Idempotent — same state regardless of replay count | `fde5_idempotent_wal_replay_after_failover` |

### 5.3 Write Conflict Retry Contract

| Property | Guarantee |
|----------|-----------|
| Conflict error is retryable | Yes — `AbortedRetryable { SerializationConflict }` |
| Retry policy | `RetryAfter(50ms)` |
| Conflict does not corrupt state | Always — abort is clean |
| Client sees exactly one outcome | Always — Committed OR Aborted, never ambiguous |

**Code**: `determinism_hardening.rs` — `TxnTerminalState`, `RetryPolicy`

### 5.4 Write Conflict Rate Under Load

Write conflict rate depends on workload contention. FalconDB does NOT
guarantee a maximum conflict rate — this is inherent to OCC.

| Workload | Expected conflict rate | Mitigation |
|----------|----------------------|------------|
| Low contention (distinct keys) | ~0% | — |
| Medium contention (hot keys) | 1-5% | Client retry with backoff |
| High contention (single hot key) | 10-30%+ | Application-level serialization recommended |

---

## 6. Combined SLA: Partition + Failover + Conflict

### 6.1 RPO (Recovery Point Objective)

| CommitPolicy | RPO | Condition |
|-------------|-----|-----------|
| `LocalWalSync` | **0 txns** | Local crash only (no replication) |
| `PrimaryWalOnly` | **≤ group_commit_window** | Txns in WAL buffer at crash time |
| `PrimaryPlusReplicaAck` | **0 txns** | If ≥1 replica alive and caught up |

### 6.2 RTO (Recovery Time Objective)

| Scenario | RTO | Condition |
|----------|-----|-----------|
| Clean failover (planned) | **< 1 s** | Drain + promote |
| Crash failover (unplanned) | **≤ 10 s** | Detection + drain timeout + promote |
| Network partition resolved | **< 1 s** | WAL catch-up + epoch reconciliation |

### 6.3 Quantified Guarantees (Combined)

| ID | Guarantee | Bound | Test Evidence |
|----|-----------|-------|---------------|
| **SLA-1** | Phantom commits after failover | **0** | 9-cell matrix (9/9), `fde1`, `fde3` |
| **SLA-2** | In-doubt resolution time | **≤ 60 s** | `fde4` (5 cycles, 50 txns, 0 remaining) |
| **SLA-3** | Partition write isolation | **100%** (0 leaks) | `fde3` (10 partition writes, 0 leaked) |
| **SLA-4** | OCC conflict determinism | **Always classifiable** | `fde2` (no phantom, no duplication) |
| **SLA-5** | WAL replay idempotency | **Always** | `fde5` (20 txns double-shipped, identical state) |
| **SLA-6** | Failover time (promote) | **< 50 ms** | 9-cell matrix `failover_ms` |
| **SLA-7** | Post-failover TPS convergence | **≤ 30 s** | `FailoverTxnCoordinator::convergence_window` |
| **SLA-8** | Blocked txn timeout | **≤ 200 ms** (default) | `FailoverBlockedTxnGuard::max_blocked_duration` |
| **SLA-9** | Failover damping (anti-flap) | **≥ 10 s** between events | `FailoverDamper::min_interval` |
| **SLA-10** | Write-blocked window | **≤ drain_timeout** (5 s) | `FailoverTxnCoordinator::drain_timeout` |

---

## 7. Client-Side Contract

### 7.1 JDBC/PG Driver Behavior

| Event | Client action | Error code |
|-------|--------------|------------|
| Connection lost during idle | Reconnect to seed list | `08006` |
| Connection lost during read | Reconnect + retry read | `08006` |
| Connection lost during write (no COMMIT) | Reconnect; txn was aborted | `08006` |
| COMMIT sent, no ACK | **Check txn status** on new primary | `08006` |
| `WriteConflict` | Retry with backoff (50 ms) | `40001` |
| `ReplicationLagExceeded` | Retry after 500 ms | `57P03` |
| `NodeNotAcceptingWrites` | Retry on different node | `25006` |
| `CircuitBreakerOpen` | Retry on different node after 1 s | `57P03` |

### 7.2 Idempotency Requirement

For **at-most-once** semantics under failover, clients MUST:
1. Use an application-level idempotency key (e.g., `INSERT ... ON CONFLICT DO NOTHING`)
2. On indeterminate COMMIT, query by idempotency key before retrying

FalconDB guarantees that WAL replay is idempotent (FC-4), but application-level
idempotency is the client's responsibility.

---

## 8. Forbidden States

The following states are **impossible** by design and tested:

| ID | Forbidden State | Prevention | Test |
|----|----------------|------------|------|
| FS-1 | Committed txn invisible after failover (sync policy) | WAL fsync before ACK | `ga_p0_1_ack_equals_durable` |
| FS-2 | Uncommitted txn visible after failover | Recovery discards uncommitted | `ga_p0_1_no_phantom_commit` |
| FS-3 | Old primary accepts writes after promote | Epoch fencing | `epoch.rs` tests |
| FS-4 | In-doubt txn silently committed on new primary | Replica doesn't have the WAL | `fde3` |
| FS-5 | In-doubt txn persists forever | TTL enforcer force-aborts | `fde4` |
| FS-6 | Write conflict produces ambiguous outcome | OCC: exactly one winner | `fde2` |
| FS-7 | WAL double-replay produces different state | Replay is idempotent | `fde5` |
| FS-8 | Split-brain: both nodes accept writes | Epoch fencing on old primary | FAIL-3 |

---

## 9. Configuration Reference

| Parameter | Default | Effect |
|-----------|---------|--------|
| `falcon.cluster.heartbeat_interval_ms` | 1000 | Heartbeat frequency |
| `falcon.cluster.failure_timeout_ms` | 5000 | Miss threshold for failure detection |
| `falcon.cluster.failover_damper_secs` | 10 | Min interval between failovers |
| `falcon.server.shutdown_drain_timeout_secs` | 5 | Max drain time during failover |
| `falcon.txn.indoubt_ttl_secs` | 60 | Max in-doubt lifetime before force-abort |
| `falcon.txn.max_blocked_duration_ms` | 200 | Max time a txn can be blocked by failover |
| `falcon.replication.max_lag_before_reject` | configurable | Admission gate threshold |
| `falcon.wal.sync_mode` | `fdatasync` | WAL durability mode |
| `falcon.wal.group_commit_window_us` | 200 | Group commit coalescing window |

---

## 10. Test Evidence Index

| Test ID | File | What It Proves |
|---------|------|---------------|
| FDE-1 | `failover_determinism.rs` | Commit-phase-at-crash → recovery outcome (FC-1, FC-2, FC-3) |
| FDE-2 | `failover_determinism.rs` | OCC conflict during failover: no phantom, no duplication |
| FDE-3 | `failover_determinism.rs` | Network partition write isolation: 0 leaks |
| FDE-4 | `failover_determinism.rs` | In-doubt TTL bounded resolution: 50 txns, 0 remaining |
| FDE-5 | `failover_determinism.rs` | Idempotent WAL replay: double-ship, identical state |
| 9-cell | `failover_determinism.rs` | 3 faults × 3 loads: 0 phantom commits |
| SS-01..04 | `failover_txn_tests.rs` | Single-shard failover × txn matrix |
| XS-01..08 | `failover_txn_tests.rs` | Cross-shard 2PC failover × txn matrix |
| CH-01..02 | `failover_txn_tests.rs` | Churn stability (repeated failover) |
| ID-01..02 | `failover_txn_tests.rs` | Idempotency + replay safety |

**Run all**: `cargo test -p falcon_cluster --test failover_determinism -- --nocapture`
