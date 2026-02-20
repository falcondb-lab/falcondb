# FalconDB — Consistency Semantics Specification

> **Status**: Normative. Every invariant in this document has a corresponding test.
> **Module**: `falcon_common::consistency`
> **Test suites**: `falcon_storage/tests/consistency_wal.rs`, `falcon_cluster/tests/consistency_replication.rs`

---

## 1. Commit Point Model

A transaction passes through three strictly ordered commit points:

| # | Commit Point | Description | Crash Behavior |
|---|---|---|---|
| **CP-L** | Logical Commit | MVCC marks txn committed (in-memory) | May be lost |
| **CP-D** | Durable Commit | WAL record satisfies the active `CommitPolicy` | Must survive |
| **CP-V** | Client-Visible Commit | Client receives success response | Must never be rolled back |

### Invariants

| ID | Statement | Test |
|----|-----------|------|
| **CP-1** | `CP-L ≤ CP-D ≤ CP-V` (strict temporal ordering) | `test_commit_point_ordering` |
| **CP-2** | CP-V must not occur before CP-D | `test_validate_commit_timeline_violations` |
| **CP-3** | Crash between CP-L and CP-D may lose the transaction | `test_crash_point_survival` |
| **CP-4** | Crash after CP-D must NOT lose the transaction | `test_crash_point_survival` |

---

## 2. Commit Policies

The system supports four configurable commit policies. The active policy determines what I/O must complete before a commit reaches CP-D.

| Policy | Local WAL fsync | Replica ACK | Crash Loss Window | Config Key |
|--------|:-:|:-:|---|---|
| `LOCAL_WAL_SYNC` (default) | ✓ | — | 0 (local) | `wal.durability_policy = "local-fsync"` |
| `PRIMARY_WAL_ONLY` | — | — | Up to `flush_interval_us` | `wal.durability_policy = "primary-wal-only"` |
| `PRIMARY_PLUS_REPLICA_ACK(N)` | ✓ | N replicas | 0 if ≥1 acked replica survives | `wal.durability_policy = "quorum-ack"` |
| `RAFT_MAJORITY` | Raft | Raft majority | 0 if majority survives | Raft mode |

### Policy Contract

| ID | Statement |
|----|-----------|
| **POL-1** | Client ACK is NOT sent until all I/O required by the active policy completes |
| **POL-2** | If the policy cannot be satisfied (e.g. no replicas), the system either degrades and flags `degraded=true`, or rejects the transaction |
| **POL-3** | Policy degradation is observable via `DurabilityAnnotation` on the transaction record and Prometheus metrics |

### Failover Implications

| Policy | Duplicate commit after failover? |
|--------|---|
| `LOCAL_WAL_SYNC` | No |
| `PRIMARY_WAL_ONLY` | **Yes** — client may have ACK but new primary has no record |
| `PRIMARY_PLUS_REPLICA_ACK(N)` | No (if promoted replica was acked) |
| `RAFT_MAJORITY` | No |

---

## 3. WAL Invariants

| ID | Invariant | Test |
|----|-----------|------|
| **WAL-1** | Every entry has a globally unique, monotonically increasing LSN | `test_wal1_unique_monotonic_lsn` |
| **WAL-2** | Replay is idempotent: `apply(apply(S, r), r) = apply(S, r)` | `test_wal2_idempotent_replay` |
| **WAL-3** | Commit timestamps are monotonically increasing in WAL order | `test_wal3_monotonic_commit_sequence` |
| **WAL-4** | Repeated replay converges to the same state | `test_wal4_wal5_replay_convergence_and_completeness` |
| **WAL-5** | After recovery: committed txns visible, uncommitted txns invisible | `test_wal4_wal5_replay_convergence_and_completeness` |
| **WAL-6** | CRC32 checksum on every record; corruption halts recovery at that point | `test_wal6_checksum_corruption_halts_recovery` |

### WAL Record Format

```
[len:4 LE][crc32:4 LE][bincode-payload:len]
```

Each record carries a `txn_id`. Segment files: `falcon_NNNNNN.wal` (64 MB default).

---

## 4. Crash Recovery Semantics

For each crash point, the recovery outcome is **deterministic**:

| Crash Point | Txn in WAL? | Commit record? | Recovery Outcome |
|---|:-:|:-:|---|
| Before WAL write | No | No | Txn does not exist | 
| After WAL write, before commit | Yes | No | **Rolled back** (uncommitted writes cleaned up) |
| After commit record, before fsync | Yes | Yes | **May or may not exist** (OS buffer dependent) |
| After fsync, before client ACK | Yes | Yes | **Must exist**. Client gets `Indeterminate` (SQLSTATE `08006`) |
| After client ACK, before replication | Yes | Yes | **Exists on primary**. Lost on failover under `LOCAL_WAL_SYNC` |
| After replication ACK | Yes | Yes | **Exists on primary + replica(s)** |

### Tests

| Scenario | Test |
|----------|------|
| Crash before WAL write | `test_crash_before_wal_write` |
| Crash after write, before commit | `test_crash_after_wal_write_before_commit` |
| Recovery with updates + deletes | `test_crash_recovery_with_updates_and_deletes` |
| Checkpoint + WAL delta recovery | `test_checkpoint_plus_wal_recovery` |
| DDL recovery (CREATE + DROP) | `test_ddl_recovery_create_and_drop` |
| Interleaved committed/uncommitted txns | `test_interleaved_txn_recovery` |

---

## 5. Replication Invariants

| ID | Invariant | Test |
|----|-----------|------|
| **REP-1** | Replica committed set ⊆ Primary committed set (prefix property) | `test_rep1_prefix_property` |
| **REP-2** | Replica never commits a txn that primary has not committed | `test_rep2_no_phantom_commits` |
| **REP-3** | Replica applies WAL entries in primary's write order | `test_rep3_strict_ordering` |
| **REP-4** | Replica ACK means WAL entry has been applied (not just received) | By design (`ReplicaAckSemantics::Applied`) |

### Replication Model

| Property | Value |
|----------|-------|
| Replication unit | WAL entry (individual records) |
| Apply ordering | Strictly sequential (same as primary) |
| Parallel apply | Not allowed |
| ACK semantics | Applied (visible to reads) |

---

## 6. Failover / Promote Semantics

| ID | Invariant | Test |
|----|-----------|------|
| **FAIL-1** | After promote: `new_primary_commits ⊆ old_primary_commits` | `test_fail1_commit_set_containment_after_promote` |
| **FAIL-2** | ACK'd txns under replica-ack policy survive failover | `test_fail2_acked_txns_survive_failover` |
| **FAIL-3** | Old primary is fenced via epoch; stale epoch rejected | `test_fail3_epoch_fencing` |

### Promote Preconditions

Before a replica can be promoted, the system validates:

1. **Cooldown**: `failover_cooldown` has elapsed since last promote
2. **Health**: At least one replica is healthy (heartbeat within `failure_timeout`)
3. **LSN**: Best replica selected by highest `applied_lsn`
4. **Epoch**: Monotonically bumped on every promote

### Post-Promote Rules

| Question | Answer |
|----------|--------|
| Which txns must NOT be rolled back? | All txns committed on the promoted replica |
| Which txns may be rolled back? | Txns committed on old primary but not yet replicated |
| Client reconnection | Must check txn status if last response was `Indeterminate` |
| Fencing mechanism | Epoch-based: `validate_epoch(request_epoch)` rejects stale writes |

### Data Survival After Multiple Failovers

Tested by `test_multiple_failovers_maintain_data`: data written across two failover cycles (6 rows, 2 promotes) is fully preserved.

---

## 7. Cross-Shard Transaction Semantics

FalconDB uses a **Hybrid Fast/Slow path** model:
- **Fast path** (single-shard): local OCC + commit
- **Slow path** (multi-shard): Two-Phase Commit (2PC) via `TwoPhaseCoordinator`

| ID | Invariant | Test |
|----|-----------|------|
| **XS-1** | All participating shards commit OR all abort (atomicity) | `test_xs1_cross_shard_atomicity_commit`, `test_xs1_cross_shard_atomicity_abort` |
| **XS-2** | A cross-shard txn commits at most once | `test_xs2_at_most_once_commit` |
| **XS-3** | Coordinator crash recovery: `CoordinatorCommit` in WAL → commit all; only `CoordinatorPrepare` → abort all | By WAL record design |
| **XS-4** | Participant holds PREPARED txn until coordinator resolves | By recovery design |

### Failure Semantics

| Failure | Outcome |
|---------|---------|
| Coordinator crash before `CoordinatorCommit` WAL record | All participants abort |
| Coordinator crash after `CoordinatorCommit` WAL record | Recovery completes commit on all participants |
| Partial shard failure during prepare | All prepared shards abort |
| Retry / duplicate commit request | New txn_id allocated; at-most-once guaranteed per txn_id |

---

## 8. Read Consistency Semantics

### Supported Isolation Levels

| Level | Guarantee |
|-------|-----------|
| **Read Committed** (default) | Each statement sees data committed before it started |
| **Snapshot Isolation** | All reads in a txn see a consistent snapshot at txn start |
| **Serializable** | Transactions equivalent to some serial execution; OCC validates read-sets |

### Replica Read Semantics

| Property | Default | Configurable |
|----------|---------|:---:|
| Read from replica | Disabled | ✓ |
| Max staleness | 5 seconds | ✓ |
| Read-your-writes | Yes (routes to primary after write) | ✓ |
| Snapshot reads | Disabled | ✓ |

**Invariant READ-4**: Reads from a replica are bounded by the replica's `applied_lsn`. No read can see data beyond this point.

---

## 9. Client-Observable Error Codes

Every consistency-critical error maps to a specific PG SQLSTATE. No generic `XX000` is used for consistency-related failures.

| SQLSTATE | Meaning | Client Action |
|----------|---------|---------------|
| `00000` | Commit succeeded | None |
| `40001` | Serialization failure | Retry transaction |
| `40P01` | Deadlock detected | Retry transaction |
| `57014` | Query cancelled by user | Retry or abort |
| `08006` | Connection failure — commit **indeterminate** | **Check txn status** before retry |
| `57P01` | Crash shutdown — commit **indeterminate** | **Check txn status** before retry |
| `25006` | Read-only transaction attempted write | Fix application logic |
| `53300` | WAL backlog exceeded (admission control) | Retry later |
| `53301` | Replication lag exceeded (admission control) | Retry later |
| `53400` | Insufficient replicas for commit policy | Retry or degrade policy |
| `57P03` | Node fenced (old primary after failover) | Reconnect to new primary |
| `58000` | Consensus error | Retry or check cluster health |

### Outcome Classification

The system classifies every commit outcome into exactly one of:

| Outcome | Client Received | Txn Survives Crash? | Txn May Be Lost? |
|---------|:-:|:-:|:-:|
| **Committed** | Success | Yes (per policy) | No |
| **Aborted** | Error | No | N/A |
| **Indeterminate** | Error/timeout | Unknown | Possibly |

**Invariant ERR-1**: `Committed` outcome → txn survives any single-node crash under the active policy.
**Invariant ERR-2**: `Aborted` outcome → txn is never visible after recovery.
**Invariant ERR-3**: `Indeterminate` → client MUST check txn status before retrying.

---

## 10. Fault Injection Framework

FalconDB includes a built-in fault injection framework (`falcon_cluster::fault_injection`) for chaos testing:

| Fault Type | Mechanism | Use Case |
|-----------|-----------|----------|
| Leader crash | `kill_leader()` / `revive_leader()` | Failover testing |
| Replica delay | `set_replica_delay(us)` | Replication lag testing |
| WAL corruption | `arm_wal_corruption()` (one-shot) | Recovery integrity testing |
| Disk latency | `set_disk_delay(us)` | I/O pressure testing |

The `ChaosRunner` executes scenario sequences with consistency checks after each:

```rust
let runner = ChaosRunner::new(injector);
let report = runner.run(scenarios, || {
    // verify consistency invariants
    Ok(())
});
assert!(report.all_consistent);
```

---

## 11. Observability

All consistency-critical paths include:

- **Tracing spans**: WAL append, commit, replication ship, failover promote
- **Prometheus metrics**: `falcon_txn_committed_total`, `falcon_txn_aborted_total`, `falcon_replication_lag_us`, `falcon_failover_count`, `falcon_wal_records_written`
- **DurabilityAnnotation**: Per-txn record of requested vs. actual durability level, degradation flag, ack latency

---

## 12. Acceptance Criteria

For any crash / failover / promote / retry:

| Criterion | Guaranteed |
|-----------|:-:|
| System behavior is predictable | ✓ |
| Client result is explainable | ✓ |
| State is replayable from WAL | ✓ |

The following do **NOT** exist in this specification:

- ~~"Probably correct"~~
- ~~"Theoretically impossible"~~
- ~~"Rarely happens in practice"~~

---

## Appendix: Test Coverage Matrix

| Invariant | Test File | Test Function |
|-----------|-----------|---------------|
| CP-1..4 | `falcon_common` | `test_commit_point_ordering`, `test_crash_point_survival`, `test_validate_commit_timeline_*` |
| POL-1..3 | `falcon_common` | `test_commit_policy_*`, `test_policy_allows_duplicate_after_failover` |
| WAL-1 | `consistency_wal.rs` | `test_wal1_unique_monotonic_lsn` |
| WAL-2 | `consistency_wal.rs` | `test_wal2_idempotent_replay` |
| WAL-3 | `consistency_wal.rs` | `test_wal3_monotonic_commit_sequence` |
| WAL-4+5 | `consistency_wal.rs` | `test_wal4_wal5_replay_convergence_and_completeness` |
| WAL-6 | `consistency_wal.rs` | `test_wal6_checksum_corruption_halts_recovery` |
| CRASH-1 | `consistency_wal.rs` | `test_crash_before_wal_write` |
| CRASH-2 | `consistency_wal.rs` | `test_crash_after_wal_write_before_commit` |
| CRASH-UPD | `consistency_wal.rs` | `test_crash_recovery_with_updates_and_deletes` |
| CRASH-CKPT | `consistency_wal.rs` | `test_checkpoint_plus_wal_recovery` |
| CRASH-DDL | `consistency_wal.rs` | `test_ddl_recovery_create_and_drop` |
| CRASH-INTLV | `consistency_wal.rs` | `test_interleaved_txn_recovery` |
| REP-1 | `consistency_replication.rs` | `test_rep1_prefix_property` |
| REP-2 | `consistency_replication.rs` | `test_rep2_no_phantom_commits` |
| REP-3 | `consistency_replication.rs` | `test_rep3_strict_ordering` |
| FAIL-1 | `consistency_replication.rs` | `test_fail1_commit_set_containment_after_promote` |
| FAIL-2 | `consistency_replication.rs` | `test_fail2_acked_txns_survive_failover` |
| FAIL-3 | `consistency_replication.rs` | `test_fail3_epoch_fencing` |
| FAIL-MULTI | `consistency_replication.rs` | `test_multiple_failovers_maintain_data` |
| XS-1 | `consistency_replication.rs` | `test_xs1_cross_shard_atomicity_commit`, `test_xs1_cross_shard_atomicity_abort` |
| XS-2 | `consistency_replication.rs` | `test_xs2_at_most_once_commit` |
| ERR-* | `falcon_common` | `test_commit_outcome_sqlstate` |
