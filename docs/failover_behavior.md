# Failover Transaction Behavior — GA Normative Reference

> **This document is normative.** Every invariant has a corresponding code reference
> and test. If docs and code disagree, it is a bug.

---

## 1. Failover Model

FalconDB uses a **primary-replica** model with configurable commit policies:

```
Client → Primary (WAL append + fsync) → Replica(s) (WAL stream)
```

On primary failure, a replica is promoted. The promoted replica's WAL
is the new source of truth.

---

## 2. Failover × Transaction Invariants

| ID | Invariant | Description | Code Reference | Test |
|----|-----------|-------------|----------------|------|
| **FAIL-1** | No phantom commit | After promote, new primary's committed set ⊆ old primary's WAL-durable set | `consistency.rs §7` | `ga_p0_3_failover_only_committed_survive` |
| **FAIL-2** | No double commit | WAL replay is idempotent — replaying N times produces identical state | `consistency.rs §4 WAL-2` | `ga_p0_2_wal_replay_idempotent` |
| **FAIL-3** | Old primary fenced | After promote, old primary MUST NOT accept new writes | `consistency.rs §7 FAIL_3_FENCING` | `epoch.rs` epoch guard tests |
| **FAIL-4** | In-flight txn resolution | Active txns at crash are rolled back (no commit record in WAL) | `engine.rs recover()` | `ga_p0_1_no_phantom_commit` |
| **FAIL-5** | New leader accepts writes | After recovery/promote, system accepts new transactions immediately | — | `ga_p0_3_new_leader_accepts_writes` |
| **FAIL-6** | ACK'd txns survive | Under `PrimaryPlusReplicaAck`, ACK'd txns survive single-node failure | `consistency.rs §7` | `ga_p0_1_ack_equals_durable` |

---

## 3. Failover Transaction Outcomes by Commit Policy

| CommitPolicy | Txn with WAL commit record on surviving node | Txn without WAL commit record | RPO |
|-------------|----------------------------------------------|-------------------------------|-----|
| `LocalWalSync` | Survives | Rolled back | 0 (local crash) |
| `PrimaryWalOnly` | Survives if fsynced before crash | Lost if only in buffer | ≤ `flush_interval_us` |
| `PrimaryPlusReplicaAck` | Survives (on replica) | Rolled back | 0 (if ≥1 replica alive) |
| `RaftMajority` ¹ | Survives (on majority) | Rolled back | 0 (if majority alive) |

¹ `RaftMajority` is defined in code but **not available in production** — requires Raft consensus (not implemented).

---

## 4. Failover Sequence

```
1. Primary fails (crash / network partition / kill -9)
2. Failure detected (heartbeat timeout)
3. Replica selected for promotion
4. Epoch incremented → old primary fenced (FAIL-3)
5. New primary replays WAL to consistent state
6. In-flight txns without commit record → rolled back (FAIL-4)
7. New primary starts accepting connections (FAIL-5)
```

### Timing Boundaries

| Metric | Default | Configurable |
|--------|---------|-------------|
| Heartbeat interval | 1s | `falcon.cluster.heartbeat_interval_ms` |
| Failure detection timeout | 5s | `falcon.cluster.failure_timeout_ms` |
| Failover drain timeout | 5s | `falcon.server.shutdown_drain_timeout_secs` |
| In-doubt TTL | 60s | `falcon.txn.indoubt_ttl_secs` |
| Min failover interval (damper) | 10s | `falcon.cluster.failover_damper_secs` |

---

## 5. Client Behavior During Failover

| Client State | Expected Behavior | SQLSTATE |
|-------------|-------------------|----------|
| Idle connection | Connection lost; reconnect to new primary | `08006` |
| In-flight read | Connection lost; retry on new primary | `08006` |
| In-flight write (before COMMIT sent) | Connection lost; txn aborted | `08006` |
| COMMIT sent, no ACK received | **Indeterminate** — client MUST check status | `08006` |
| COMMIT ACK received | Txn IS committed; survives failover | — |

### Indeterminate Resolution

When a client sends COMMIT but does not receive ACK:
1. Reconnect to new primary
2. Query transaction status (application-level idempotency key)
3. If committed → proceed
4. If not found → retry (at-most-once semantics via idempotency)

---

## 6. Admission Control During Failover

| Condition | Action | SQLSTATE |
|-----------|--------|----------|
| Memory Critical | Reject all new txns | `53200` |
| WAL backlog exceeded | Reject write txns | `53300` |
| Replication lag exceeded | Reject write txns | `57P03` |

**Code**: `falcon_txn::manager::try_begin_with_classification()` — 3-gate admission control

---

## 7. Recovery After Failover

Recovery is fully automated. No human intervention required.

```
1. StorageEngine::recover(wal_dir)
2. Load checkpoint (if exists)
3. Replay WAL records from checkpoint onward
4. Committed txns → applied
5. Aborted txns → discarded
6. In-flight txns (no terminal record) → discarded
7. Index rebuild
8. Ready to serve
```

**Test**: `ga_p0_5_multiple_crash_restart_cycles` — 5 consecutive crash-restart cycles
with data integrity verified at each step.

---

## 8. Test Coverage

| Test | What it proves |
|------|---------------|
| `ga_p0_3_failover_only_committed_survive` | FAIL-1: No phantom commit after crash |
| `ga_p0_3_new_leader_accepts_writes` | FAIL-5: New leader can accept writes |
| `ga_p0_1_ack_equals_durable` | FAIL-6: ACK'd txns survive crash |
| `ga_p0_1_no_phantom_commit` | FAIL-4: In-flight txns rolled back |
| `ga_p0_2_wal_replay_idempotent` | FAIL-2: No double commit |
| `ga_p0_5_multiple_crash_restart_cycles` | Automated recovery, no human fix |
| `failover_determinism.rs` (9 matrix tests) | Failover matrix: 3 fault × 3 load types |
| `fde1_commit_phase_determines_recovery_outcome` | Commit-phase-at-crash → recovery outcome (FC-1, FC-2, FC-3) |
| `fde2_occ_write_conflict_during_failover` | OCC conflict during failover: no phantom, no duplication |
| `fde3_partition_writes_invisible_after_promote` | Network partition write isolation: 0 leaks (no split-brain) |
| `fde4_indoubt_ttl_bounded_resolution` | In-doubt bounded resolution: 50 txns across 5 cycles, 0 remaining |
| `fde5_idempotent_wal_replay_after_failover` | Idempotent WAL replay: double-ship produces identical state |
| `failover_txn_tests.rs` (SS-01..04, XS-01..08, CH-01..02, ID-01..02) | 16-case failover × txn state machine |

> **See also**: [`docs/failover_partition_sla.md`](failover_partition_sla.md) for quantified SLA
> covering network partition + write conflict + in-doubt resolution bounds.
