# Crash Point Behavior Matrix — GA Normative Reference

> **This document is normative.** Every row has a corresponding automated test.
> If docs and code disagree, it is a bug.

---

## 1. Commit Point Model (recap)

```
CP-L (LogicalCommit) ≤ CP-D (DurableCommit) ≤ CP-V (ClientVisibleCommit)
```

- **CP-L**: Transaction marked committed in MVCC (in-memory).
- **CP-D**: WAL record fsynced per active `CommitPolicy`.
- **CP-V**: Client receives success response.

**Code**: `falcon_common::consistency::CommitPoint`

---

## 2. Crash Point Matrix

| ID | Crash Point | WAL State at Crash | Recovery Behavior | Client Outcome | Test |
|----|-------------|-------------------|-------------------|----------------|------|
| **CP-1** | Before WAL write | No WAL records for txn | Txn does not exist | Connection lost; txn never started | `ga_p0_2_crash_before_wal_write` |
| **CP-2** | After WAL insert, before CommitTxn record | Insert records exist, no commit record | Txn rolled back (writes discarded) | Connection lost; txn NOT committed | `ga_p0_2_crash_after_insert_before_commit` |
| **CP-3** | After CommitTxn record written, before fsync | Commit record in buffer, not on disk | **May lose txn** (policy-dependent) | Connection lost; txn status indeterminate | See §3 below |
| **CP-4** | After CommitTxn record fsynced | Commit record durable on disk | **Txn MUST survive** | Connection lost; txn IS committed | `ga_p0_2_crash_after_commit_fsynced` |
| **CP-5** | After client ACK sent | Full commit path complete | Txn durable and visible | Client knows committed | `ga_p0_1_ack_equals_durable` |

### CP-3 Detail: Policy-Dependent Behavior

| CommitPolicy | CP-3 Behavior | Data Loss Window |
|-------------|---------------|-----------------|
| `LocalWalSync` | **Cannot occur** — fsync before ACK | 0 |
| `PrimaryWalOnly` | Txn MAY be lost | Up to `flush_interval_us` |
| `PrimaryPlusReplicaAck` | Txn survives if ≥1 replica has it | 0 (if replica alive) |
| `RaftMajority` ¹ | Txn survives if majority has it | 0 (if majority alive) |

¹ `RaftMajority` is defined in code but **not available in production** — requires Raft consensus (not implemented).

**Code**: `falcon_common::consistency::CommitPolicy`

---

## 3. Abort / In-Flight Crash Points

| ID | Scenario | Recovery Behavior | Test |
|----|----------|-------------------|------|
| **AB-1** | Explicit `ABORT` before crash | Txn rolled back; writes discarded | `ga_p0_1_txn_terminal_states_from_wal` |
| **AB-2** | In-flight txn (no commit/abort) at crash | Txn rolled back; writes discarded | `ga_p0_1_no_phantom_commit` |
| **AB-3** | Multiple in-flight txns interleaved | Each independently resolved by presence/absence of commit record | `ga_p0_2_multi_table_interleaved_recovery` |

---

## 4. DDL Crash Points

| ID | Scenario | Recovery Behavior | Test |
|----|----------|-------------------|------|
| **DDL-1** | CREATE TABLE + data, then crash | Table + data recovered | `ga_p0_5_ddl_survives_restart` |
| **DDL-2** | CREATE TABLE + DROP TABLE, then crash | Table does NOT exist after recovery | `ga_p0_5_ddl_survives_restart` |
| **DDL-3** | CREATE TABLE (no flush), crash | Table may or may not exist depending on WAL buffer state | `test_wal6_checksum_corruption_halts_recovery` |

---

## 5. Checkpoint + WAL Delta Crash Points

| ID | Scenario | Recovery Behavior | Test |
|----|----------|-------------------|------|
| **CK-1** | Checkpoint taken, post-checkpoint data committed, crash | All data recovered (checkpoint + WAL delta) | `ga_p0_2_checkpoint_plus_delta_recovery` |
| **CK-2** | Checkpoint taken, post-checkpoint data uncommitted, crash | Checkpoint data recovered; uncommitted data discarded | `test_checkpoint_plus_wal_recovery` |

---

## 6. Invariants

| ID | Invariant | Enforcement |
|----|-----------|-------------|
| **INV-1** | No committed txn is lost after CP-D | WAL fsync before ACK |
| **INV-2** | No uncommitted txn becomes visible (no phantom commit) | Recovery discards writes without CommitTxn record |
| **INV-3** | WAL replay is idempotent (N replays → same state) | `ga_p0_2_wal_replay_idempotent` (7 replays) |
| **INV-4** | At-most-once commit (no double commit) | `ga_p0_1_at_most_once_commit` (5 replays) |
| **INV-5** | Recovery requires zero human intervention | `ga_p0_5_multiple_crash_restart_cycles` (5 cycles) |

---

## 7. Test Coverage Summary

| GA Gate | Tests | Status |
|---------|-------|--------|
| P0-1: Transaction correctness | 4 | `ga_release_gate.rs` |
| P0-2: WAL durability + crash recovery | 7 | `ga_release_gate.rs` |
| P0-3: Failover transaction behavior | 2 | `ga_release_gate.rs` |
| P0-4: Memory safety (OOM) | 3 | `ga_release_gate.rs` |
| P0-5: Start/Stop/Restart safety | 3 | `ga_release_gate.rs` |
| **Total GA gate tests** | **19** | |
| Pre-existing WAL consistency tests | 10 | `consistency_wal.rs` |
| Pre-existing memory backpressure tests | 16 | `memory_backpressure.rs` |

**Run**: `cargo test --test ga_release_gate -- --test-threads=1`
