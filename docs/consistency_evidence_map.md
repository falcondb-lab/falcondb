# FalconDB — Consistency Evidence Map

> **Purpose**: Enable any third party to verify FalconDB's consistency guarantees in under 10 minutes.
> Every invariant listed below links to its definition in code and a runnable test.

---

## How to Use This Document

1. Pick any invariant row below.
2. Run the **Reproduce** command — it executes in < 30 seconds on any machine with Rust 1.75+.
3. Read the **Plain English** column — no database background required.
4. Optionally inspect the **Code Definition** link for formal semantics.

**Quick full verification** (runs all consistency tests, ~60 s):

```bash
cargo test -p falcon_storage --test consistency_wal -- --nocapture
cargo test -p falcon_cluster --test consistency_replication -- --nocapture
cargo test -p falcon_common --lib consistency -- --nocapture
```

---

## Core Promise: Deterministic Commit Guarantee (DCG)

> **"If FalconDB told your application 'committed', that data will survive any single-node crash, any failover, and any recovery — no exceptions, no caveats."**

This is not a marketing claim. It is a testable engineering property composed of the invariants below.

---

## 1. Commit Point Invariants

These define *when* a transaction becomes irrevocable.

| ID | Invariant (Plain English) | Formal Name | Code Definition | Test File | Test Function | Reproduce |
|----|--------------------------|-------------|-----------------|-----------|---------------|-----------|
| **CP-1** | A transaction goes through three stages in strict order: Logical → Durable → Client-Visible. It can never skip ahead. | `CommitPoint` ordering | `crates/falcon_common/src/consistency.rs` §1 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_consistency_validation_helpers` | `cargo test -p falcon_cluster --test consistency_replication test_consistency_validation_helpers` |
| **CP-2** | The client is never told "success" before the data is crash-safe on disk (under the active durability policy). | No ACK before durable | `crates/falcon_common/src/consistency.rs` §1–§2 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_consistency_validation_helpers` | `cargo test -p falcon_cluster --test consistency_replication test_consistency_validation_helpers` |
| **CP-4** | After the WAL fsync completes, the transaction survives any crash. Period. | Durable = permanent | `crates/falcon_common/src/consistency.rs` §1 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal4_wal5_replay_convergence_and_completeness` | `cargo test -p falcon_storage --test consistency_wal test_wal4_wal5` |

---

## 2. WAL (Write-Ahead Log) Invariants

These ensure the WAL — the crash-safety backbone — behaves correctly.

| ID | Invariant (Plain English) | Formal Name | Code Definition | Test File | Test Function | Reproduce |
|----|--------------------------|-------------|-----------------|-----------|---------------|-----------|
| **WAL-1** | Every WAL entry gets a unique, ever-increasing sequence number. No gaps, no duplicates. | Unique monotonic LSN | `crates/falcon_common/src/consistency.rs` §4 `wal_invariants` | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal1_unique_monotonic_lsn` | `cargo test -p falcon_storage --test consistency_wal test_wal1` |
| **WAL-2** | Replaying the WAL twice produces the exact same state as replaying it once. Safe to retry recovery. | Idempotent replay | `crates/falcon_common/src/consistency.rs` §4 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal2_idempotent_replay` | `cargo test -p falcon_storage --test consistency_wal test_wal2` |
| **WAL-3** | Commit timestamps in the WAL are always increasing. No time travel. | Monotonic commit sequence | `crates/falcon_common/src/consistency.rs` §4 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal3_monotonic_commit_sequence` | `cargo test -p falcon_storage --test consistency_wal test_wal3` |
| **WAL-4** | Recovery always produces the same result, no matter how many times you run it. | Replay convergence | `crates/falcon_common/src/consistency.rs` §4 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal4_wal5_replay_convergence_and_completeness` | `cargo test -p falcon_storage --test consistency_wal test_wal4_wal5` |
| **WAL-5** | After crash recovery: committed transactions are visible, uncommitted are gone. No exceptions. | Recovery completeness | `crates/falcon_common/src/consistency.rs` §4 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal4_wal5_replay_convergence_and_completeness` | `cargo test -p falcon_storage --test consistency_wal test_wal4_wal5` |
| **WAL-6** | Every WAL record has a CRC32 checksum. Corrupted data stops recovery — it never silently skips bad data. | Checksum integrity | `crates/falcon_common/src/consistency.rs` §4 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_wal6_checksum_corruption_halts_recovery` | `cargo test -p falcon_storage --test consistency_wal test_wal6` |

---

## 3. Crash Recovery Invariants

These define what happens when the process crashes at different points during a commit.

| ID | Invariant (Plain English) | Crash Point | Code Definition | Test File | Test Function | Reproduce |
|----|--------------------------|-------------|-----------------|-----------|---------------|-----------|
| **CRASH-1** | If the system crashes before writing anything to the WAL, the transaction simply doesn't exist. | Before WAL write | `crates/falcon_common/src/consistency.rs` §5 `CrashPoint` | `crates/falcon_storage/tests/consistency_wal.rs` | `test_crash_before_wal_write` | `cargo test -p falcon_storage --test consistency_wal test_crash_before_wal_write` |
| **CRASH-2** | If the system crashes after writing data but before the commit record, the transaction is rolled back. Your data is cleanly gone. | After write, before commit | `crates/falcon_common/src/consistency.rs` §5 | `crates/falcon_storage/tests/consistency_wal.rs` | `test_crash_after_wal_write_before_commit` | `cargo test -p falcon_storage --test consistency_wal test_crash_after_wal_write_before_commit` |
| **CRASH-3** | Updates and deletes are also correctly recovered — not just inserts. | Mixed DML recovery | — | `crates/falcon_storage/tests/consistency_wal.rs` | `test_crash_recovery_with_updates_and_deletes` | `cargo test -p falcon_storage --test consistency_wal test_crash_recovery_with_updates_and_deletes` |
| **CRASH-4** | Recovery works correctly even after a checkpoint — data from both the checkpoint and subsequent WAL entries is restored. | Checkpoint + WAL delta | — | `crates/falcon_storage/tests/consistency_wal.rs` | `test_checkpoint_plus_wal_recovery` | `cargo test -p falcon_storage --test consistency_wal test_checkpoint_plus_wal_recovery` |
| **CRASH-5** | DDL operations (CREATE TABLE, DROP TABLE) are also recovered correctly from WAL. | DDL recovery | — | `crates/falcon_storage/tests/consistency_wal.rs` | `test_ddl_recovery_create_and_drop` | `cargo test -p falcon_storage --test consistency_wal test_ddl_recovery_create_and_drop` |
| **CRASH-6** | If two transactions are interleaved and only one commits, recovery shows only the committed one. | Interleaved txn isolation | — | `crates/falcon_storage/tests/consistency_wal.rs` | `test_interleaved_txn_recovery` | `cargo test -p falcon_storage --test consistency_wal test_interleaved_txn_recovery` |

---

## 4. Replication Invariants

These ensure replicas never contradict the primary.

| ID | Invariant (Plain English) | Formal Name | Code Definition | Test File | Test Function | Reproduce |
|----|--------------------------|-------------|-----------------|-----------|---------------|-----------|
| **REP-1** | A replica's data is always a subset of the primary's data. A replica never has something the primary doesn't. | Prefix property | `crates/falcon_common/src/consistency.rs` §6 `replication_invariants` | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_rep1_prefix_property` | `cargo test -p falcon_cluster --test consistency_replication test_rep1` |
| **REP-2** | A replica never invents transactions. Every committed row on the replica also exists on the primary. | No phantom commits | `crates/falcon_common/src/consistency.rs` §6 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_rep2_no_phantom_commits` | `cargo test -p falcon_cluster --test consistency_replication test_rep2` |
| **REP-3** | WAL entries are applied on the replica in the exact same order as the primary wrote them. | Strict ordering | `crates/falcon_common/src/consistency.rs` §6 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_rep3_strict_ordering` | `cargo test -p falcon_cluster --test consistency_replication test_rep3` |

---

## 5. Failover Invariants

These ensure that promoting a replica to primary never creates data out of thin air.

| ID | Invariant (Plain English) | Formal Name | Code Definition | Test File | Test Function | Reproduce |
|----|--------------------------|-------------|-----------------|-----------|---------------|-----------|
| **FAIL-1** | After failover, the new primary's data is a subset of the old primary's data. Failover never creates data. | Commit set containment | `crates/falcon_common/src/consistency.rs` §7 `failover_invariants` | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_fail1_commit_set_containment_after_promote` | `cargo test -p falcon_cluster --test consistency_replication test_fail1` |
| **FAIL-2** | Transactions that were confirmed to the client survive failover (when replicas were caught up). | ACK'd txns survive | `crates/falcon_common/src/consistency.rs` §7 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_fail2_acked_txns_survive_failover` | `cargo test -p falcon_cluster --test consistency_replication test_fail2` |
| **FAIL-3** | After failover, the old primary is fenced — it cannot accept new writes. Stale epoch numbers are rejected. | Epoch fencing | `crates/falcon_common/src/consistency.rs` §7 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_fail3_epoch_fencing` | `cargo test -p falcon_cluster --test consistency_replication test_fail3` |
| **FAIL-4** | Data survives multiple sequential failovers, not just one. | Multi-failover durability | — | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_multiple_failovers_maintain_data` | `cargo test -p falcon_cluster --test consistency_replication test_multiple_failovers` |

---

## 6. Cross-Shard Transaction Invariants

These ensure distributed transactions are atomic — all or nothing, across shards.

| ID | Invariant (Plain English) | Formal Name | Code Definition | Test File | Test Function | Reproduce |
|----|--------------------------|-------------|-----------------|-----------|---------------|-----------|
| **XS-1** | A cross-shard transaction either commits on ALL shards or aborts on ALL. No shard gets left in an inconsistent state. | Atomicity | `crates/falcon_common/src/consistency.rs` §8 `cross_shard_invariants` | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_xs1_cross_shard_atomicity_commit`, `test_xs1_cross_shard_atomicity_abort` | `cargo test -p falcon_cluster --test consistency_replication test_xs1` |
| **XS-2** | A transaction cannot commit twice, even if the coordinator retries. | At-most-once commit | `crates/falcon_common/src/consistency.rs` §8 | `crates/falcon_cluster/tests/consistency_replication.rs` | `test_xs2_at_most_once_commit` | `cargo test -p falcon_cluster --test consistency_replication test_xs2` |

---

## 7. Commit Policy Invariants

These define what must happen before the client is told "success" under each durability policy.

| ID | Policy | What Happens Before Client ACK | Crash Loss Window | Code Definition |
|----|--------|-------------------------------|-------------------|-----------------|
| **POL-1** | `LocalWalSync` (default) | WAL fsync to local disk | **Zero** — data is on disk | `crates/falcon_common/src/consistency.rs` §2 |
| **POL-2** | `PrimaryPlusReplicaAck(N)` | Local fsync **AND** N replica ACKs | **Zero** if ≥1 replica survives | `crates/falcon_common/src/consistency.rs` §2 |
| **POL-3** | `PrimaryWalOnly` | WAL buffer write (no fsync) | Up to `flush_interval_us` of data | `crates/falcon_common/src/consistency.rs` §2 |

**Key rule (POL-1 in code)**: The system MUST NOT send a client ACK until ALL I/O required by the active policy has completed. This is the core of the Deterministic Commit Guarantee.

---

## 8. Read Consistency Invariants

| ID | Invariant (Plain English) | Formal Name | Code Definition |
|----|--------------------------|-------------|-----------------|
| **READ-1** | Under Read Committed, each SQL statement sees only data that was committed before it started. | Read Committed | `crates/falcon_common/src/consistency.rs` §9 |
| **READ-2** | Under Snapshot Isolation, the entire transaction sees a frozen snapshot from when it began. No mid-transaction surprises. | Snapshot Isolation | `crates/falcon_common/src/consistency.rs` §9 |

---

## 9. Error Classification Invariants

| ID | Invariant (Plain English) | Code Definition |
|----|--------------------------|-----------------|
| **ERR-1** | The client can always tell the difference between "committed", "aborted", and "unknown" from the error code alone. | `crates/falcon_common/src/consistency.rs` §10 `pg_error_codes` |
| **ERR-2** | No consistency-critical error uses a generic internal error code. Each maps to a specific PostgreSQL SQLSTATE. | `crates/falcon_common/src/consistency.rs` §10 |

---

## 10. Commit Phase Tracking (Runtime Safety Net)

FalconDB tracks each transaction's commit phase at runtime to prevent regressions.

| Phase | Code | Meaning |
|-------|------|---------|
| CP-L | `WalLogged` | WAL record written to memory |
| CP-D | `WalDurable` | WAL fsync'd to disk |
| CP-V | `Visible` | Transaction state visible to readers |
| CP-A | `Acknowledged` | Client received success response |

**Enforcement** (`crates/falcon_cluster/src/stability_hardening.rs`):
- Phases are **forward-only** — any attempt to move backward is rejected with `InvariantViolation`.
- Once past CP-V, the outcome is **irreversible** — the system cannot roll it back.
- Duplicate commit signals are idempotent (no double-commit risk).

---

## Verification Summary

| Category | Invariants | Tests | Command |
|----------|-----------|-------|---------|
| WAL | WAL-1 through WAL-6 | 10 | `cargo test -p falcon_storage --test consistency_wal` |
| Crash Recovery | CRASH-1 through CRASH-6 | 10 | `cargo test -p falcon_storage --test consistency_wal` |
| Replication | REP-1 through REP-3 | 3 | `cargo test -p falcon_cluster --test consistency_replication` |
| Failover | FAIL-1 through FAIL-4 | 4 | `cargo test -p falcon_cluster --test consistency_replication` |
| Cross-Shard | XS-1 through XS-2 | 3 | `cargo test -p falcon_cluster --test consistency_replication` |
| Commit Points | CP-1, CP-2, CP-4 | 1 | `cargo test -p falcon_cluster --test consistency_replication test_consistency_validation` |
| **Total** | **24 invariants** | **31 tests** | **~60 seconds** |

All consistency tests pass in CI on every commit. See `.github/workflows/ci.yml`.
