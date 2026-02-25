# GA Release Gate Checklist

> **Purpose**: Single-page Go/No-Go checklist for GA (stable) release.
> Every row must be ✅ before tagging a GA release.

---

## P0 — Must Pass (blocks release)

| ID | Requirement | Acceptance Criteria | Evidence | Status |
|----|-------------|-------------------|----------|--------|
| **P0-1a** | ACK = Durable | Committed txn survives any single crash | `ga_p0_1_ack_equals_durable` | ✅ |
| **P0-1b** | No Phantom Commit | Uncommitted txn never visible after recovery | `ga_p0_1_no_phantom_commit` | ✅ |
| **P0-1c** | At-most-once Commit | WAL replayed 5× → identical state | `ga_p0_1_at_most_once_commit` | ✅ |
| **P0-1d** | Terminal States | Committed/Aborted/In-flight correctly resolved | `ga_p0_1_txn_terminal_states_from_wal` | ✅ |
| **P0-2a** | Crash before WAL | Txn does not exist | `ga_p0_2_crash_before_wal_write` | ✅ |
| **P0-2b** | Crash after insert, before commit | Txn rolled back | `ga_p0_2_crash_after_insert_before_commit` | ✅ |
| **P0-2c** | Crash after commit fsynced | Txn survives | `ga_p0_2_crash_after_commit_fsynced` | ✅ |
| **P0-2d** | Idempotent replay | 7 recoveries → identical | `ga_p0_2_wal_replay_idempotent` | ✅ |
| **P0-2e** | Multi-table interleaved | Correct per-txn resolution | `ga_p0_2_multi_table_interleaved_recovery` | ✅ |
| **P0-2f** | Update/Delete recovery | Correct final state | `ga_p0_2_update_delete_recovery` | ✅ |
| **P0-2g** | Checkpoint + delta | Both recovered | `ga_p0_2_checkpoint_plus_delta_recovery` | ✅ |
| **P0-3a** | Failover: only committed survive | In-flight txns discarded | `ga_p0_3_failover_only_committed_survive` | ✅ |
| **P0-3b** | New leader accepts writes | Recovery → write → commit works | `ga_p0_3_new_leader_accepts_writes` | ✅ |
| **P0-4a** | Memory pressure states | Normal/Pressure/Critical correct | `ga_p0_4_memory_pressure_states` | ✅ |
| **P0-4b** | Memory tracker accounting | alloc/dealloc/total correct | `ga_p0_4_memory_tracker_accounting` | ✅ |
| **P0-4c** | Global governor backpressure | 4-tier: None/Soft/Hard/Emergency | `ga_p0_4_global_governor_backpressure` | ✅ |
| **P0-5a** | Clean shutdown + restart | WAL flushed, all data recovered | `ga_p0_5_clean_shutdown_restart` | ✅ |
| **P0-5b** | Multiple crash-restart cycles | 5 cycles, data consistent | `ga_p0_5_multiple_crash_restart_cycles` | ✅ |
| **P0-5c** | DDL survives restart | CREATE + DROP table correct | `ga_p0_5_ddl_survives_restart` | ✅ |

**Run**: `cargo test -p falcon_storage --test ga_release_gate -- --test-threads=1`

---

## P1 — Should Pass (documented exception allowed)

| ID | Requirement | Acceptance Criteria | Evidence | Status |
|----|-------------|-------------------|----------|--------|
| **P1-1a** | SQL whitelist tested | DML, JOIN, GROUP BY, UPSERT, RETURNING | handler tests (232+) | ✅ |
| **P1-1b** | Unsupported → clear error | SQLSTATE `0A000` | `test_unsupported_create_trigger_error` | ✅ |
| **P1-1c** | Not-supported list published | `docs/ga_sql_boundary.md` §2 | Document exists | ✅ |
| **P1-2a** | SHOW memory | Returns data | `test_show_memory_stats` | ✅ |
| **P1-2b** | SHOW nodes | Returns data | `test_show_nodes_stats` | ✅ |
| **P1-2c** | SHOW replication | Returns data | `test_show_replication_stats` | ✅ |
| **P1-2d** | Structured logging | tracing with level/target | Code audit | ✅ |

---

## Pre-existing Gates (must not regress)

| Gate | Script | Status |
|------|--------|--------|
| WAL consistency (WAL-1..6, CRASH-1..5) | `cargo test --test consistency_wal` | ✅ |
| Memory backpressure (16 tests) | `cargo test --test memory_backpressure` | ✅ |
| Isolation (35 SI litmus tests) | `scripts/ci_isolation_gate.sh` | ✅ |
| Failover determinism (9 matrix) | `cargo test --test failover_determinism` | ✅ |
| Replication integrity (5 tests) | `cargo test --test replication_integrity` | ✅ |
| Full workspace | `cargo test --workspace` | ✅ |
| Clippy zero warnings | `cargo clippy --workspace -- -D warnings` | ✅ |

---

## Documentation Deliverables

| Document | Path | Status |
|----------|------|--------|
| Crash point behavior matrix | `docs/crash_point_matrix.md` | ✅ |
| Failover behavior | `docs/failover_behavior.md` | ✅ |
| SQL boundary + not-supported | `docs/ga_sql_boundary.md` | ✅ |
| Consistency contract | `docs/CONSISTENCY.md` | ✅ |
| GA hardening guide | `docs/ga_hardening.md` | ✅ |
| Memory backpressure | `docs/memory_backpressure.md` | ✅ |
| Operability baseline | `docs/operability_baseline.md` | ✅ |

---

## CI Gate

**Unified script**: `scripts/ci_ga_release_gate.sh`

```bash
# Full gate (recommended):
./scripts/ci_ga_release_gate.sh

# Fast gate (skip perf + failover):
FALCON_SKIP_PERF=1 FALCON_SKIP_FAILOVER=1 ./scripts/ci_ga_release_gate.sh
```

---

## Sign-off

| Role | Name | Date | Verdict |
|------|------|------|---------|
| Engineering Lead | | | GO / NO-GO |
| QA | | | GO / NO-GO |
| Product | | | GO / NO-GO |
