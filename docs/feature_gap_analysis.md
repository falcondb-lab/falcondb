# FalconDB Feature Gap Analysis

> Updated: 2026-02-24  
> Scope: Full codebase audit across all 16 crates + Java JDBC driver  
> Status: 2,700+ tests passing, 0 failures

---

## ✅ Previously Reported — Now Fixed

The following items from the original gap analysis have been implemented:

| Item | Status |
|------|--------|
| DROP INDEX no-op | ✅ Fixed — `StorageEngine::drop_index()` removes from `index_registry` + MemTable |
| Authentication trust-only | ✅ Fixed — Trust/Password/MD5/SCRAM-SHA-256 all implemented in `server.rs` |
| SAVEPOINT no undo | ✅ Fixed — `write_set_snapshot`/`rollback_write_set_after` implemented |
| Views not WAL-logged | ✅ Fixed — `WalRecord::CreateView`/`DropView` emitted and replayed |
| ALTER TABLE not WAL-logged | ✅ Fixed — `WalRecord::AlterTable { operation_json }` for all ops |
| Sequences not WAL-logged | ✅ Fixed — `WalRecord::CreateSequence`/`DropSequence`/`SetSequenceValue` |
| TRUNCATE not WAL-logged | ✅ Fixed — `WalRecord::TruncateTable` emitted and replayed |
| No IndexScan plan node | ✅ Fixed — `PhysicalPlan::IndexScan`, `Planner::plan_with_indexes`, `try_index_scan_plan` |
| Background GC thread | ✅ Fixed — `GcRunner` spawned in `falcon_server/src/main.rs` |
| LISTEN/NOTIFY | ✅ Fixed — `NotificationHub` shared across sessions |
| Cancel request not supported | ✅ Fixed — `CancellationRegistry` + `BackendKeyData` + cancel polling |
| Describe type inference | ✅ Fixed — aggregate/expression OID inferred from type |
| CREATE INDEX not WAL-logged | ✅ Fixed (2026-02-21) — `WalRecord::CreateIndex`/`DropIndex` emitted + replayed |
| NUMERIC / DECIMAL type missing | ✅ Fixed (2026-02-21) — `Datum::Decimal(i128, u8)` + `DataType::Decimal(u8, u8)`, 11 tests |
| CHECK constraint not enforced at runtime | ✅ Fixed (2026-02-21) — `CheckConstraintViolation` error + SQLSTATE `23514`, enforced in INSERT/UPDATE/INSERT SELECT |
| No composite / covering / prefix indexes | ✅ Fixed (2026-02-21) — `SecondaryIndex::column_indices`, `new_composite/covering/prefix()`, `prefix_scan()`, 10 tests |
| Transaction READ ONLY / timeout not enforced | ✅ Fixed (2026-02-21) — `TxnHandle::read_only`, `timeout_ms`, `exec_summary`; DML guards in executor |
| No RBAC / schema-level privilege management | ✅ Fixed (2026-02-21) — `RoleCatalog` (transitive inheritance), `PrivilegeManager` (GRANT/REVOKE, schema defaults), 17 tests |
| Logical replication / CDC | ✅ Fixed (2026-02-22) — `CdcManager` with replication slots, INSERT/UPDATE/DELETE/COMMIT events, bounded ring buffer, 9 tests |
| Row-level security | ✅ Fixed (2026-02-22) — `RlsPolicyManager` with permissive/restrictive policies, role-scoped targeting, 15 tests |
| Partitioned tables | ✅ Fixed (2026-02-22) — `PartitionManager` with Range/Hash/List strategies, routing, pruning, 10 tests |
| No memory budget enforcement | ✅ Fixed (2026-02-22) — `UnifiedMemoryBudget` with 5 categories, 3 escalation levels, 10 tests |
| No native protocol / JDBC driver | ✅ Fixed (2026-02-22) — `falcon_protocol_native` + `falcon_native_server` (67 tests) + Java JDBC driver with HA failover |
| Cluster Membership — empty placeholder | ✅ Fixed (2026-02-24) — `MembershipManager` with NodeState machine (Joining→Active→Draining→Leaving→Dead), epoch/version monotonicity, quorum checks, heartbeat, dead-node detection, observability metrics. 23 tests |
| SSL/TLS — runtime partial | ✅ Fixed (2026-02-24) — `tls.rs` module with `TlsConfig`, `TlsAcceptor` via tokio-rustls, `PgStream` (plain/TLS), SSLRequest negotiation with stream upgrade, `require_ssl` enforcement (TLS-1/TLS-2/TLS-3 invariants). 9 tests |
| No CURSOR / DECLARE / FETCH | ✅ Fixed (2026-02-24) — `CursorStream` with `ChunkedRows` for streaming FETCH, `CursorState` uses streaming API, MOVE FORWARD/BACKWARD support. 11 tests |

---

## 1. Missing Data Types (Severity: MEDIUM)

**Location:** `falcon_common/src/types.rs` — `enum DataType`; `falcon_common/src/datum.rs` — `enum Datum`

Currently supported: `Boolean, Int32, Int64, Float64, Decimal, Text, Timestamp, Date, Time, Interval, Uuid, Bytea, Array, Jsonb`

**Missing types commonly expected by PG clients and ORMs:**
- **SMALLINT (INT16)** — 2-byte integer
- **REAL (FLOAT32)** — single-precision float

**Impact:** Low — most ORMs map SMALLINT→INT32 and REAL→FLOAT64 transparently.

---

## 2. Raft Network — Single-Node Stub (Severity: LOW)

**Location:** `falcon_raft/src/network.rs`

**Problem:** The `NetworkFactory` returns a `NetworkConnection` that fails all RPCs with `Unreachable("single-node mode")`. Raft consensus only works in single-node mode.

**Note:** FalconDB has a separate WAL-based streaming replication (primary→replica via gRPC) that works for multi-node. The Raft layer is a stub for future use. The new `MembershipManager` handles cluster membership independently of Raft.

---

## Summary by Priority

| Priority | Count | Items |
|----------|-------|-------|
| **MEDIUM** | 1 | Missing data types (SMALLINT/REAL) |
| **LOW** | 1 | Raft network stub (streaming replication works independently) |
