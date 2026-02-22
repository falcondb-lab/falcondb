# FalconDB Feature Gap Analysis

> Updated: 2026-02-22  
> Scope: Full codebase audit across all 15 crates + Java JDBC driver  
> Status: 2,239 tests passing, 0 failures

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

---

## 1. Missing Data Types (Severity: MEDIUM)

**Location:** `falcon_common/src/types.rs` — `enum DataType`; `falcon_common/src/datum.rs` — `enum Datum`

Currently supported: `Boolean, Int32, Int64, Float64, Decimal, Text, Timestamp, Date, Array, Jsonb`

**Missing types commonly expected by PG clients and ORMs:**
- **TIME** — time-of-day without date
- **INTERVAL** — duration/delta (used in date arithmetic)
- **UUID** — native UUID type (currently returned as `Text` from `GEN_RANDOM_UUID()`)
- **BYTEA** — binary data
- **SMALLINT (INT16)** — 2-byte integer
- **REAL (FLOAT32)** — single-precision float

**Impact:** ORMs like SQLAlchemy, Prisma, and JDBC drivers introspect column types. Missing types cause schema creation failures or silent data truncation.

---

## 2. No CURSOR / DECLARE / FETCH Support (Severity: MEDIUM)

**Problem:** Server-side cursors (`DECLARE cursor_name CURSOR FOR ...`, `FETCH n FROM cursor_name`) are not implemented. JDBC `setFetchSize()` and many BI tools rely on cursors for large result set streaming.

**Current state:** No parser/binder/executor support for cursor statements. All query results are fully materialized in memory.

---

## 3. Raft Network — Single-Node Stub (Severity: MEDIUM)

**Location:** `falcon_raft/src/network.rs`

**Problem:** The `NetworkFactory` returns a `NetworkConnection` that fails all RPCs with `Unreachable("single-node mode")`. Raft consensus only works in single-node mode.

**Note:** FalconDB has a separate WAL-based streaming replication (primary→replica via gRPC) that works for multi-node. The Raft layer is a stub for future use.

**Fix:** Implement `RaftNetwork` using tonic gRPC to forward `AppendEntries`, `Vote`, and `InstallSnapshot` RPCs to remote nodes.

---

## 4. Cluster Membership — Empty Placeholder (Severity: MEDIUM)

**Location:** `falcon_cluster/src/cluster/membership.rs`

**Problem:** No dynamic node join/leave, health checking, or membership management. Cluster topology is static and configured at startup.

---

## 5. SSL/TLS — Config Supported, Runtime Partial (Severity: LOW)

**Location:** `falcon_protocol_pg/src/server.rs` — SSLRequest handling

**Problem:** TLS config is parsed from `falcon.toml` but the SSLRequest handshake path responds with `N` (not supported) unless a cert/key is explicitly configured. Encrypted connections require explicit cert/key setup.

---

## Summary by Priority

| Priority | Count | Items |
|----------|-------|-------|
| **MEDIUM** | 3 | Missing data types (SMALLINT/REAL), No cursors, Raft network stub |
| **LOW** | 1 | SSL/TLS runtime partial |
