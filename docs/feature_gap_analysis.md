# FalconDB Feature Gap Analysis

> Generated: 2026-02-19  
> Scope: Full codebase audit across all 12 crates

---

## 1. DROP INDEX — No-Op (Severity: HIGH)

**Location:** `falcon_executor/src/executor.rs:351-357`

```rust
PhysicalPlan::DropIndex { index_name } => {
    // DROP INDEX — currently a no-op as index names are not tracked
    Ok(ExecutionResult::Ddl {
        message: format!("DROP INDEX {}", index_name),
    })
}
```

**Problem:** `DROP INDEX` silently succeeds without actually removing any index. The storage layer creates indexes by `(table_name, column_idx)` but never associates them with a named index handle, so there is no way to find and remove the correct index.

**Fix:** Add an `index_name → (table_id, column_idx, unique)` mapping in `StorageEngine` (e.g. a `DashMap<String, IndexMeta>`), populate it during `CREATE INDEX`, and use it in a real `drop_index(name)` method.

---

## 2. Authentication — Trust-Only (Severity: HIGH)

**Location:** `falcon_protocol_pg/src/server.rs:295-296`

```rust
// Trust auth: send AuthenticationOk immediately
send_message(&mut stream, &BackendMessage::AuthenticationOk).await?;
```

**Problem:** Every connection is accepted without any password check. The codec already defines `AuthenticationCleartextPassword`, `AuthenticationMd5Password`, and `PasswordMessage`, but none of them are used in the connection handshake. Any client connecting to the port gets full read/write access.

**Fix:** Add a configurable auth mode (`trust` / `password` / `md5`) in `FalconConfig`, store hashed credentials, and implement the challenge-response flow in `handle_client()`.

---

## 3. SAVEPOINT — Name-Only, No Undo (Severity: HIGH)

**Location:** `falcon_protocol_pg/src/handler.rs:682-731`

**Problem:** `SAVEPOINT`, `ROLLBACK TO`, and `RELEASE` only manipulate a `Vec<String>` of savepoint names in the session. No actual transaction state is captured, and `ROLLBACK TO <savepoint>` does **not** undo any DML executed after the savepoint. This means the feature is cosmetically present (no error) but semantically broken.

**Fix:** At each `SAVEPOINT`, snapshot the current write-set. On `ROLLBACK TO`, abort writes performed after the snapshot and restore the prior state. This requires write-set versioning in `StorageEngine` or a nested-transaction model in `TxnManager`.

---

## 4. Missing Data Types (Severity: MEDIUM)

**Location:** `falcon_common/src/types.rs` — `enum DataType`; `falcon_common/src/datum.rs` — `enum Datum`

Currently supported: `Boolean, Int32, Int64, Float64, Text, Timestamp, Date, Array, Jsonb`

**Missing types commonly expected by PG clients and ORMs:**
- **NUMERIC / DECIMAL** — arbitrary-precision numeric, required by financial workloads
- **TIME** — time-of-day without date
- **INTERVAL** — duration/delta (used in date arithmetic)
- **UUID** — native UUID type (currently returned as `Text` from `GEN_RANDOM_UUID()`)
- **BYTEA** — binary data
- **SMALLINT (INT16)** — 2-byte integer
- **REAL (FLOAT32)** — single-precision float

**Impact:** ORMs like SQLAlchemy, Prisma, and JDBC drivers introspect column types. Missing types cause schema creation failures or silent data truncation.

---

## 5. Cancel Request Not Supported (Severity: MEDIUM)

**Location:** `falcon_protocol_pg/src/codec.rs:146-148`

```rust
if version == 80877102 {
    return Err("Cancel request not supported".into());
}
```

**Problem:** PG clients (psql, JDBC, libpq) send cancel requests when the user presses Ctrl+C during a long query. The server rejects these with an error, which causes the client connection to drop.

**Fix:** Parse the `BackendKeyData` (process_id, secret_key) from the cancel request, look up the target session, and set a cancellation flag that the executor checks between row iterations.

---

## 6. No CURSOR / DECLARE / FETCH Support (Severity: MEDIUM)

**Problem:** Server-side cursors (`DECLARE cursor_name CURSOR FOR ...`, `FETCH n FROM cursor_name`) are not implemented. JDBC `setFetchSize()` and many BI tools rely on cursors for large result set streaming.

**Current state:** No parser/binder/executor support for cursor statements. All query results are fully materialized in memory.

---

## 7. No LISTEN / NOTIFY (Severity: LOW)

**Problem:** PG's asynchronous notification mechanism (`LISTEN channel; NOTIFY channel, 'payload'`) is not implemented. Some applications (e.g., Supabase Realtime, Hasura) depend on it for change data capture.

---

## 8. Raft Network — Single-Node Stub (Severity: MEDIUM)

**Location:** `falcon_raft/src/network.rs`

**Problem:** The `NetworkFactory` returns a `NetworkConnection` that fails all RPCs with `Unreachable("single-node mode")`. This means Raft consensus **only works in single-node mode** — no multi-node replication or leader election is possible through the Raft layer.

**Note:** FalconDB has a separate WAL-based streaming replication (primary→replica via gRPC) that does work for multi-node. But the Raft layer cannot be used for multi-node consensus until a real network implementation is provided.

**Fix:** Implement `RaftNetwork` using tonic gRPC to forward `AppendEntries`, `Vote`, and `InstallSnapshot` RPCs to remote nodes.

---

## 9. Cluster Membership — Empty Placeholder (Severity: MEDIUM)

**Location:** `falcon_cluster/src/cluster/membership.rs`

```rust
//! Cluster membership management (placeholder for future node discovery).
//! Currently empty
```

**Problem:** There is no dynamic node join/leave, health checking, or membership management. The cluster topology is static and configured at startup. If a node goes down, there is no automatic failover or re-routing (except the primary→replica WAL-based failover which is manual).

---

## 10. Planner — No Index Scan Planning (Severity: MEDIUM)

**Location:** `falcon_planner/src/planner.rs`

**Problem:** The planner always produces `SeqScan`, `NestedLoopJoin`, or `HashJoin` nodes. There is **no IndexScan plan node**. Index scans are opportunistically applied *inside the executor* (`try_pk_point_lookup`, `try_index_scan_predicate`) after the SeqScan plan has already been chosen.

**Impact:** `EXPLAIN` never shows index usage, the planner cannot cost-compare SeqScan vs IndexScan, and the optimizer has no opportunity to choose between scan strategies.

**Fix:** Add `IndexScan` and `PkPointLookup` as PhysicalPlan variants; move index-eligibility detection from the executor into the planner.

---

## 11. Views — Not WAL-Logged (Severity: MEDIUM)

**Location:** `falcon_storage/src/engine.rs:386-420`

**Problem:** `CREATE VIEW` and `DROP VIEW` modify the in-memory catalog but are **not written to the WAL**. After a crash and recovery, all views are lost. Tables are correctly WAL-logged (`WalRecord::CreateTable`, `WalRecord::DropTable`), but there are no `WalRecord::CreateView` / `WalRecord::DropView` variants.

**Fix:** Add `WalRecord::CreateView { name, query_sql }` and `WalRecord::DropView { name }` variants; emit them in `create_view()` / `drop_view()`.

---

## 12. ALTER TABLE — Not WAL-Logged (Severity: MEDIUM)

**Location:** `falcon_storage/src/engine.rs:422-620`

**Problem:** All `ALTER TABLE` operations (add/drop/rename column, rename table, change column type, set/drop default, set/drop not null) modify the in-memory catalog but produce **no WAL records**. After crash recovery, schema modifications are lost.

**Fix:** Add `WalRecord::AlterTable { table_name, operation_json }` and emit during each alter operation.

---

## 13. Sequences — Not WAL-Logged (Severity: MEDIUM)

**Location:** `falcon_storage/src/engine.rs:491-510` (executor) + `StorageEngine::sequences` DashMap

**Problem:** `CREATE SEQUENCE`, `DROP SEQUENCE`, and `NEXTVAL` calls modify an in-memory `DashMap<String, AtomicI64>` with no WAL durability. Sequence state is lost on restart.

---

## 14. TRUNCATE — Not WAL-Logged (Severity: LOW)

**Location:** `falcon_storage/src/engine.rs:622-635`

**Problem:** `TRUNCATE TABLE` replaces the in-memory `MemTable` but writes no WAL record. If a crash occurs after TRUNCATE, the data may reappear from WAL replay.

---

## 15. Describe for Aggregates/Expressions Returns TEXT OID (Severity: LOW)

**Location:** `falcon_protocol_pg/src/handler.rs:2076-2088`

```rust
BoundProjection::Aggregate(_, _, alias, _, _)
| BoundProjection::Expr(_, alias) => {
    // For aggregates/expressions, default to TEXT type
    FieldDescription { type_oid: 25, /* TEXT */ ... }
}
```

**Problem:** `Describe` for extended query protocol reports all aggregate and expression columns as TEXT (OID 25), even when the actual result is INT64 or FLOAT64. This causes JDBC/Go drivers to mis-parse numeric results.

**Fix:** Infer the result type from the aggregate function and expression type during planning, and propagate it through `FieldDescription`.

---

## 16. No Memory Budget Enforcement (Severity: LOW)

**Location:** `falcon_common/src/config.rs:36` — `memory_limit_bytes: u64`

**Problem:** The config accepts `memory_limit_bytes` but it is never checked anywhere. The storage engine, executor, and join operators will grow unbounded until OOM.

---

## 17. No Background GC Thread (Severity: LOW)

**Problem:** GC is only triggered manually via `SHOW falcon_gc`. There is no automatic periodic GC sweep. Long-running workloads will accumulate stale MVCC versions until `run_gc()` is explicitly called.

**Fix:** Spawn a background tokio task that periodically calls `engine.gc_sweep(txn_mgr.min_active_ts())`.

---

## 18. SSL/TLS Not Implemented (Severity: LOW)

**Location:** `falcon_protocol_pg/src/server.rs` — SSLRequest handling

**Problem:** When a client sends `SSLRequest`, the server responds with `N` (SSL not supported). Encrypted connections are not possible.

---

## Summary by Priority

| Priority | Count | Items |
|----------|-------|-------|
| **HIGH** | 3 | DROP INDEX no-op, Auth trust-only, SAVEPOINT no undo |
| **MEDIUM** | 7 | Missing data types, Cancel request, Cursors, Raft network stub, Membership placeholder, No IndexScan plan node, Views/ALTER/Sequences not WAL-logged |
| **LOW** | 5 | LISTEN/NOTIFY, Describe type inference, Memory budget, Background GC, SSL/TLS, TRUNCATE WAL |
