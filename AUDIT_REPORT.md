# FalconDB Code Audit Report (Round 2)

> Scope: all 18 crates under `crates/`, focusing on production code paths.
> Previous fixes (P0–P2) already applied: OCC validation order, memory backpressure, group commit error propagation, startup handshake timeout, WAL recovery logging.

---

## P0 — Data Correctness / Safety

### 1. Abort WAL record silently discarded on validation failure
**Files**: `engine.rs:1835,1849,1882,1896`
**Issue**: When `pre_validate_write_set` or `apply_commit_to_write_set` fails, the abort WAL record is written with `let _ = self.append_wal(...)`. If the WAL append itself fails (disk full, I/O error), the abort is lost. On recovery, the transaction's insert records exist in WAL without a corresponding abort — they will be replayed and committed, causing **phantom rows**.
**Severity**: Data corruption on crash after WAL-full abort path.
**Fix**: Log a `tracing::error!` on failure; consider panic if abort WAL is critical for correctness.

### 2. `TokenBucket::set_rate()` is a no-op
**File**: `falcon_cluster/src/token_bucket.rs:226-232`
**Issue**: The method accepts `rate_per_sec` but immediately discards it (`let _ = rate_per_sec;`). Any runtime rate adjustment (rebalance throttling, operator override) silently does nothing.
**Severity**: Functional bug — rate limiting cannot be dynamically tuned.
**Fix**: Add interior mutability (`AtomicU64` or `Mutex`) for `config.rate_per_sec`.

### 3. `max_connections` check races with handshake
**File**: `server.rs:1119-1135`
**Issue**: `active_connections` is incremented at `spawn_connection` (line 427) but `max_connections` is checked post-handshake (line 1119). During the 30s handshake window, the counter is inflated. Concurrent arrivals can all pass the spawn gate, then all fail post-handshake — wasting resources.
**Severity**: Medium — DoS amplification. Many connections consume handshake resources before rejection.
**Fix**: Add a pre-handshake check or use a semaphore to cap in-flight handshakes.

---

## P1 — Error Handling / Robustness

### 4. Pervasive `.unwrap()` in production code
**Scope**: ~300+ occurrences in `falcon_storage`, ~60+ in `falcon_cluster`, ~40 in `falcon_protocol_pg`.
**High-risk locations**:
- `wal.rs:985,998`: `try_into().unwrap()` in WAL record boundary parsing — panics on corrupted WAL data.
- `lsm/engine.rs`: 62 unwraps in LSM engine paths.
- `encryption.rs`: 52 unwraps in crypto code paths.
- `membership.rs`: 41 unwraps in cluster membership.
- `async_wal_writer.rs`: 37 unwraps in async WAL I/O.
**Severity**: Any panic in a server task crashes the connection handler; panics in shared paths (WAL, GC) can crash the process.
**Fix**: Audit top-20 files by unwrap count; replace with `?` or `.ok_or()` in I/O and parsing paths.

### 5. `ResourceMeter` / `TenantRegistry` DashMaps grow unbounded
**Files**: `metering.rs:138-139`, `tenant_registry.rs:99`
**Issue**: `ResourceMeter.counters` and `TenantRegistry.tenants` are `DashMap`s with no eviction or cleanup mechanism. In multi-tenant deployments, tenants that disconnect or are deleted leave entries forever.
**Severity**: Memory leak proportional to tenant churn.
**Fix**: Add periodic cleanup or TTL-based eviction.

### 6. WAL `find_records_boundary` / `next_record_len` panic on corrupt data
**File**: `wal.rs:985,998`
**Issue**: `data[pos..pos+4].try_into().unwrap()` will panic if the WAL segment is truncated or contains garbage. These are called during recovery and flush — a corrupted WAL segment crashes the server instead of returning an error.
**Severity**: Crash on corrupt WAL (should be graceful degradation).
**Fix**: Replace with `try_into().ok()?` or return an error.

### 7. Silent `let _` on catalog DDL during recovery
**File**: `engine.rs:2371,2674,2696`
**Issue**: `let _ = catalog.drop_database(name)`, `let _ = catalog.drop_schema(name)`, `let _ = catalog.drop_role(name)` during WAL recovery. Unlike the DML paths (already fixed with `tracing::debug!`), these DDL replay errors are still silently discarded.
**Severity**: Low — DDL replay is idempotent, but a log message would help debugging.
**Fix**: Add `tracing::debug!` like the DML paths.

---

## P2 — Security

### 8. `bind_params` string interpolation is fragile
**File**: `server.rs:2070-2114`
**Issue**: The `bind_params` function does manual string interpolation of `$1`, `$2` parameters into SQL by single-quote escaping. It only escapes `'` → `''`. This is used as a fallback path when `ps.plan.is_none()`. While FalconDB uses a proper parser/binder for execution, the interpolated string is passed to `handle_query_inner` which re-parses it — so injection risk is low. However, the escaping is incomplete (no handling of backslash escapes in `standard_conforming_strings=off` mode).
**Severity**: Low — defense-in-depth concern, not exploitable in current code path.
**Fix**: Document that this path relies on `standard_conforming_strings=on` (which FalconDB always sets).

### 9. No per-IP rate limiting on auth failures
**File**: `server.rs:844,904,1025-1039`
**Issue**: Failed authentication attempts (wrong password, SCRAM failure) return FATAL and close the connection, but there's no tracking of repeated failures from the same IP. An attacker can brute-force passwords at connection rate.
**Severity**: Medium — auth brute-force possible without detection.
**Fix**: Add a per-IP failure counter with exponential backoff or temporary ban.

---

## P3 — Performance / Architecture

### 10. `OwnedRow::clone()` on every MVCC read
**Files**: `mvcc.rs:122,129,136,151,165,169,218,226,258,279`
**Issue**: Every `read_committed`, `read_for_txn`, `commit_and_report`, and `abort_and_report` clones `OwnedRow` (heap-allocated `Vec<Datum>`). For read-heavy workloads, this is a major allocation source.
**Severity**: Performance — significant allocation pressure on read paths.
**Fix**: Consider `Arc<OwnedRow>` for zero-copy reads, or a `Cow`-based approach.

### 11. `VersionChain::commit_and_report` traverses entire chain
**File**: `mvcc.rs:212-230`
**Issue**: Even for single-version chains (common case: autocommit INSERT), the method traverses all versions to find `old_data` for secondary index updates. The `commit_no_report` fast path exists but is only used in the single-key commit optimization.
**Severity**: Minor — mitigated by `commit_no_report` for the common case.

### 12. 14 `unsafe` blocks in `wal_win_async.rs`
**File**: `falcon_storage/src/wal_win_async.rs`
**Issue**: Windows IOCP WAL writer uses 14 unsafe blocks for Win32 FFI (CreateIoCompletionPort, WriteFile, FlushFileBuffers, GetQueuedCompletionStatus). Each has SAFETY comments but the surface area is large.
**Severity**: Low — well-commented, but any FFI bug is UB.
**Fix**: Consider wrapping in a safe abstraction layer; add integration tests for error paths.

### 13. `detect_deadlocks` holds Mutex across O(V+E) DFS
**File**: `cross_shard.rs:1038-1153`
**Issue**: `WaitForGraphDetector::detect_deadlocks` holds `self.entries.lock()` for the entire DFS cycle detection algorithm. With many in-flight cross-shard transactions, this blocks all `register_wait`/`unregister` calls.
**Severity**: Latency spike during deadlock detection sweeps.
**Fix**: Clone the entries snapshot, release the lock, then run DFS on the clone.

---

## Summary

| Priority | Count | Key Theme |
|----------|-------|-----------|
| **P0** | 3 | Abort WAL loss, no-op rate limiter, connection check race |
| **P1** | 4 | Unwrap panics, unbounded maps, WAL parse panics, silent DDL errors |
| **P2** | 2 | Auth brute-force, bind_params escaping |
| **P3** | 4 | OwnedRow cloning, chain traversal, unsafe surface, lock contention |

**Recommended order**: P0-1 → P0-2 → P0-3 → P1-4 (top-20 files) → P1-6 → P2-9 → P1-5
