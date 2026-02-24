# Prepared Statement & Binary Params — Performance Report

> FalconDB v1.x  
> Date: 2026-02-24

---

## 1. Architecture

### Plan Cache (`falcon_protocol_pg::plan_cache`)

| Property | Value |
|----------|-------|
| **Key** | Normalized SQL (whitespace-collapsed, case-folded) |
| **Value** | `PhysicalPlan` + access count |
| **Capacity** | Configurable (`plan_cache_capacity`, default 1024) |
| **Eviction** | LFU (least-frequently-used entry evicted at capacity) |
| **Invalidation** | Schema generation counter; bumped on every DDL |
| **Thread safety** | `RwLock<PlanCacheInner>` |
| **Scope** | Shared across all pooled connections via `Arc<PlanCache>` |

### DDL Invalidation Triggers

The plan cache is invalidated (`schema_generation++`) on:
- `CREATE TABLE` / `DROP TABLE`
- `ALTER TABLE` (ADD/DROP COLUMN, RENAME)
- `CREATE VIEW` / `DROP VIEW`
- `CREATE INDEX` / `DROP INDEX`
- `TRUNCATE TABLE`

Code refs: `handler.rs` lines calling `self.plan_cache.invalidate()` after `ExecutionResult::Ddl`.

### What Gets Cached

Only DML query plans are cached (SELECT, INSERT via planner). Excluded:
- DDL plans (CREATE/DROP/ALTER)
- Transaction control (BEGIN/COMMIT/ROLLBACK)
- UPDATE/DELETE (unique WHERE literals per execution)

### Extended Query Protocol (Parse/Bind/Execute)

| Phase | Action |
|-------|--------|
| **Parse** | SQL → `PreparedStatement` with `param_types`, optional `PhysicalPlan` |
| **Bind** | Portal created, parameter values decoded (text or binary format) |
| **Execute** | Plan executed against storage engine |
| **Sync** | ReadyForQuery sent |

Parse caches the plan in the session's `prepared_statements` map.
Bind resolves parameters using `resolve_param_format()` to detect text vs binary.

---

## 2. Binary Parameter Support

| Type | PG OID | Binary Format | Status |
|------|--------|---------------|--------|
| `INT4` | 23 | 4-byte BE | ✅ |
| `INT8` | 20 | 8-byte BE (also accepts 4-byte) | ✅ |
| `FLOAT8` | 701 | 8-byte IEEE 754 BE | ✅ |
| `FLOAT4` | 700 | 4-byte IEEE 754 BE (widened to f64) | ✅ |
| `BOOL` | 16 | 1-byte (0/1) | ✅ |
| `TEXT` | 25 | UTF-8 bytes | ✅ |
| `BYTEA` | 17 | Raw bytes | ✅ |
| `UUID` | 2950 | 16-byte raw → formatted string | ✅ |
| `NUMERIC` | 1700 | Text fallback (PG numeric wire is complex) | ✅ (text) |
| `TIMESTAMP` | 1114 | Text fallback | ✅ (text) |
| `DATE` | 1082 | Text fallback | ✅ (text) |
| `JSONB` | 3802 | Text fallback | ✅ (text) |

**No-hint inference**: When the driver sends binary without type OID:
- 1 byte → `Boolean`
- 4 bytes → `Int32`
- 8 bytes → `Int64`
- Other → `Text` (UTF-8 lossy)

---

## 3. Expected Performance Impact

### Prepared Statements (plan cache hit vs miss)

| Metric | Cache Miss | Cache Hit | Improvement |
|--------|-----------|-----------|-------------|
| Parse time | ~50–200µs (SQL parse + bind + plan) | ~0µs (hash lookup) | **100%** |
| Per-query overhead | Full pipeline | Plan clone only | **50–80% latency reduction** on simple queries |

The plan cache eliminates re-parsing and re-planning for repeated queries.
For OLTP workloads with many identical SELECT/INSERT patterns, this is the
single largest latency win.

### Binary Params (binary vs text)

| Metric | Text Format | Binary Format | Improvement |
|--------|-------------|---------------|-------------|
| INT4 decode | String parse (~20ns) | 4-byte memcpy (~2ns) | **10x** |
| INT8 decode | String parse (~25ns) | 8-byte memcpy (~2ns) | **12x** |
| FLOAT8 decode | String parse (~40ns) | 8-byte memcpy (~2ns) | **20x** |
| BOOL decode | String compare (~10ns) | 1-byte check (~1ns) | **10x** |

For parameter-heavy workloads (e.g., batch INSERT with 1000 rows × 5 columns),
binary params eliminate ~100µs of string parsing per batch.

---

## 4. Test Coverage

| Component | Test Count | Location |
|-----------|-----------|----------|
| Plan cache | 7 | `plan_cache.rs::tests` |
| Binary param decode | 12 | `server.rs::tests::test_decode_binary_*` |
| Extended query protocol | 8+ | `handler.rs::tests` (Parse/Bind/Execute) |
| Prepared SQL commands | 3+ | `handler_session.rs` (PREPARE/EXECUTE/DEALLOCATE) |

---

## 5. Verification Commands

```bash
# Plan cache tests
cargo test -p falcon_protocol_pg -- "plan_cache"

# Binary param tests
cargo test -p falcon_protocol_pg -- "decode_binary"

# Full protocol tests
cargo test -p falcon_protocol_pg

# Benchmark (if configured)
cargo run -p falcon_bench -- --config bench_configs/oltp_prepared.toml
```
