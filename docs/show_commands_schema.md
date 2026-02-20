# SHOW falcon.* — Stable Output Schema Reference

All `SHOW falcon.*` commands return a two-column result set: `metric` (TEXT)
and `value` (TEXT). This document defines the **stable schema contract** for
each command. Columns and metrics listed here are guaranteed across minor
versions; additions are backward-compatible, removals require a major bump.

> **Version**: v0.1.0 (M1)
> **Wire format**: PostgreSQL RowDescription + DataRow messages

---

## SHOW falcon.txn_stats

Transaction commit/abort counters and latency percentiles.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `total_committed` | u64 | count | Total committed transactions since startup |
| `fast_path_commits` | u64 | count | Commits via fast-path (LocalTxn, single-shard) |
| `slow_path_commits` | u64 | count | Commits via slow-path (GlobalTxn, XA-2PC) |
| `total_aborted` | u64 | count | Total aborted transactions |
| `occ_conflicts` | u64 | count | OCC serialization failures (SI write-write conflicts) |
| `constraint_violations` | u64 | count | Unique/PK/FK/CHECK constraint violations |
| `active_count` | u64 | count | Currently active (in-flight) transactions |
| `fast_p50_us` | u64 | µs | Fast-path commit latency p50 |
| `fast_p95_us` | u64 | µs | Fast-path commit latency p95 |
| `fast_p99_us` | u64 | µs | Fast-path commit latency p99 |
| `slow_p50_us` | u64 | µs | Slow-path commit latency p50 |
| `slow_p95_us` | u64 | µs | Slow-path commit latency p95 |
| `slow_p99_us` | u64 | µs | Slow-path commit latency p99 |

---

## SHOW falcon.wal_stats

WAL writer statistics.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `records_written` | u64 | count | Total WAL records written |
| `bytes_written` | u64 | bytes | Total WAL bytes written |
| `flushes` | u64 | count | Total WAL flush (fsync) operations |
| `current_segment` | u64 | id | Current WAL segment number |
| `current_lsn` | u64 | lsn | Current (latest) Log Sequence Number |

---

## SHOW falcon.node_role

Current node role in the cluster.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `role` | string | — | One of: `standalone`, `primary`, `replica`, `analytics` |

---

## SHOW falcon.connections

Active connection counters.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `active_connections` | u64 | count | Currently open PG connections |
| `max_connections` | u64 | count | Configured connection limit |

---

## SHOW falcon.gc_stats

MVCC garbage collection statistics.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `gc_safepoint_ts` | u64 | ts | Current GC watermark timestamp |
| `active_txn_count` | u64 | count | Active transactions blocking GC |
| `oldest_txn_ts` | u64 | ts | Start timestamp of oldest active transaction |
| `total_sweeps` | u64 | count | Total GC sweep cycles completed |
| `reclaimed_version_count` | u64 | count | Total MVCC versions reclaimed |
| `reclaimed_memory_bytes` | u64 | bytes | Total bytes freed by GC |
| `last_sweep_duration_us` | u64 | µs | Duration of last GC sweep |
| `max_chain_length` | u64 | count | Longest version chain observed |

---

## SHOW falcon.gc_safepoint

GC safepoint diagnostic (long-transaction detection).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `active_txn_count` | u64 | count | Active transactions blocking GC |
| `longest_txn_age_us` | u64 | µs | Age of the longest-running active transaction |
| `min_active_start_ts` | u64 | ts | Start timestamp of the oldest active transaction |
| `current_ts` | u64 | ts | Current timestamp allocator value |
| `stalled` | bool | — | Whether GC safepoint is stalled by long-running txns |

---

## SHOW falcon.table_stats

Per-table statistics (row counts, column cardinality). Without a table name
suffix, returns stats for all analyzed tables.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `table_name` | string | — | Table name |
| `row_count` | u64 | count | Estimated row count |
| `columns_analyzed` | u64 | count | Number of columns with statistics |
| (per-column rows follow with `col_name`, `null_frac`, `n_distinct`, `avg_width`) |

Variant: `SHOW falcon.table_stats_<tablename>` — returns stats for a single table.

---

## SHOW falcon.sequences

All defined sequences and their current values.

| Column | Type | Description |
|--------|------|-------------|
| `sequence_name` | string | Sequence name |
| `current_value` | i64 | Current value (last returned by `nextval`) |

---

## SHOW falcon.slow_queries

Slow query log entries (requires `SET log_min_duration_statement = <ms>`).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `threshold_ms` | u64 | ms | Current slow query threshold |
| `total_slow_queries` | u64 | count | Total slow queries captured |
| (recent entries follow with `sql_preview`, `duration_us`, `session_id`, `timestamp`) |

---

## SHOW falcon.checkpoint_stats

WAL checkpoint status.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `wal_enabled` | bool | — | Whether WAL is active |
| `records_written` | u64 | count | WAL records since last checkpoint |
| `flushes` | u64 | count | WAL flushes since last checkpoint |

---

## SHOW falcon.plan_cache

Prepared statement / plan cache statistics.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `entries` | u64 | count | Current cached plans |
| `capacity` | u64 | count | Maximum cache capacity |
| `hits` | u64 | count | Cache hits |
| `misses` | u64 | count | Cache misses |
| `hit_rate_pct` | f64 | % | Hit rate percentage |

---

## SHOW falcon.replication_stats

Replication and failover metrics (primary perspective).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `promote_count` | u64 | count | Total promote operations completed |
| `last_failover_time_ms` | u64 | ms | Duration of last failover |
| `leader_changes` | u64 | count | Total leader change events |
| `replication_lag_us` | u64 | µs | Current replication lag |
| `max_replication_lag_us` | u64 | µs | Peak replication lag observed |

---

## Schema Stability Guarantees

1. **Metric names** are stable within a major version — no renames.
2. **New metrics** may be appended in minor releases (backward-compatible).
3. **Metric removal** only in major version bumps with migration notes.
4. **Value types** (u64, string, bool, f64) are stable — no type changes within a major version.
5. Clients SHOULD tolerate unknown metric names gracefully (skip, don't error).
