# FalconDB — Production Operability Baseline (P1-4)

## Purpose

This document enables an **ops engineer who has never read FalconDB source code**
to monitor, diagnose, and resolve common production issues using only metrics and logs.

---

## 1. Health Endpoints

| Endpoint | Method | Purpose | Expected Response |
|----------|--------|---------|-------------------|
| `/health` | GET | Liveness probe | `200` if process is alive |
| `/ready` | GET | Readiness probe | `200` if accepting queries |
| `/status` | GET | Detailed status JSON | Memory, WAL, replication, connections |

---

## 2. Prometheus Metrics Catalog

### 2.1 Query Performance

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `falcon_queries_total` | counter | `type`, `success` | Total queries executed |
| `falcon_query_duration_us` | histogram | `type` | Query latency in µs |
| `falcon_active_connections` | gauge | — | Current active connections |

### 2.2 Transaction Health

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `falcon_txn_committed_total` | gauge | — | Total committed txns |
| `falcon_txn_aborted_total` | gauge | — | Total aborted txns |
| `falcon_txn_fast_path_commits` | gauge | — | Fast-path (single-shard) commits |
| `falcon_txn_slow_path_commits` | gauge | — | Slow-path (cross-shard) commits |
| `falcon_txn_occ_conflicts` | gauge | — | OCC conflicts detected |
| `falcon_txn_active_count` | gauge | — | Currently active txns |
| `falcon_txn_terminal_total` | counter | `type`, `reason`, `sqlstate` | Terminal state classification |

### 2.3 Memory Pressure

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_memory_total_bytes` | gauge | Total tracked memory (all 7 consumers) |
| `falcon_memory_mvcc_bytes` | gauge | MVCC version chain bytes |
| `falcon_memory_index_bytes` | gauge | Secondary index bytes |
| `falcon_memory_write_buffer_bytes` | gauge | Uncommitted write-buffer bytes |
| `falcon_memory_soft_limit_bytes` | gauge | Soft limit threshold |
| `falcon_memory_hard_limit_bytes` | gauge | Hard limit threshold |
| `falcon_memory_pressure_state` | gauge | 0=normal, 1=pressure, 2=critical |
| `falcon_memory_admission_rejections` | gauge | Cumulative rejected txns |

### 2.4 WAL & Durability

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_wal_fsync_total_us` | gauge | Cumulative fsync time |
| `falcon_wal_fsync_max_us` | gauge | Max single fsync latency |
| `falcon_wal_fsync_avg_us` | gauge | Average fsync latency |
| `falcon_wal_backlog_bytes` | gauge | Pending WAL bytes not yet flushed |
| `falcon_wal_group_commit_avg_size` | gauge | Average group commit batch size |

### 2.5 Replication

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_replication_lag_us` | gauge | Current replication lag |
| `falcon_replication_max_lag_us` | gauge | Peak replication lag |
| `falcon_replication_leader_changes` | gauge | Total leader elections |
| `falcon_replication_promote_count` | gauge | Total promotions |

### 2.6 GC & Compaction

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_gc_sweep_duration_us` | histogram | GC sweep duration |
| `falcon_gc_reclaimed_versions` | gauge | Versions reclaimed by GC |
| `falcon_gc_reclaimed_bytes` | gauge | Bytes reclaimed by GC |
| `falcon_gc_max_chain_length` | gauge | Longest MVCC chain |

### 2.7 Admission Control

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `falcon_admission_rejection_total` | counter | `resource`, `sqlstate` | Rejections by resource |
| `falcon_sla_p99_us` | gauge | — | Current p99 latency |
| `falcon_sla_total_rejected` | gauge | — | Total SLA rejections |

---

## 3. Log Levels & Structured Events

### Level Policy

| Level | When | Action Required |
|-------|------|-----------------|
| **INFO** | Normal operations, startup, config loaded | None |
| **WARN** | Memory pressure transition, replica lag, degraded performance | Monitor |
| **ERROR** | Memory critical, txn rejection, WAL flush failure | Investigate |
| **FATAL** | Unrecoverable state, data corruption detected | Immediate response |

### Key Structured Log Events

| Event | Level | Fields | Meaning |
|-------|-------|--------|---------|
| `Starting FalconDB...` | INFO | `version`, `git`, `built`, `mode` | Server starting |
| `Memory pressure transition` | WARN/ERROR | `pressure_from`, `pressure_to`, `total_bytes`, limits | Memory state change |
| `global memory governor: backpressure level changed` | WARN | `from`, `to`, `used_bytes`, `budget_bytes` | Node-level backpressure |
| Failover / promotion | WARN | role, term, shard | Leader change |

---

## 4. Troubleshooting Runbook

### Problem: TPS Dropping

1. Check `falcon_txn_active_count` — is it rising? (connection leak)
2. Check `falcon_memory_pressure_state` — if > 0, memory pressure is throttling
3. Check `falcon_gc_max_chain_length` — if high, GC is behind
4. Check `falcon_wal_fsync_max_us` — if spiking, disk is slow

### Problem: Memory Pressure (pressure_state = 1 or 2)

1. Check which consumer dominates: `falcon_memory_mvcc_bytes` vs `falcon_memory_write_buffer_bytes` etc.
2. If MVCC dominates → long-running transactions preventing GC
3. If write_buffer dominates → too many concurrent uncommitted txns
4. If WAL buffer dominates → WAL flush is stalled
5. Action: kill long transactions, reduce concurrency, or increase limits

### Problem: Replica Lagging

1. Check `falcon_replication_lag_us` — current lag
2. Check `falcon_wal_backlog_bytes` — WAL shipping backlog
3. Check network between primary and replica
4. If lag grows monotonically → replica cannot keep up; consider snapshot resync

### Problem: High Abort Rate

1. Check `falcon_txn_aborted_total` / `falcon_txn_committed_total` ratio
2. Check `falcon_txn_occ_conflicts` — OCC contention
3. If conflicts high → workload has hot rows; consider sharding or serialization
4. Check `falcon_admission_rejection_total` — if rising, system is overloaded

### Problem: WAL Flush Latency Spikes

1. Check `falcon_wal_fsync_max_us` — single worst fsync
2. Check disk IOPS and queue depth (OS-level)
3. Consider `wal_sync_method` configuration
4. If on Windows: check `NO_BUFFERING` mode via `falcon doctor`

---

## 5. Alerting Rules (Recommended)

```yaml
# Prometheus alerting rules (example)
groups:
  - name: falcondb
    rules:
      - alert: FalconDBMemoryPressure
        expr: falcon_memory_pressure_state >= 1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "FalconDB memory pressure detected"

      - alert: FalconDBMemoryCritical
        expr: falcon_memory_pressure_state >= 2
        for: 30s
        labels:
          severity: critical

      - alert: FalconDBReplicaLag
        expr: falcon_replication_lag_us > 5000000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Replica lag > 5 seconds"

      - alert: FalconDBHighAbortRate
        expr: rate(falcon_txn_terminal_total{type="aborted_retryable"}[5m]) > 10
        for: 3m
        labels:
          severity: warning

      - alert: FalconDBWALFlushSlow
        expr: falcon_wal_fsync_max_us > 100000
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "WAL fsync > 100ms"
```

---

## 6. Grafana Dashboard (Recommended Panels)

| Panel | Metric(s) | Type |
|-------|----------|------|
| TPS | `rate(falcon_queries_total[1m])` | Time series |
| Query Latency p99 | `falcon_sla_p99_us` | Time series |
| Memory Usage | `falcon_memory_total_bytes`, `falcon_memory_soft_limit_bytes` | Stacked area |
| Memory Pressure | `falcon_memory_pressure_state` | Stat |
| Txn Abort Rate | `falcon_txn_aborted_total / falcon_txn_committed_total` | Gauge |
| Replication Lag | `falcon_replication_lag_us` | Time series |
| WAL Flush Latency | `falcon_wal_fsync_avg_us`, `falcon_wal_fsync_max_us` | Time series |
| Active Connections | `falcon_active_connections` | Stat |
