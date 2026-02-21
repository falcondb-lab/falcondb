# FalconDB Production Readiness Guide

## Overview

This document defines the production readiness criteria, SLO targets, operational
procedures, and troubleshooting guide for FalconDB.

---

## SLO Targets

### Latency

| Operation | P50 Target | P99 Target | Alert Threshold |
|-----------|-----------|-----------|-----------------|
| Point lookup (PK) | < 0.5ms | < 2ms | P99 > 10ms |
| Simple INSERT | < 1ms | < 5ms | P99 > 20ms |
| Txn commit (local, empty) | < 0.5ms | < 1ms | P99 > 5ms |
| Txn commit (mixed load) | < 2ms | < 10ms | P99 > 50ms |
| 2PC commit (cross-shard) | < 5ms | < 20ms | P99 > 100ms |
| Full table scan (1M rows) | < 100ms | < 500ms | P99 > 2s |
| Columnar aggregate (1M rows) | < 50ms | < 200ms | P99 > 1s |

### Availability

| Metric | Target | Alert |
|--------|--------|-------|
| Failover RTO (primary → replica) | < 5s | > 15s |
| RPO (quorum-ack mode) | 0 bytes | any loss |
| RPO (async mode) | < 1s of writes | > 5s |
| Replication lag | < 100ms | > 1s |
| GC safepoint lag | < 10s | > 60s |

### Throughput

| Workload | Target | Notes |
|----------|--------|-------|
| YCSB-A (50% read, 50% write) | > 50K ops/s | single shard, in-memory |
| YCSB-B (95% read, 5% write) | > 100K ops/s | single shard |
| YCSB-C (100% read) | > 200K ops/s | single shard |

---

## Operational Procedures

### Starting a Cluster

```bash
# Single node (standalone)
falcon_server --port 5432 --data-dir /data/falcon

# Primary node
falcon_server --port 5432 --data-dir /data/falcon \
  --role primary --grpc-addr 0.0.0.0:50051

# Replica node
falcon_server --port 5432 --data-dir /data/falcon \
  --role replica --grpc-addr 0.0.0.0:50052 \
  --primary-endpoint http://primary:50051

# Analytics node (ColumnStore workloads)
falcon_server --port 5432 --data-dir /data/falcon \
  --role analytics --grpc-addr 0.0.0.0:50053 \
  --primary-endpoint http://primary:50051
```

### Node Replacement

1. Start new replica with `--primary-endpoint` pointing to current primary.
2. Wait for `SHOW falcon.replication_stats` to show `lag_lsn = 0`.
3. If replacing primary: promote new replica, update DNS/proxy.
4. Decommission old node.

### Scaling Out (Adding Shards)

```bash
# Trigger rebalancer (dry-run first)
SHOW falcon.rebalance_status;

# Apply rebalancing
# (via admin API or SHOW falcon.rebalance_status after config change)
```

### Rolling Upgrade

```bash
./scripts/rolling_upgrade_smoke.sh
```

Procedure:
1. Upgrade replicas one-by-one (restart with new binary).
2. Verify each replica catches up (`lag_lsn = 0`) before proceeding.
3. Promote an upgraded replica to primary.
4. Upgrade old primary (now a replica).

### Manual Failover

```bash
# Via local cluster harness
./scripts/local_cluster_harness.sh failover

# Via CI gate
./scripts/ci_failover_gate.sh
```

---

## Monitoring & Alerting

### Key Metrics to Monitor

```
# Prometheus queries (examples)
falcon_txn_active > 1000                    # Too many active txns
rate(falcon_txn_aborted_total[1m]) > 10     # High abort rate
falcon_replication_lag_lsn > 10000          # Replica falling behind
falcon_memory_used_bytes / falcon_memory_limit_bytes > 0.85  # Memory pressure
rate(falcon_slow_query_total[1m]) > 5       # Slow query spike
falcon_wal_backlog_bytes > 104857600        # WAL backlog > 100MB
```

### SHOW Commands for Health Check

```sql
SHOW falcon.health_score;        -- Overall health (0-100)
SHOW falcon.txn_stats;           -- Transaction statistics
SHOW falcon.wal_stats;           -- WAL statistics
SHOW falcon.replication_stats;   -- Replication lag and status
SHOW falcon.gc_stats;            -- GC safepoint and reclamation
SHOW falcon.memory_pressure;     -- Memory usage and budget
SHOW falcon.slow_queries;        -- Recent slow queries
SHOW falcon.ha_status;           -- HA group status
SHOW falcon.replica_health;      -- Per-replica health
SHOW falcon.admission;           -- Admission control state
```

---

## Backpressure & Admission Control

### Thresholds (Configurable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_inflight_queries` | 1000 | Max concurrent queries |
| `max_inflight_writes_per_shard` | 500 | Max concurrent writes per shard |
| `max_wal_flush_queue_bytes` | 64MB | WAL flush queue backpressure |
| `max_replication_apply_lag_bytes` | 256MB | Replication lag backpressure |
| `memory_budget_bytes` | 4GB | Global memory budget |
| `memory_pressure_ratio` | 0.80 | Soft limit (backpressure) |
| `memory_hard_limit_ratio` | 0.95 | Hard limit (reject all writes) |

### Behavior Under Pressure

1. **Soft limit (80%)**: New writes are backpressured (`Transient` error, `retry_after_ms=50`).
2. **Hard limit (95%)**: All writes rejected (`Transient` error, `retry_after_ms=500`).
3. **WAL backlog**: Writes blocked until WAL flushes.
4. **Replication lag**: Writes throttled to prevent replica from falling too far behind.

---

## Recovery Procedures

### WAL Recovery

On restart, FalconDB automatically:
1. Loads catalog metadata.
2. Replays WAL from last checkpoint.
3. Rebuilds in-memory indexes.
4. Opens for reads (after replay).
5. Opens for writes (after validation).

Recovery progress is logged at INFO level:
```
[INFO] recovery: loading catalog (stage=1/5)
[INFO] recovery: replaying WAL from lsn=12345 (stage=2/5)
[INFO] recovery: rebuilding indexes (stage=3/5)
[INFO] recovery: opening for reads (stage=4/5)
[INFO] recovery: opening for writes (stage=5/5)
[INFO] recovery: complete in 1234ms
```

### In-Doubt Transaction Resolution

2PC transactions that are in-doubt (coordinator crashed after prepare) are
automatically resolved by the background `InDoubtResolver`:

```sql
SHOW falcon.two_phase;  -- View in-doubt transactions
```

Resolution happens within 30 seconds of restart. If resolution fails,
check logs for `InternalBug` errors.

### Online DDL Recovery

If a node crashes during an Online DDL operation:
1. On restart, DDL state machine resumes from last durable checkpoint.
2. If in `backfill` phase: backfill resumes from last batch.
3. If in `validate` phase: validation re-runs.
4. If in `swap` phase: swap is atomic (either completes or rolls back).

---

## Troubleshooting

### High Abort Rate

**Symptom**: `rate(falcon_txn_aborted_total[1m]) > 10`

**Causes**:
1. Write-write conflicts (high contention on hot keys).
2. Serialization failures (SI anomaly detection).
3. Memory pressure.

**Resolution**:
1. Check `SHOW falcon.slow_txns` for long-running transactions.
2. Check `SHOW falcon.hotspots` for hot keys.
3. Consider increasing transaction retry count in application.

### Replica Lag

**Symptom**: `falcon_replication_lag_lsn > 10000`

**Causes**:
1. Replica CPU/IO bottleneck.
2. Network congestion.
3. Large transactions generating many WAL records.

**Resolution**:
1. Check `SHOW falcon.replication_stats` for lag trend.
2. Check replica CPU/IO metrics.
3. Consider reducing write throughput temporarily.

### Memory Pressure

**Symptom**: `falcon_memory_used_bytes / falcon_memory_limit_bytes > 0.85`

**Causes**:
1. Long-running transactions holding MVCC versions.
2. Large result sets in memory.
3. GC safepoint stuck (replica lag blocking GC).

**Resolution**:
1. Check `SHOW falcon.gc_stats` for safepoint lag.
2. Check `SHOW falcon.slow_txns` for long-running transactions.
3. Trigger manual GC: `CHECKPOINT;`

### Slow Queries

**Symptom**: `rate(falcon_slow_query_total[1m]) > 5`

**Resolution**:
1. Check `SHOW falcon.slow_queries` for recent slow queries.
2. Use `EXPLAIN ANALYZE <query>` to identify bottlenecks.
3. Check for missing indexes.
4. Check for table statistics staleness: `ANALYZE <table>;`

---

## Security Checklist

- [ ] TLS enabled for client connections (`sslmode=require`)
- [ ] TLS enabled for replication gRPC
- [ ] Authentication method set (not `trust` in production)
- [ ] Role-based access control configured
- [ ] Audit log enabled
- [ ] IP allowlist configured (if applicable)
- [ ] Secrets not in config files (use env vars or secrets manager)

---

## Capacity Planning

### Memory

- Base overhead: ~100MB
- Per active transaction: ~10KB (write set) + MVCC versions
- Per table (in-memory): ~100 bytes per row + indexes
- WAL buffer: configurable (default 64MB)
- Result buffers: up to `max_result_bytes` per query

### Storage (WAL)

- WAL growth rate: ~1KB per write transaction
- Checkpoint interval: configurable (default every 1000 WAL records)
- Retention: until all replicas have applied

### Network

- Replication bandwidth: proportional to write throughput
- Client connections: ~10KB per connection overhead

---

## CI Gates

```bash
# Full production gate (all checks)
./scripts/ci_production_gate.sh

# Fast mode (skip load test)
./scripts/ci_production_gate.sh --fast

# Failover gate only
./scripts/ci_failover_gate.sh

# unwrap scan only
./scripts/deny_unwrap.sh

# Local cluster smoke
./scripts/local_cluster_harness.sh smoke
```
