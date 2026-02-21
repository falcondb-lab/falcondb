# FalconDB Operations Playbook

## Overview

Step-by-step procedures for common operational tasks. Each procedure includes
pre-conditions, steps, verification, and rollback instructions.

---

## 1. Scale-Out: Adding a Node

### Pre-conditions
- New node has `falcon_server` binary installed
- Network connectivity to all existing nodes
- Sufficient disk space for WAL replay

### Steps

```bash
# 1. Start new node as replica
falcon_server \
  --port 5432 \
  --data-dir /data/falcon \
  --role replica \
  --grpc-addr 0.0.0.0:50051 \
  --primary-endpoint http://<primary>:50051

# 2. Monitor catch-up progress
watch -n 1 'psql -h <new-node> -U falcon -c "SHOW falcon.replication_stats;"'

# 3. Wait for lag_lsn = 0
# Expected output: lag_lsn | 0

# 4. Verify node is serving reads
psql -h <new-node> -U falcon -c "SELECT COUNT(*) FROM <table>;"

# 5. Trigger rebalance (dry-run first)
psql -h <primary> -U falcon -c "SHOW falcon.rebalance_status;"

# 6. Apply rebalance (if dry-run looks correct)
# (via config change or admin API — see rebalancer docs)
```

### Verification
```sql
-- On primary
SHOW falcon.ha_status;
SHOW falcon.replica_health;
SHOW falcon.rebalance_status;
```

### Rollback
```bash
# Simply stop the new node — no data migration has occurred yet
kill $(cat /var/run/falcon/new-node.pid)
```

---

## 2. Scale-In: Removing a Node

### Pre-conditions
- Node is a replica (not primary)
- All shards on this node have been migrated (rebalance complete)
- At least 1 other replica remains

### Steps

```bash
# 1. Drain traffic from node (stop routing reads to it)
# (Update load balancer / proxy config to exclude this node)

# 2. Verify no active connections
psql -h <node-to-remove> -U falcon -c "SHOW falcon.connections;"

# 3. Verify replication lag is 0 (all data replicated)
psql -h <primary> -U falcon -c "SHOW falcon.replication_stats;"

# 4. Graceful shutdown
kill -SIGTERM $(cat /var/run/falcon/<node>.pid)

# 5. Wait for shutdown confirmation
# Log: "[INFO] falcon_server: graceful shutdown complete"

# 6. Remove from cluster membership (if applicable)
# (via admin API or config change)
```

### Verification
```sql
-- On primary: verify remaining replicas are healthy
SHOW falcon.replica_health;
SHOW falcon.ha_status;
```

### Rollback
```bash
# Restart the node — it will rejoin as replica and catch up
falcon_server --role replica --primary-endpoint http://<primary>:50051 ...
```

---

## 3. Rebalancing Shards

### Pre-conditions
- Cluster is healthy (all nodes up, no in-doubt txns)
- Sufficient network bandwidth for data migration

### Steps

```bash
# 1. Check current shard distribution
psql -h <primary> -U falcon -c "SHOW falcon.rebalance_status;"

# 2. Generate rebalance plan (dry-run)
# Expected output: list of shard moves, estimated data volume, estimated time

# 3. Review plan — verify:
#    - No shard moves to/from unhealthy nodes
#    - Estimated time is acceptable
#    - Data volume is within network capacity

# 4. Apply rebalance
# (via config change or admin API)

# 5. Monitor progress
watch -n 5 'psql -h <primary> -U falcon -c "SHOW falcon.rebalance_status;"'

# 6. Verify completion
psql -h <primary> -U falcon -c "SHOW falcon.rebalance_status;"
# Expected: status = "idle", all shards balanced
```

### Rate Limiting
Rebalance uses a token bucket to limit migration bandwidth:
```toml
# falcon.toml
[rebalancer]
max_bytes_per_sec = 104857600  # 100MB/s
pause_on_oltp_pressure = true
oltp_p99_threshold_ms = 20
```

### Pausing / Resuming
```bash
# Pause (via admin API or SIGSTOP to rebalancer thread)
psql -h <primary> -U falcon -c "-- SHOW falcon.rebalance_pause;"

# Resume
psql -h <primary> -U falcon -c "-- SHOW falcon.rebalance_resume;"
```

### Rollback
Rebalance is designed to be idempotent and restartable:
```bash
# Stop rebalancer (SIGTERM falcon_server or config change)
# Data that was already migrated stays on new node
# Re-run with original config to migrate back
```

---

## 4. Failover: Promoting a Replica

### Automatic Failover (HA mode)
FalconDB's `FailoverOrchestrator` handles this automatically:
- Detects primary failure via heartbeat timeout
- Selects best replica (lowest lag_lsn)
- Promotes replica to primary
- Updates cluster membership

### Manual Failover

```bash
# 1. Verify primary is truly down (not just slow)
psql -h <primary> -U falcon -c "SELECT 1;" || echo "Primary unreachable"

# 2. Check replica lag
psql -h <replica> -U falcon -c "SHOW falcon.replication_stats;"
# Verify lag_lsn is acceptable (ideally 0)

# 3. If replica has large lag — wait for catch-up OR accept RPO
# Policy: if lag > 10000 LSN, require explicit --force flag

# 4. Promote replica
# Option A: restart replica as primary
falcon_server --role primary --grpc-addr 0.0.0.0:50051 ...

# Option B: via admin API (if implemented)
# curl -X POST http://<replica>:8080/admin/promote

# 5. Update DNS / proxy to point to new primary

# 6. Restart old primary as replica
falcon_server --role replica --primary-endpoint http://<new-primary>:50051 ...
```

### Verification
```sql
-- On new primary
SHOW falcon.ha_status;
-- Expected: role = "primary", leader_id = <this node>

-- Verify writes work
INSERT INTO test_failover VALUES (NOW(), 'post-failover');
SELECT COUNT(*) FROM test_failover;
```

### RTO / RPO Targets
| Mode | RTO | RPO |
|------|-----|-----|
| Quorum-ack replication | < 5s | 0 bytes |
| Async replication | < 5s | < 1s of writes |
| Manual failover | < 30s | depends on lag |

---

## 5. Rolling Upgrade

See `scripts/rolling_upgrade_smoke.sh` for automated procedure.

### Manual Steps

```bash
# 1. Upgrade replicas one by one
for replica in replica1 replica2; do
  # Stop replica
  kill -SIGTERM $(cat /var/run/falcon/$replica.pid)
  sleep 2
  
  # Install new binary
  cp falcon_server_new /usr/local/bin/falcon_server
  
  # Restart replica
  falcon_server --role replica --primary-endpoint http://<primary>:50051 ...
  
  # Wait for catch-up
  while ! psql -h $replica -U falcon -c "SHOW falcon.replication_stats;" | grep "lag_lsn.*0"; do
    sleep 1
  done
  echo "$replica upgraded and caught up"
done

# 2. Promote an upgraded replica
# (see Failover procedure above)

# 3. Upgrade old primary (now a replica)
kill -SIGTERM $(cat /var/run/falcon/primary.pid)
cp falcon_server_new /usr/local/bin/falcon_server
falcon_server --role replica --primary-endpoint http://<new-primary>:50051 ...
```

### Rollback
```bash
# Restore old binary
cp falcon_server_old /usr/local/bin/falcon_server
# Restart affected nodes
```

---

## 6. Diagnostic Bundle Collection

```bash
# Collect bundle via SHOW command (outputs to log)
psql -h <node> -U falcon -c "SHOW falcon.diag_bundle;"

# Or via HTTP health endpoint
curl http://<node>:8080/diag/bundle > diag_bundle_$(date +%s).json

# Bundle contains:
# - Recent panic events
# - Key metric snapshots
# - Topology / epoch / leader / apply-lag
# - Inflight txn / queue / budget state
# - Circuit breaker states
# - Recent log lines
```

---

## 7. Common Troubleshooting

### High Abort Rate
```sql
SHOW falcon.slow_txns;      -- Find long-running transactions
SHOW falcon.hotspots;       -- Find hot keys
SHOW falcon.txn_stats;      -- Abort rate breakdown
```
**Resolution**: Reduce transaction size, add retry logic in application, check for hot key patterns.

### Replica Lag Growing
```sql
SHOW falcon.replication_stats;  -- Per-replica lag
SHOW falcon.wal_stats;          -- WAL backlog
```
**Resolution**: Check replica CPU/IO, reduce write throughput temporarily, check network bandwidth.

### Memory Pressure
```sql
SHOW falcon.memory_pressure;  -- Budget usage
SHOW falcon.gc_stats;         -- GC safepoint lag
SHOW falcon.slow_txns;        -- Long-running txns blocking GC
```
**Resolution**: Kill long-running transactions, trigger `CHECKPOINT;`, increase memory budget.

### Circuit Breaker Open
```sql
SHOW falcon.circuit_breakers;  -- Per-shard breaker state
```
**Resolution**: Investigate shard health, fix underlying issue, then:
```sql
-- Force-close breaker (after fix)
-- (via admin API or restart)
```

### In-Doubt Transactions
```sql
SHOW falcon.two_phase;  -- In-doubt txn list
```
**Resolution**: `InDoubtResolver` handles automatically within 30s. If stuck:
1. Check coordinator logs for decision
2. Manually record decision via admin API
3. Restart coordinator node

---

## 8. Monitoring Checklist

Run these checks daily in production:

```sql
-- Health score (0-100)
SHOW falcon.health_score;

-- Active transactions
SHOW falcon.txn_stats;

-- Replication health
SHOW falcon.replication_stats;

-- Memory usage
SHOW falcon.memory_pressure;

-- Recent slow queries
SHOW falcon.slow_queries;

-- In-doubt transactions (should be 0)
SHOW falcon.two_phase;

-- Circuit breakers (should all be "closed")
SHOW falcon.circuit_breakers;
```

### Alert Thresholds
| Metric | Warning | Critical |
|--------|---------|----------|
| `falcon_replication_lag_lsn` | > 1000 | > 10000 |
| `falcon_txn_aborted_total` rate | > 5/s | > 50/s |
| `falcon_memory_used_ratio` | > 0.80 | > 0.90 |
| `falcon_slow_query_total` rate | > 5/min | > 50/min |
| `falcon_indoubt_txns` | > 0 for 60s | > 0 for 300s |
| `falcon_circuit_breaker_open` | > 0 | > 1 |
| `falcon_panic_count` rate | > 0 | > 5/min |
