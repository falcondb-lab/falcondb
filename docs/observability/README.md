# FalconDB — Production Observability

## Overview

FalconDB exposes Prometheus metrics on the configured `--metrics-addr` (default `0.0.0.0:9090`).
All metrics use the `falcon_` prefix.

## Quick Start

```bash
# Start FalconDB with metrics
falcon --metrics-addr 0.0.0.0:9090

# Verify metrics endpoint
curl http://localhost:9090/metrics
```

## Grafana Dashboard

Import `grafana_dashboard.json` into Grafana:

1. Open Grafana → Dashboards → Import
2. Upload `docs/observability/grafana_dashboard.json`
3. Select your Prometheus data source
4. Dashboard UID: `falcondb-cluster-v1`

### Panels

| Row | Panels |
|-----|--------|
| Cluster Health | Health status, alive nodes, shard leaders, replication lag, epoch, memory pressure |
| Query Performance | QPS by type, p50/p99 latency, active connections |
| Transactions | Txn rate, fast/slow path, SLA latency by priority |
| Gateway Routing | Request classification, inflight/forwarded, topology cache hit rate |
| Client Discovery | Subscriptions, connection failovers, NOT_LEADER redirector |
| Epoch & Fencing | Epoch fence checks, shard migrations, 2PC decision log |
| Replication Streaming | Segment streaming rate, streaming bytes, checksum/rollback errors |
| Shard Rebalancer | Running/paused status, imbalance ratio gauge, throughput, move rate & task counts |
| Memory & Storage | Memory breakdown vs limits, WAL fsync latency |

## Alerting Rules

Import `alerting_rules.yml` into Prometheus:

```yaml
# prometheus.yml
rule_files:
  - "alerting_rules.yml"
```

### Alert Summary

| Group | Alerts | Severity |
|-------|--------|----------|
| Cluster Health | ClusterDegraded, ClusterCritical, NodeDown, ShardLeaderless | warning/critical |
| Replication | ReplicationLagHigh (>2s), ReplicationLagCritical (>10s) | warning/critical |
| Epoch Fencing | StaleEpochWritesDetected, StaleEpochWritesSustained | warning/critical |
| Gateway | NoRouteRejections, Overloaded, ForwardFailureRate, CacheMissRate | warning |
| Client Discovery | EventsDropped, FailoversHigh, BudgetExhausted | warning |
| Shard Migration | MigrationFailed, MigrationStuck (>30m) | warning |
| 2PC | UnappliedDecisions (>10), HardTimeouts | warning/critical |
| Rebalancer | ImbalanceHigh (>2.0, 10m), TasksFailing | warning |
| Repl Streaming | ChecksumFailures | critical |
| Memory | MemoryPressure, MemoryCritical | warning/critical |
| Query | LatencyP99High (>50ms), SlaViolations | warning |
| Storage | WalFsyncHigh (>100ms), WalBacklogGrowing (>100MB) | warning |

## Metric Categories

### Core Database
- `falcon_queries_total` — query counter by type/success
- `falcon_query_duration_us` — query latency histogram
- `falcon_active_connections` — current connection count
- `falcon_txn_*` — transaction counters and path metrics

### Distributed Cluster (P2)
- `falcon_cluster_health_status` — 0=healthy, 1=degraded, 2=critical
- `falcon_cluster_{total,alive}_nodes` — node counts
- `falcon_cluster_{total_shards,shards_with_leader}` — shard health
- `falcon_cluster_replication_lag_max_ms` — max lag across replicas

### Gateway Routing
- `falcon_gateway_{local_exec,forward,reject_*}_total` — classification
- `falcon_gateway_{inflight,forwarded}` — active request counts
- `falcon_topology_{cache_hits,cache_misses,invalidations}` — cache
- `falcon_topology_current_epoch` — cluster epoch

### Client Discovery
- `falcon_discovery_{active_subscriptions,events_*}` — subscriptions
- `falcon_client_conn_{failovers,connects,failures}_total` — connections
- `falcon_redirector_{attempts,successes,budget_exhausted}_total` — redirects

### Epoch Fencing
- `falcon_epoch_fence_{current,checks,rejections,accepted}_total`

### Shard Migration
- `falcon_shard_migration_{active,completed,failed,rolled_back}_total`

### Replication Streaming (v1.2)
- `falcon_repl_handshakes_total` — replica handshake count
- `falcon_repl_segments_streamed_total` — WAL segments streamed to replicas
- `falcon_repl_segment_bytes_total` — bytes transferred in full segments
- `falcon_repl_tail_bytes_total` — bytes transferred in tail streaming
- `falcon_repl_checksum_failures_total` — segment checksum mismatches (data integrity)
- `falcon_repl_error_rollbacks_total` — streaming errors that caused rollback
- `falcon_repl_snapshots_created_total` — full snapshots triggered

### Shard Rebalancer (v1.2)
- `falcon_rebalancer_runs_total` — completed rebalance cycles
- `falcon_rebalancer_rows_migrated_total` — cumulative rows moved
- `falcon_rebalancer_running` — 1 if currently rebalancing
- `falcon_rebalancer_paused` — 1 if SLA-paused
- `falcon_rebalancer_imbalance_ratio` — max/min shard load ratio
- `falcon_rebalancer_completed_tasks` — successful migrations last run
- `falcon_rebalancer_failed_tasks` — failed migrations last run
- `falcon_rebalancer_last_duration_ms` — last rebalance wall time
- `falcon_rebalancer_move_rate_rows_per_sec` — migration throughput

### Memory & WAL
- `falcon_memory_{total,mvcc,index,write_buffer}_bytes`
- `falcon_memory_pressure_state` — 0=normal, 1=pressure, 2=critical
- `falcon_wal_fsync_{avg,max}_us` — fsync latency
