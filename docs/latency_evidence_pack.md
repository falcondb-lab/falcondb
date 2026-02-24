# Low Tail-Latency Evidence Pack — FalconDB P2

> Version: P2-draft  
> Purpose: Externally publishable proof of deterministic tail-latency behavior

---

## 1. Executive Summary

FalconDB is designed as a **p99/p999-first** distributed OLTP engine. Under
pressure and fault conditions, it maintains predictable latency by:

- **SLA-aware admission**: rejects deterministically rather than queueing
- **Budget-bounded GC**: never allows GC to spike tail latency
- **Minimal data movement**: pushdown + partial aggregation in distributed queries
- **Explainable rejection**: every rejection has a reason, metric, and remedy

This document provides the configuration, commands, and expected results to
independently verify these claims.

---

## 2. Test Environment

### Hardware Requirements
| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 16 cores |
| RAM | 8 GB | 32 GB |
| Disk | SSD (any) | NVMe SSD |
| Network | 1 Gbps | 10 Gbps |

### Software
- FalconDB v2.x (built from source)
- Rust 1.75+ (for building)
- `psql` or any PG-compatible client

### Configuration
```toml
# falcon.toml — Evidence Pack configuration
[server]
port = 5432
data_dir = "./falcon_data"

[sla]
target_p99_ms = 20
target_p999_ms = 100
max_inflight_txn = 500
max_cross_shard_inflight = 50
slow_path_quota_ratio = 0.3
latency_window_size = 10000

[gc]
max_sweep_us = 5000
max_keys_per_sweep = 10000
normal_interval_ms = 500
pressure_interval_ms = 100
critical_interval_ms = 20

[storage]
memory_budget_bytes = 4294967296
```

---

## 3. Benchmark Configurations

### 3a. Steady-State Latency (Normal Load)

```toml
# bench_configs/latency/steady_state.toml
[workload]
duration_sec = 300
concurrent_connections = 50
target_tps = 500
mix = { select = 60, insert = 20, update = 15, delete = 5 }
table_count = 10
rows_per_table = 100000

[thresholds]
max_p99_ms = 20
max_p999_ms = 100
min_accept_rate = 0.99
```

### 3b. Overload Ramp (Degradation Curve)

```toml
# bench_configs/latency/overload_ramp.toml
[workload]
duration_sec = 600
concurrent_connections = 200
target_tps_ramp = [100, 500, 1000, 2000, 5000]
ramp_step_sec = 120
mix = { select = 50, insert = 25, update = 20, delete = 5 }
table_count = 10
rows_per_table = 100000

[thresholds]
max_p99_multiplier = 2.0
max_cliff_ratio = 3.0
```

### 3c. GC Stress (Write-Heavy + Long Txns)

```toml
# bench_configs/latency/gc_stress.toml
[workload]
duration_sec = 600
concurrent_connections = 100
target_tps = 1000
mix = { select = 20, insert = 40, update = 30, delete = 10 }
table_count = 5
rows_per_table = 500000
long_txn_ratio = 0.05
long_txn_duration_ms = 5000

[thresholds]
max_gc_sweep_us = 5000
max_chain_length = 100
max_p999_ms = 200
```

### 3d. Cross-Shard Distributed Query

```toml
# bench_configs/latency/cross_shard.toml
[workload]
duration_sec = 300
concurrent_connections = 50
target_tps = 200
shard_count = 4
cross_shard_ratio = 0.5
agg_query_ratio = 0.3
limit_query_ratio = 0.4

[thresholds]
max_p99_ms = 50
max_network_bytes_per_query = 1048576
```

---

## 4. Run Commands

```bash
# Full evidence pack (all 4 benchmarks)
./scripts/run_latency_pack.sh

# Individual benchmarks
cargo run -p falcon_bench -- --config bench_configs/latency/steady_state.toml
cargo run -p falcon_bench -- --config bench_configs/latency/overload_ramp.toml
cargo run -p falcon_bench -- --config bench_configs/latency/gc_stress.toml
cargo run -p falcon_bench -- --config bench_configs/latency/cross_shard.toml
```

---

## 5. Expected Results

### 5a. Steady-State (Normal Load)

| Metric | Target | Expected |
|--------|--------|----------|
| p50 (ms) | < 5 | 1-3 |
| p99 (ms) | < 20 | 5-15 |
| p999 (ms) | < 100 | 10-50 |
| Accept rate | > 99% | ~100% |
| GC sweep (us) | < 5000 | 500-2000 |

### 5b. Overload Ramp

| QPS | p99 (ms) | p999 (ms) | Accept % | Reject Signal |
|-----|----------|-----------|----------|---------------|
| 100 | < 10 | < 30 | ~100% | — |
| 500 | < 15 | < 50 | ~100% | — |
| 1000 | < 20 | < 100 | > 95% | — |
| 2000 | < 25 | < 120 | > 70% | slow_path_quota |
| 5000 | < 30 | < 150 | > 40% | p999_exceeded |

**Key property**: p99 DOES NOT exhibit cliff (> 3x jump between levels).

### 5c. GC Stress

| Metric | Target | Notes |
|--------|--------|-------|
| GC sweep (us) | < 5000 | Budget-bounded |
| Max chain length | < 100 | Observable, bounded |
| p999 during GC | < 200ms | No GC-induced spike |
| GC budget exhaustion rate | < 10% | Healthy |

### 5d. Cross-Shard

| Metric | Target | Notes |
|--------|--------|-------|
| p99 (ms) | < 50 | Higher than single-shard |
| Bytes saved by pushdown | > 30% | LIMIT/filter pushdown |
| Partial agg usage | > 20% | Two-phase aggregation |

---

## 6. Comparison Points

### vs PostgreSQL (single-node)

| Metric | FalconDB | PostgreSQL | Notes |
|--------|----------|------------|-------|
| p99 under overload | Bounded by SLA | Unbounded queue | PG has no admission control |
| GC latency impact | < 5ms budget | Autovacuum spikes | PG autovacuum can cause seconds-long stalls |
| Reject behavior | Deterministic | Connection refused | PG has `max_connections` only |

### vs TiDB (distributed)

| Metric | FalconDB | TiDB | Notes |
|--------|----------|------|-------|
| p99 under overload | SLA-bounded | Varies | TiDB lacks SLA-driven admission |
| GC model | Budget-bounded incremental | Centralized GC | TiDB GC can cause hotspot stalls |
| Cross-shard latency | Pushdown + partial agg | Similar | Comparable distributed execution |
| Rejection explainability | Full (signal + reason) | Limited | FalconDB has per-rejection explanation |

---

## 7. Prometheus Queries

```promql
# p99 latency trend
histogram_quantile(0.99, falcon_sla_p99_us)

# Accept vs reject rate
rate(falcon_sla_total_accepted[1m]) / (rate(falcon_sla_total_accepted[1m]) + rate(falcon_sla_total_rejected[1m]))

# Rejection breakdown by signal
sum by (signal) (rate(falcon_sla_rejection_total[1m]))

# GC sweep duration distribution
histogram_quantile(0.99, falcon_gc_sweep_duration_us)

# Network byte savings
1 - (rate(falcon_dist_query_bytes_out_total[5m]) / rate(falcon_dist_query_bytes_in_total[5m]))

# Chain length max (should be bounded)
falcon_gc_max_chain_length
```

---

## 8. Artifact Checksums

After running the evidence pack, verify artifacts:

```bash
ls -la results/latency_evidence/
# Expected:
#   steady_state_results.json
#   overload_ramp_results.json
#   gc_stress_results.json
#   cross_shard_results.json
#   summary.md
```
