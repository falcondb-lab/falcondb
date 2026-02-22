# FalconDB Performance Testing & Regression Gate

## Overview

FalconDB ships a commercial-grade performance verification system built into `falcon_bench`.
It provides:

- **Baseline freeze** — TPS and P99 latency captured as versioned contracts
- **Regression detection** — CI gate fails if TPS drops or P99 rises beyond thresholds
- **Invariant enforcement** — fast-path P99 ≤ slow-path P99, scale-out TPS monotonic
- **Long-run stability** — continuous metrics sampling over configurable duration
- **Failover resilience** — TPS/latency before/during/after failover + data integrity

## Quick Start

```bash
# Default YCSB workload
cargo run -p falcon_bench -- --ops 10000 --export json

# Save a performance baseline
cargo run -p falcon_bench -- --ops 10000 --baseline-save

# Check against baseline (fail on regression)
cargo run -p falcon_bench -- --ops 10000 --baseline-check

# Fast-path vs slow-path comparison
cargo run -p falcon_bench -- --ops 10000 --compare

# Scale-out benchmark (1/2/4/8 shards)
cargo run -p falcon_bench -- --ops 5000 --scaleout

# Failover benchmark
cargo run -p falcon_bench -- --ops 5000 --failover

# Long-run stress test
cargo run -p falcon_bench -- --long-run --duration 1h --sample-interval-secs 10

# TPC-B (pgbench) workload
cargo run -p falcon_bench -- --tpcb --ops 10000

# LSM disk-backed KV benchmark
cargo run -p falcon_bench -- --lsm --ops 10000
```

## CLI Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--ops` | 10000 | Total operations per benchmark |
| `--read-pct` | 50 | Read percentage (0-100) |
| `--local-pct` | 80 | Local (single-shard) txn percentage |
| `--shards` | 4 | Number of logical shards |
| `--record-count` | 1000 | Pre-loaded rows |
| `--isolation` | rc | Isolation level: `rc` or `si` |
| `--seed` | 42 | Deterministic random seed |
| `--threads` | 1 | Concurrency level |
| `--export` | text | Output format: `text`, `csv`, `json` |
| `--compare` | false | Run fast-path vs slow-path comparison |
| `--scaleout` | false | Run shard scale-out benchmark |
| `--failover` | false | Run failover benchmark |
| `--tpcb` | false | Run TPC-B workload |
| `--lsm` | false | Run LSM KV benchmark |
| `--long-run` | false | Run long-run stress test |
| `--duration` | 60s | Duration for long-run (e.g. `60s`, `10m`, `1h`, `24h`) |
| `--sample-interval-secs` | 5 | Metrics sampling interval |
| `--baseline-save` | false | Save results as baseline |
| `--baseline-check` | false | Check results against baseline |
| `--baseline-path` | perf_baseline.json | Baseline file path |
| `--tps-threshold-pct` | 10 | TPS regression threshold (%) |
| `--p99-threshold-pct` | 20 | P99 regression threshold (%) |

## Performance Invariants (P0-1)

The following invariants are enforced as code-level contracts:

1. **TPS floor**: Current TPS must not drop below `baseline_tps * (1 - tps_threshold_pct/100)`
2. **P99 ceiling**: Current P99 must not exceed `baseline_p99 * (1 + p99_threshold_pct/100)`
3. **Fast > Slow**: Fast-path (LocalTxn) P99 must be ≤ slow-path (GlobalTxn) P99
4. **Scale-out monotonic**: TPS must not decrease as shard count increases (5% tolerance)
5. **Long-run stable**: Interval TPS must not drop below 50% of peak TPS

## Output Schema (Stable)

### YCSB JSON Output

```json
{
  "label": "YCSB Workload A",
  "seed": 42,
  "ops": 10000,
  "elapsed_ms": 1234,
  "tps": 8103.7,
  "txn_mix": {
    "local_count": 8000,
    "global_count": 2000,
    "local_pct": 80.0,
    "global_pct": 20.0
  },
  "committed": 9500,
  "fast_path_commits": 7800,
  "slow_path_commits": 1700,
  "aborted": 500,
  "latency": {
    "fast_path": { "count": 7800, "p50_us": 5, "p95_us": 12, "p99_us": 25 },
    "slow_path": { "count": 1700, "p50_us": 15, "p95_us": 35, "p99_us": 60 },
    "all":       { "count": 9500, "p50_us": 7,  "p95_us": 20, "p99_us": 40 }
  }
}
```

### CSV Header (Stable)

```
label,ops,elapsed_ms,tps,committed,fast,slow,aborted,occ,constraint,fast_p50,fast_p95,fast_p99,slow_p50,slow_p95,slow_p99,all_p50,all_p95,all_p99
```

### Baseline JSON

```json
{
  "version": "1.0.0-rc.1",
  "timestamp": "1740000000",
  "seed": 42,
  "ops": 10000,
  "record_count": 1000,
  "shards": 4,
  "read_pct": 50,
  "local_pct": 80,
  "tps": 8103.7,
  "fast_path_p99_us": 25,
  "slow_path_p99_us": 60,
  "all_p99_us": 40,
  "fast_path_p50_us": 5,
  "slow_path_p50_us": 15,
  "all_p50_us": 7
}
```

## CI Performance Gate

```bash
# Run all 5 gates
./scripts/ci_perf_gate.sh

# Save new baseline
./scripts/ci_perf_gate.sh --save-baseline

# Fast mode (skip long-run)
./scripts/ci_perf_gate.sh --fast
```

### Gates

| Gate | Name | Blocking |
|------|------|----------|
| 1 | YCSB baseline regression (TPS + P99) | Yes |
| 2 | Fast-path vs slow-path invariant | Yes |
| 3 | Scale-out TPS monotonicity | Warning |
| 4 | Failover data integrity | Yes |
| 5 | Long-run stability (30s smoke) | Yes |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FALCON_PERF_OPS` | 5000 | Operations per gate |
| `FALCON_PERF_RECORDS` | 500 | Record count |
| `FALCON_PERF_SEED` | 42 | Random seed |
| `FALCON_PERF_TPS_PCT` | 10 | TPS regression threshold % |
| `FALCON_PERF_P99_PCT` | 20 | P99 regression threshold % |

## Long-Run Stress Test (P4-1)

The long-run mode continuously executes the YCSB workload for a configurable duration,
sampling metrics at regular intervals. It verifies:

- No OOM or crash
- P99 latency does not explode
- TPS remains stable (min interval TPS ≥ 50% of peak)

During the test, MVCC GC, compaction, flush, and WAL fsync all run concurrently
(via the in-memory storage engine's background subsystems).

### Long-Run JSON Report

```json
{
  "duration_secs": 3600,
  "total_ops": 28000000,
  "avg_tps": 7777.8,
  "min_interval_tps": 6500.0,
  "max_interval_tps": 8200.0,
  "final_p99_us": 42,
  "max_p99_us": 65,
  "total_committed": 27500000,
  "total_aborted": 500000,
  "samples": [ ... ],
  "stable": true
}
```
