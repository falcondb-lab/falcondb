# FalconDB Benchmark Methodology

This document defines the rules, constraints, and reproducibility requirements for all FalconDB benchmarks. Any published result **must** comply with these rules or explicitly state deviations.

## 1. Hardware & Environment Disclosure

Every benchmark report **must** include:

| Field           | Required | Example                         |
|----------------|----------|---------------------------------|
| CPU model       | Yes      | AMD Ryzen 9 7950X (16C/32T)    |
| CPU cores       | Yes      | 16 physical, 32 logical         |
| RAM             | Yes      | 64 GB DDR5-5600                 |
| Disk            | Yes      | Samsung 990 Pro 2TB NVMe        |
| OS              | Yes      | Ubuntu 24.04 LTS / Windows 11   |
| Kernel          | Linux    | 6.8.0-45-generic                |
| File system     | Yes      | ext4 / NTFS                     |
| Git commit      | Yes      | `git rev-parse HEAD`            |
| FalconDB config | Yes      | Full TOML file attached          |

Collect automatically with the benchmark scripts (`environment.txt`).

## 2. Configuration Disclosure

### FalconDB
- The **exact config file** used must be saved alongside results.
- WAL **must be enabled** (`wal_enabled = true`).
- `sync_mode` must be stated explicitly (default: `fdatasync`).
- Authentication mode must be stated (TRUST or SCRAM-SHA-256).

### PostgreSQL
At minimum, disclose these `postgresql.conf` settings:
- `shared_buffers`
- `wal_level`
- `synchronous_commit`
- `fsync`
- `max_connections`
- `work_mem`
- `checkpoint_timeout`

**Fairness rule**: If PostgreSQL uses `synchronous_commit = on` and `fsync = on`, FalconDB must use `wal_enabled = true` and `sync_mode = fdatasync`. No asymmetric durability settings.

## 3. Fairness Rules

These rules are **non-negotiable** for any comparison benchmark:

1. **Same dataset**: Identical `pgbench -s SCALE` for both targets.
2. **Same concurrency**: Identical `-c` and `-j` values.
3. **Same duration**: Identical `-T` duration.
4. **Same workload**: Identical pgbench mode/script.
5. **WAL enabled**: Both systems must have WAL/durability enabled.
6. **No cheating**:
   - No `fsync=off` on either side.
   - No disabling WAL on either side.
   - No unrealistic `shared_buffers` (e.g., 100% of RAM).
   - No OS-level tuning on one side but not the other.
7. **Same machine**: Both must run on the same hardware, or hardware must be disclosed and comparable.

## 4. Warm-up & Run Count

- **Warm-up**: At least 1 warm-up run (default 15s) before recorded runs.
- **Recorded runs**: At least 3 (default). Report mean and individual values.
- **Duration**: At least 60 seconds per recorded run.
- **Cooldown**: No explicit cooldown required, but sequential runs should have a brief pause.

## 5. Metrics Collected

| Metric                  | Source    | Notes                              |
|------------------------|-----------|-------------------------------------|
| TPS (incl connections) | pgbench   | Includes connection setup overhead  |
| TPS (excl connections) | pgbench   | Steady-state throughput             |
| Latency average (ms)   | pgbench   | Mean transaction latency            |
| Latency p95 (ms)       | pgbench   | If available (requires `-r` flag)   |
| Errors                 | pgbench   | Any reported errors                 |

## 6. Benchmark Types

### 6.1 pgbench Comparison (`bench_pgbench_vs_postgres.sh`)
- **Purpose**: Customer-facing SQL-layer comparison.
- **Modes**: `tpcb_like` (default), `simple_update`, `read_only`, `custom`.
- **Interpretation**: Measures end-to-end SQL throughput including parsing, planning, execution, WAL, and network.

### 6.2 Failover Under Load (`bench_failover_under_load.sh`)
- **Purpose**: Demonstrate deterministic commit semantics under failure.
- **Key metric**: PASS/FAIL — all acknowledged commits survive failover.
- **Secondary**: Failover time, TPS recovery curve.
- **Not a throughput benchmark** — focus is correctness.

### 6.3 Kernel Benchmark (`bench_kernel_falcon_bench.sh`)
- **Purpose**: Internal engine performance measurement.
- **Measures**: Raw `StorageEngine` + `TxnManager` throughput.
- **⚠️ NOT comparable to pgbench**: No SQL parsing, no network, no connection management.
- **Use for**: Regression detection, fast-path vs slow-path comparison, scale-out analysis.

## 7. Reproducing Results

### Quick Start (< 10 minutes)

```bash
# 1. Build FalconDB
cargo build -p falcon_server --release

# 2. Start FalconDB (in a separate terminal)
./target/release/falcon_server -c bench_configs/falcon.bench.toml

# 3. Ensure PostgreSQL is running on port 5432

# 4. Run the benchmark
./scripts/bench_pgbench_vs_postgres.sh

# 5. View results
cat bench_out/*/REPORT.md
```

### Minimal Smoke Test

```bash
SCALE=1 CONCURRENCY=1 DURATION_SEC=5 WARMUP_SEC=2 RUNS=1 \
  ./scripts/bench_pgbench_vs_postgres.sh
```

### Kernel Benchmark

```bash
./scripts/bench_kernel_falcon_bench.sh
cat bench_out/*/kernel_bench/REPORT.md
```

### Failover Test

```bash
cargo build -p falcon_server --release
./scripts/bench_failover_under_load.sh
# Prints PASS/FAIL
```

## 8. Interpretation Guidance

### Throughput (TPS)
- Higher is better.
- Use "excluding connections" TPS for steady-state comparison.
- Report mean across runs; individual run variation > 10% suggests instability.

### Latency
- Lower is better.
- Average latency is useful for general comparison.
- p95/p99 tail latency reveals worst-case behavior (use `-r` flag with pgbench).
- Tail latency matters more for OLTP workloads.

### What Workloads Represent OLTP
- `tpcb_like`: Mixed read/write, representative of banking transactions.
- `simple_update`: Write-heavy, tests WAL throughput.
- `read_only`: Read-heavy, tests MVCC scan performance.

### What to Watch For
- TPS variance across runs (> 10% indicates instability).
- Error counts (should be 0 for valid benchmarks).
- Latency spikes (check system load, GC pauses).

## 9. Known Limitations

### Authentication
- FalconDB currently supports TRUST mode for benchmarks.
- SCRAM-SHA-256 is implemented but may add overhead. State which mode was used.

### SQL Compatibility
- FalconDB does not support all PostgreSQL SQL features.
- pgbench default scripts (`-b tpcb-like`) work correctly.
- Custom scripts should be tested against FalconDB's supported SQL subset.
- Known unsupported: `CREATE TRIGGER`, `CREATE FUNCTION`, stored procedures.

### Scale
- FalconDB is memory-first; very large scale factors may exceed available RAM.
- State the scale factor and verify data fits in memory for fair comparison.

## 10. Anti-Patterns (Do NOT)

- ❌ Run benchmarks on a busy machine with other workloads.
- ❌ Compare TRUST-mode FalconDB against SCRAM-mode PostgreSQL.
- ❌ Disable WAL or fsync on either side for "performance" numbers.
- ❌ Use different hardware and claim direct comparison.
- ❌ Cherry-pick the best run from multiple attempts.
- ❌ Claim TPC-B or TPC-C compliance (we do not certify).

## 11. Output Structure

All benchmarks produce artifacts under `bench_out/<timestamp>/`:

```
bench_out/
  20250227T103000Z/
    REPORT.md                    # Human-readable report
    summary_pgbench.json         # Machine-readable metrics
    environment.txt              # Hardware + OS snapshot
    falcon_config_used.toml      # Exact config used
    pgbench/
      falcon/
        init.log                 # Schema initialization
        warmup.log               # Warm-up run
        run1.log ... runN.log    # Recorded runs
      postgres/
        init.log
        warmup.log
        run1.log ... runN.log
    failover/                    # (if failover bench was run)
      summary_failover.json
      logs/node_a.log
      logs/node_b.log
      load/phase1.log
      load/phase2.log
    kernel_bench/                # (if kernel bench was run)
      REPORT.md
      raw_output.log
      summary_kernel.json
```
