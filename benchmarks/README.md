# FalconDB Benchmarks — Public, Reproducible OLTP Comparison

## Overview

This directory contains **fully reproducible** benchmarks comparing FalconDB
against PostgreSQL on identical hardware, identical workloads, and identical
measurement methodology.

**Design principle**: A third party with zero FalconDB knowledge can clone this
repo and reproduce the results. No internal tools, no secret parameters.

## Quick Start

```bash
# Prerequisites: FalconDB built, PostgreSQL installed, psql available
# 1. Build FalconDB
cargo build --release -p falcon_server

# 2. Run all benchmarks (FalconDB + PostgreSQL, same machine)
chmod +x benchmarks/scripts/run_all.sh
./benchmarks/scripts/run_all.sh

# 3. View results
cat benchmarks/RESULTS.md
```

## Directory Structure

```
benchmarks/
├── README.md                  # This file
├── RESULTS.md                 # Latest results template + baseline
├── workloads/
│   ├── single_table_oltp.sql  # Workload W1: single-table point ops
│   ├── multi_table_txn.sql    # Workload W2: multi-table transactions
│   └── setup.sql              # Schema creation (shared)
├── falcondb/
│   ├── config.toml            # FalconDB config for benchmarks
│   └── run.sh                 # Start FalconDB for benchmark
├── postgresql/
│   ├── postgresql.conf        # PostgreSQL config for benchmarks
│   └── run.sh                 # Start PostgreSQL for benchmark
├── scripts/
│   ├── run_all.sh             # One-click: run everything
│   ├── run_workload.sh        # Run a single workload against a target
│   ├── collect_metrics.sh     # Collect TPS, latency, resource usage
│   └── generate_report.sh    # Generate RESULTS.md from raw data
└── results/                   # Raw output (gitignored)
    └── .gitkeep
```

## Workloads

### W1: Single-Table OLTP

- **Schema**: `accounts(id INT PRIMARY KEY, balance BIGINT, name TEXT)`
- **Operations**: 70% point SELECT, 20% UPDATE, 10% INSERT
- **Concurrency**: 1, 4, 8, 16 threads
- **Duration**: 60 seconds per concurrency level
- **Rows**: Pre-loaded 100K rows

### W2: Multi-Table Transaction

- **Schema**: `accounts` + `transfers(id SERIAL, from_id INT, to_id INT, amount BIGINT, ts TIMESTAMP)`
- **Operations**: BEGIN → SELECT account → INSERT transfer → UPDATE balance → COMMIT
- **Concurrency**: 1, 4, 8, 16 threads
- **Duration**: 60 seconds per concurrency level
- **Rows**: Pre-loaded 100K accounts

## Metrics Collected

| Metric | Unit | Collection Method |
|--------|------|------------------|
| TPS | txn/sec | Client-side completed txns / elapsed time |
| Avg latency | µs | Client-side mean |
| p99 latency | µs | Client-side 99th percentile |
| p99.9 latency | µs | Client-side 99.9th percentile |
| Abort rate | % | Aborted / total attempted |
| CPU usage | % | `top` / `Get-Process` sampling |
| RSS memory | MB | `/proc/PID/status` / `Get-Process` |

## Hardware Requirements

- **Minimum**: 4 cores, 8 GB RAM, SSD
- **Recommended**: 8 cores, 16 GB RAM, NVMe SSD
- Both databases run on the **same machine** to eliminate network variance
- No other significant workload during benchmark

## Configuration Parity

Both databases are configured for **fair comparison**:

| Parameter | FalconDB | PostgreSQL |
|-----------|----------|------------|
| Shared buffers / memory | 2 GB | `shared_buffers = 2GB` |
| WAL sync | fdatasync | `wal_sync_method = fdatasync` |
| Max connections | 100 | `max_connections = 100` |
| Checkpoint interval | Default | `checkpoint_timeout = 5min` |
| Logging | Minimal | `log_min_duration_statement = -1` |

## Known Limitations

- FalconDB is memory-first; PostgreSQL is disk-first. Memory-bound workloads
  favor FalconDB by design.
- FalconDB does not support full SQL (no JOINs, no subqueries in benchmarks).
- Results are hardware-dependent; absolute numbers will vary.
- These benchmarks measure **single-node** performance only.

## Reproducing Results

1. **Clone the repo**: `git clone <repo-url> && cd falcondb`
2. **Build**: `cargo build --release -p falcon_server`
3. **Install PostgreSQL**: `apt install postgresql` or equivalent
4. **Run**: `./benchmarks/scripts/run_all.sh`
5. **Compare**: Results written to `benchmarks/results/` and `benchmarks/RESULTS.md`

The `run_all.sh` script is fully automated and requires no manual intervention.
