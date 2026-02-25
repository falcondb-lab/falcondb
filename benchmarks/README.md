# FalconDB Benchmark Matrix — Public Reproducible 4-Engine Comparison

## Overview

This directory contains **fully reproducible** benchmarks comparing FalconDB
against **PostgreSQL**, **VoltDB**, and **SingleStore** on identical hardware,
identical workloads, and identical measurement methodology.

**Design principle**: A third party with zero FalconDB knowledge can clone this
repo and reproduce every number. No internal tools, no secret parameters.

**Fair-comparison policy**: We disclose each engine's architectural advantages
and configure all engines to use the same memory budget (2 GB), same durability
level (synchronous WAL), and same connection limit. See [RESULTS.md](RESULTS.md)
for the full configuration parity table.

---

## Quick Start

```bash
# 1. Build FalconDB
cargo build --release -p falcon_server

# 2. Run the full 4-engine benchmark matrix
chmod +x benchmarks/scripts/run_matrix.sh
./benchmarks/scripts/run_matrix.sh --quick   # 10s per run (~14 min)

# 3. View results
cat benchmarks/results/matrix_*/MATRIX_REPORT.md

# Or run only specific engines:
./benchmarks/scripts/run_matrix.sh --engines falcondb,postgresql

# Original 2-engine comparison (backward compatible):
./benchmarks/scripts/run_all.sh
```

Engines not installed on the machine are **automatically skipped**.

---

## Directory Structure

```
benchmarks/
├── README.md                       # This file
├── RESULTS.md                      # Results template (4-engine × 5-workload)
├── workloads/
│   ├── setup.sql                   # Shared schema (PG-compatible)
│   ├── setup_voltdb.sql            # VoltDB-specific schema (partitioned)
│   ├── setup_singlestore.sql       # SingleStore-specific schema (rowstore)
│   ├── setup_batch.sql             # W5 batch insert table
│   ├── single_table_oltp.sql       # W1: point SELECT/UPDATE/INSERT
│   ├── multi_table_txn.sql         # W2: multi-table ACID transaction
│   ├── analytic_scan.sql           # W3: range scan with aggregation
│   ├── hot_contention.sql          # W4: hot-key contention (10 keys)
│   └── batch_insert.sql            # W5: sustained INSERT throughput
├── falcondb/
│   ├── config.toml                 # FalconDB config (2 GB memory)
│   └── run.sh                      # Start FalconDB
├── postgresql/
│   ├── postgresql.conf             # PG config (shared_buffers=2GB)
│   └── run.sh                      # Start PostgreSQL
├── voltdb/
│   ├── deployment.xml              # VoltDB deployment (2 GB, sync cmd log)
│   └── run.sh                      # Start VoltDB
├── singlestore/
│   ├── singlestore.cnf             # SingleStore config (max_memory=2048)
│   └── run.sh                      # Start SingleStore (Docker or native)
├── scripts/
│   ├── run_matrix.sh               # One-click: 4-engine × 5-workload matrix
│   ├── run_all.sh                  # Legacy: FalconDB vs PG only
│   └── run_workload.sh             # Run a single workload against one target
└── results/                        # Raw output (gitignored)
    └── matrix_<timestamp>/         # Per-run results directory
```

---

## Workloads

### W1: Single-Table OLTP

| Property | Value |
|----------|-------|
| Schema | `accounts(id PK, balance, name)` |
| Operations | 70% point SELECT, 20% UPDATE, 10% INSERT |
| Concurrency | 1, 4, 8, 16 threads |
| Duration | 60s (full) / 10s (quick) |
| Data | 100K pre-loaded rows |
| Tests | Index lookup speed, basic concurrency overhead |

### W2: Multi-Table Transaction

| Property | Value |
|----------|-------|
| Schema | `accounts` + `transfers` |
| Operations | BEGIN → SELECT → INSERT → UPDATE × 2 → COMMIT |
| Concurrency | 1, 4, 8, 16 threads |
| Duration | 60s / 10s |
| Tests | ACID coordination, lock granularity, WAL write amplification |

### W3: Analytic Range Scan

| Property | Value |
|----------|-------|
| Schema | `accounts` |
| Operations | `SELECT COUNT, SUM, AVG, MIN, MAX WHERE id BETWEEN ...` |
| Range | 10K rows (10% of table) per query |
| Concurrency | 1, 4, 8, 16 threads |
| Tests | Scan throughput, aggregation speed, row-vs-column trade-offs |

### W4: Hot-Key Contention

| Property | Value |
|----------|-------|
| Schema | `accounts` |
| Operations | `UPDATE accounts SET balance = balance + delta WHERE id = :hot_id` |
| Hot keys | 10 (extreme skew) |
| Concurrency | 1, 4, 8, 16 threads |
| Tests | Lock contention, MVCC retry cost, abort rate |

### W5: Batch Insert Throughput

| Property | Value |
|----------|-------|
| Schema | `bench_events(id PK, account_id, event_type, amount, created_at)` |
| Operations | Single-row INSERT with random PK |
| Concurrency | 1, 4, 8, 16 threads |
| Tests | WAL write speed, index maintenance, memory allocation |

---

## Metrics Collected

| Metric | Unit | Collection Method |
|--------|------|------------------|
| TPS | txn/sec | Client-side completed txns / elapsed time |
| Avg latency | ms | Client-side mean |
| p99 latency | ms | Client-side 99th percentile |
| p99.9 latency | ms | Client-side 99.9th percentile |
| Abort rate | % | Aborted / total attempted |
| CPU usage | % | OS-level sampling during run |
| RSS memory | MB | OS-level sampling during run |

---

## Engine Installation

### FalconDB

```bash
cargo build --release -p falcon_server
```

### PostgreSQL

```bash
# Debian/Ubuntu
sudo apt install postgresql postgresql-contrib

# macOS
brew install postgresql

# Verify
psql --version && pgbench --version
```

### VoltDB (Community Edition)

```bash
# Download from https://www.voltdb.com/try-voltdb/
# Extract and set:
export VOLTDB_HOME=/path/to/voltdb

# Verify
$VOLTDB_HOME/bin/voltdb --version
```

### SingleStore

```bash
# Docker (recommended):
docker pull singlestore/cluster-in-a-box:latest
export SINGLESTORE_LICENSE="your-free-license-key"
# Get a free license at https://www.singlestore.com/cloud-trial/

# Or native install via sdb-deploy
```

---

## Hardware Requirements

- **Minimum**: 4 cores, 8 GB RAM, SSD
- **Recommended**: 8+ cores, 16+ GB RAM, NVMe SSD
- All engines run on the **same machine** to eliminate network variance
- No other significant workload during benchmark

---

## Configuration Parity

All engines configured for **fair single-node comparison**:

| Parameter | FalconDB | PostgreSQL | VoltDB | SingleStore |
|-----------|----------|------------|--------|-------------|
| Memory | 2 GB | shared_buffers=2GB | 2 GB cluster | max_memory=2048 |
| WAL sync | fdatasync | fdatasync | sync cmd log | sync_permissions=ON |
| Connections | 100 | 100 | partitioned | 100 |
| Replication | Off | Off | k=0 | redundancy=1 |
| Logging | Minimal | Minimal | Default | Default |

---

## Fair-Comparison Disclosure

We believe benchmarks should be honest about each engine's design trade-offs:

| Engine | Design | Where It Should Excel | Where It May Struggle |
|--------|--------|----------------------|----------------------|
| **FalconDB** | In-memory MVCC | W1 (point ops), W5 (insert) | W3 (no columnstore) |
| **PostgreSQL** | Disk-based MVCC | W2 (mature txn manager) | W1 (disk I/O overhead) |
| **VoltDB** | Deterministic serial | W1 (partition-local) | W2 (cross-partition txn) |
| **SingleStore** | Hybrid row+column | W3 (scan), W5 (insert) | W4 (lock overhead) |

---

## Reproducing Results

1. **Clone**: `git clone <repo-url> && cd falcondb`
2. **Build FalconDB**: `cargo build --release -p falcon_server`
3. **Install engines**: See [Engine Installation](#engine-installation) above
4. **Run**: `./benchmarks/scripts/run_matrix.sh`
5. **Compare**: Results in `benchmarks/results/matrix_<timestamp>/MATRIX_REPORT.md`

The `run_matrix.sh` script is fully automated. Engines not installed are skipped.
No manual intervention required.
