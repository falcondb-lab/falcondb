# FalconDB vs PostgreSQL — Benchmark Results

> **Version**: <!-- auto-filled by run_all.sh -->
> **Date**: <!-- auto-filled -->
> **Hardware**: <!-- auto-filled from environment capture -->

## How to Read This Document

- All numbers are from the **same machine**, same OS, same memory allocation
- FalconDB and PostgreSQL configs are in `falcondb/config.toml` and `postgresql/postgresql.conf`
- Raw data is in `results/` — every number here can be traced back to a file

## W1: Single-Table OLTP (70% SELECT, 20% UPDATE, 10% INSERT)

| Threads | FalconDB TPS | PG TPS | FalconDB p99 (ms) | PG p99 (ms) | Ratio |
|---------|-------------|--------|-------------------|-------------|-------|
| 1 | — | — | — | — | — |
| 4 | — | — | — | — | — |
| 8 | — | — | — | — | — |
| 16 | — | — | — | — | — |

## W2: Multi-Table Transaction (BEGIN → SELECT → INSERT → UPDATE × 2 → COMMIT)

| Threads | FalconDB TPS | PG TPS | FalconDB p99 (ms) | PG p99 (ms) | Ratio |
|---------|-------------|--------|-------------------|-------------|-------|
| 1 | — | — | — | — | — |
| 4 | — | — | — | — | — |
| 8 | — | — | — | — | — |
| 16 | — | — | — | — | — |

## Environment

```
OS:     <!-- from results/environment_*.txt -->
CPU:    <!-- cores, model -->
Memory: <!-- total RAM -->
Disk:   <!-- SSD type -->
```

## Configuration Parity

| Parameter | FalconDB | PostgreSQL |
|-----------|----------|------------|
| Memory allocation | 2 GB | shared_buffers = 2GB |
| WAL sync | fdatasync | wal_sync_method = fdatasync |
| Connections | 100 | max_connections = 100 |
| Compression | Off | N/A |

## Known Limitations

1. FalconDB is memory-first — advantages are expected for in-memory workloads
2. FalconDB SQL coverage is limited (no JOINs in benchmarks)
3. Single-node only; distributed benchmarks planned for v2.0
4. Results are hardware-dependent — run on your own hardware to verify

## Reproduction

```bash
# Quick (10s per test):
./benchmarks/scripts/run_all.sh --quick

# Full (60s per test):
./benchmarks/scripts/run_all.sh
```
