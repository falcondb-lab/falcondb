# FalconDB Benchmark Matrix — 4-Engine OLTP Comparison

> **Version**: <!-- auto-filled by run_matrix.sh -->
> **Date**: <!-- auto-filled -->
> **Hardware**: <!-- auto-filled from environment capture -->
> **Engines**: FalconDB · PostgreSQL · VoltDB · SingleStore

## How to Read This Document

- All numbers are from the **same machine**, same OS, same memory allocation (2 GB)
- Each engine's config is in its own directory under `benchmarks/`
- Raw data is in `results/matrix_<timestamp>/` — every number can be traced to a file
- `—` means the engine was not installed or the run failed

---

## W1: Single-Table OLTP (70% SELECT, 20% UPDATE, 10% INSERT)

Point-lookup and point-update workload. Tests index lookup speed and
basic concurrency control overhead.

| Threads | FalconDB TPS | PG TPS | VoltDB TPS | SingleStore TPS | FalconDB p99 | PG p99 | VoltDB p99 | SS p99 |
|---------|-------------|--------|------------|-----------------|-------------|--------|------------|--------|
| 1       | —           | —      | —          | —               | —           | —      | —          | —      |
| 4       | —           | —      | —          | —               | —           | —      | —          | —      |
| 8       | —           | —      | —          | —               | —           | —      | —          | —      |
| 16      | —           | —      | —          | —               | —           | —      | —          | —      |

## W2: Multi-Table Transaction (BEGIN → SELECT → INSERT → UPDATE × 2 → COMMIT)

Full ACID transaction spanning two tables. Tests transaction coordinator
overhead, lock granularity, and WAL write amplification.

| Threads | FalconDB TPS | PG TPS | VoltDB TPS | SingleStore TPS | FalconDB p99 | PG p99 | VoltDB p99 | SS p99 |
|---------|-------------|--------|------------|-----------------|-------------|--------|------------|--------|
| 1       | —           | —      | —          | —               | —           | —      | —          | —      |
| 4       | —           | —      | —          | —               | —           | —      | —          | —      |
| 8       | —           | —      | —          | —               | —           | —      | —          | —      |
| 16      | —           | —      | —          | —               | —           | —      | —          | —      |

## W3: Analytic Range Scan (COUNT/SUM/AVG over 10% of rows)

Aggregate query over a range of 10K rows. Tests scan throughput,
column-vs-row storage trade-offs, and aggregation execution speed.

| Threads | FalconDB TPS | PG TPS | VoltDB TPS | SingleStore TPS | FalconDB p99 | PG p99 | VoltDB p99 | SS p99 |
|---------|-------------|--------|------------|-----------------|-------------|--------|------------|--------|
| 1       | —           | —      | —          | —               | —           | —      | —          | —      |
| 4       | —           | —      | —          | —               | —           | —      | —          | —      |
| 8       | —           | —      | —          | —               | —           | —      | —          | —      |
| 16      | —           | —      | —          | —               | —           | —      | —          | —      |

## W4: Hot-Key Contention (10 hot keys, all threads competing)

All threads UPDATE the same 10 rows. Tests lock/latch contention,
MVCC retry cost, and abort rate under extreme skew.

| Threads | FalconDB TPS | PG TPS | VoltDB TPS | SingleStore TPS | FalconDB p99 | PG p99 | VoltDB p99 | SS p99 |
|---------|-------------|--------|------------|-----------------|-------------|--------|------------|--------|
| 1       | —           | —      | —          | —               | —           | —      | —          | —      |
| 4       | —           | —      | —          | —               | —           | —      | —          | —      |
| 8       | —           | —      | —          | —               | —           | —      | —          | —      |
| 16      | —           | —      | —          | —               | —           | —      | —          | —      |

## W5: Batch Insert Throughput (single-row INSERT into append table)

Sustained INSERT into a fresh table. Tests WAL write speed, index
maintenance overhead, and memory allocation throughput.

| Threads | FalconDB TPS | PG TPS | VoltDB TPS | SingleStore TPS | FalconDB p99 | PG p99 | VoltDB p99 | SS p99 |
|---------|-------------|--------|------------|-----------------|-------------|--------|------------|--------|
| 1       | —           | —      | —          | —               | —           | —      | —          | —      |
| 4       | —           | —      | —          | —               | —           | —      | —          | —      |
| 8       | —           | —      | —          | —               | —           | —      | —          | —      |
| 16      | —           | —      | —          | —               | —           | —      | —          | —      |

---

## Environment

```
OS:     <!-- from results/matrix_*/environment.txt -->
CPU:    <!-- cores, model -->
Memory: <!-- total RAM -->
Disk:   <!-- SSD type -->
```

## Configuration Parity

All engines configured for **fair single-node comparison**:

| Parameter          | FalconDB     | PostgreSQL           | VoltDB                  | SingleStore           |
|--------------------|-------------|----------------------|-------------------------|-----------------------|
| Memory allocation  | 2 GB        | shared_buffers = 2GB | cluster: 2GB            | max_memory = 2048 MB  |
| WAL / durability   | fdatasync   | wal_sync_method      | sync command log        | sync_permissions = ON |
| Max connections     | 100         | 100                  | N/A (partitioned)       | 100                   |
| Storage model      | In-memory   | Disk (buffered)      | In-memory               | Rowstore (in-memory)  |
| Replication         | Off         | Off                  | k=0 (no replication)    | redundancy_level = 1  |

## Engine Characteristics (Fair-Comparison Disclosure)

| Engine      | Architecture    | Storage          | Concurrency Model     | Expected Advantage            |
|-------------|----------------|------------------|-----------------------|-------------------------------|
| FalconDB    | Single-node    | In-memory + WAL  | MVCC (snapshot)       | Low-latency point ops         |
| PostgreSQL  | Single-node    | Disk + buffer    | MVCC (SSI)            | Mature optimizer, broad SQL   |
| VoltDB      | Partitioned    | In-memory        | Deterministic serial  | Partition-local throughput    |
| SingleStore | Distributed    | Rowstore + col.  | MVCC (lock-free reads)| Scan/analytic throughput      |

## Known Caveats

1. **FalconDB** is memory-first — advantages are expected for in-memory workloads
2. **VoltDB** uses deterministic execution — partition-local ops avoid locking but
   cross-partition transactions are expensive (W2 may underperform)
3. **SingleStore** rowstore tables are used for OLTP fairness; columnstore would
   benefit W3 (analytic scan) significantly
4. **PostgreSQL** is disk-based; sufficient shared_buffers ensure data fits in memory
5. All results are **single-node only** — distributed topologies not tested here
6. Results are **hardware-dependent** — run on your own hardware to verify

## Reproduction

```bash
# Full matrix (60s per run, ~80 runs → ~80 min with 4 engines):
./benchmarks/scripts/run_matrix.sh

# Quick mode (10s per run → ~14 min):
./benchmarks/scripts/run_matrix.sh --quick

# Subset of engines:
./benchmarks/scripts/run_matrix.sh --engines falcondb,postgresql

# Subset of workloads:
./benchmarks/scripts/run_matrix.sh --workloads w1,w2,w4

# Original 2-engine comparison (backward compatible):
./benchmarks/scripts/run_all.sh
```
