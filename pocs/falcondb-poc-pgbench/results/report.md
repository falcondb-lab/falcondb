# FalconDB vs PostgreSQL - pgbench Comparison Report

**Generated**: 2026-03-03T11:30:05Z

---

## Summary

| System       | TPS (median) | Avg Latency (ms) |
|:-------------|-------------:|------------------:|
| **PostgreSQL** | 12671.766065 | 0.789 |
| **FalconDB** | 2420.194394 | 4.131 |

**TPS ratio** (FalconDB / PostgreSQL): **0.19x**
**Latency ratio** (PostgreSQL / FalconDB): **0.19x**

---

## All Runs

### FalconDB

| Run | TPS | Avg Latency (ms) |
|:---:|----:|------------------:|
| 1 | 2420.194394 | 4.131 |
| 2 | 2306.978819 | 4.334 |
| 3 | 2794.665464 | 3.578 |
| **Median** | **2420.194394** | **4.131** |

### PostgreSQL

| Run | TPS | Avg Latency (ms) |
|:---:|----:|------------------:|
| 1 | 12671.766065 | 0.789 |
| 2 | 12858.913477 | 0.778 |
| 3 | 11627.029965 | 0.86 |
| **Median** | **12671.766065** | **0.789** |

---

## Methodology

- **Benchmark tool**: pgbench (built-in tpcb-like workload)
- **Warm-up**: 1 run (15s), not included in results
- **Measured runs**: 3 per system
- **Result selection**: Median of 3 runs
- **Durability**: WAL enabled, fsync/fdatasync on both systems
- **All raw output preserved** in `results/raw/`

---

## Fairness Statement

Both systems were tested on the same machine, same OS, same dataset scale,
same concurrency, same duration. WAL and durability were enabled on both.
No unsafe optimizations were applied to either system. All runs are preserved.

See `docs/benchmark_methodology.md` for full details.
