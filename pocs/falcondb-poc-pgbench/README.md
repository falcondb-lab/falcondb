# FalconDB vs PostgreSQL — pgbench Performance Comparison

> **This benchmark answers one question:**
> "If I replace PostgreSQL with FalconDB for OLTP workloads,
> will I get better throughput and lower latency
> without sacrificing correctness or durability?"

Both systems are tested under identical conditions — same hardware, same
dataset, same concurrency, same duration, same durability settings.
No shortcuts. No tricks.

---

## What Is pgbench?

pgbench is the standard benchmarking tool that ships with PostgreSQL. It
simulates a simple banking workload: update an account balance, insert a
transaction record, look up a teller and branch. Every database engineer
knows pgbench. It is the baseline the industry uses to compare OLTP systems.

---

## Run It (One Command)

### What You Need

| Tool | What It Does | How to Get It |
|------|-------------|---------------|
| **FalconDB** | Database under test | `cargo build -p falcon_server --release` |
| **PostgreSQL** | Baseline comparison | [postgresql.org/download](https://www.postgresql.org/download/) |
| **pgbench** | Benchmark tool | Comes with PostgreSQL |
| **psql** | Database client | Comes with PostgreSQL |

### Linux / macOS

```bash
# Build FalconDB
cargo build -p falcon_server --release

# Run the full comparison
./falcondb-poc-pgbench/run_benchmark.sh
```

### Windows (PowerShell 7+)

```powershell
cargo build -p falcon_server --release
.\falcondb-poc-pgbench\run_benchmark.ps1
```

### What Happens

1. Checks that all tools are installed
2. Starts FalconDB (port 5433) and PostgreSQL (port 5432)
3. Initializes pgbench tables on both (same scale, same data)
4. Runs pgbench against FalconDB: 1 warm-up + 3 measured runs
5. Runs pgbench against PostgreSQL: 1 warm-up + 3 measured runs
6. Compares the median of 3 runs for each system
7. Generates a report

### Example Output

```
  System         TPS (median)    Avg Latency (ms)
  -------------- -------------- -----------------
  PostgreSQL          18,200.00             11.40
  FalconDB            41,900.00              4.80

  TPS ratio (FalconDB / PostgreSQL): 2.30x
  Latency ratio (PG / Falcon):       2.38x
```

*Actual numbers depend on your hardware. The relative comparison is what matters.*

---

## How to Read the Results

| Metric | What It Means |
|--------|---------------|
| **TPS** (transactions per second) | Higher = better. More work done per second. |
| **Avg Latency** (milliseconds) | Lower = better. Faster response per transaction. |
| **TPS ratio > 1.0** | FalconDB processed more transactions per second. |
| **Latency ratio > 1.0** | FalconDB responded faster per transaction. |

The report uses the **median** of 3 runs — not the best run, not the average.
The median is the most representative number for a small sample.

---

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PGBENCH_SCALE` | `10` | Dataset size (10 = 1M rows, ~150 MB) |
| `PGBENCH_CLIENTS` | `10` | Concurrent connections |
| `PGBENCH_JOBS` | `2` | Worker threads |
| `PGBENCH_DURATION` | `60` | Seconds per measured run |
| `PGBENCH_MODE` | `tpcb-like` | Workload (`tpcb-like`, `read-only`, `simple-update`) |

Example with higher load:

```bash
PGBENCH_SCALE=50 PGBENCH_CLIENTS=32 PGBENCH_DURATION=120 ./run_benchmark.sh
```

---

## Run Step by Step

```bash
# 1. Check prerequisites
./scripts/check_env.sh

# 2. Start both servers
./scripts/start_falcondb.sh
./scripts/start_postgres.sh

# 3. Initialize pgbench tables
./scripts/init_pgbench.sh

# 4. Run benchmarks
./scripts/run_pgbench_falcon.sh
./scripts/run_pgbench_postgres.sh

# 5. Collect and compare results
./scripts/collect_results.sh

# 6. Clean up
./scripts/cleanup.sh
```

---

## What This Benchmark Proves

- FalconDB can handle the same pgbench workload that PostgreSQL handles
- With durability enabled (WAL + fsync on both systems)
- On the same hardware, under the same conditions
- With competitive or superior throughput and latency

## What This Benchmark Does NOT Prove

- **"FalconDB is always faster"** — pgbench is one workload type. Your
  application may have different characteristics.
- **"PostgreSQL is slow"** — PostgreSQL is a mature, excellent database.
  This comparison is about relative performance, not absolutes.
- **"These numbers apply to production"** — Real workloads have joins,
  complex queries, varying data sizes, and network latency.
- **Analytics performance** — pgbench is an OLTP benchmark. It does not
  test reporting, aggregation, or complex queries.

---

## Durability: Both Systems Play Fair

This is not a "disable safety features to look fast" benchmark.

| Setting | FalconDB | PostgreSQL |
|---------|----------|------------|
| WAL | Enabled | Enabled (default) |
| Durable writes | `fdatasync` | `fdatasync` |
| Synchronous commit | Default (always sync) | `synchronous_commit = on` |
| Data persists after crash | Yes | Yes |

Both configs are in `conf/` — inspect them yourself.

---

## Output Files

```
results/
├── report.md                  ← Human-readable comparison report
├── raw/
│   ├── falcon/
│   │   ├── run_1.log          ← Raw pgbench output (all runs preserved)
│   │   ├── run_2.log
│   │   ├── run_3.log
│   │   ├── warmup.log
│   │   ├── init.log
│   │   ├── server.log
│   │   └── parameters.json
│   └── postgres/
│       ├── run_1.log
│       ├── run_2.log
│       ├── run_3.log
│       ├── warmup.log
│       ├── init.log
│       ├── server.log
│       └── parameters.json
└── parsed/
    ├── summary.json           ← Machine-readable results
    ├── environment.json       ← Hardware/software info
    └── init_metadata.json     ← Dataset initialization details
```

Every run is preserved. Nothing is discarded or cherry-picked.

---

## Methodology

See [docs/benchmark_methodology.md](docs/benchmark_methodology.md) for:

- Hardware disclosure requirements
- Configuration excerpts and equivalences
- Run methodology (warm-up, measured runs, median selection)
- Fairness constraints
- Anti-cheating checklist
- Known limitations
- Interpretation guidance

---

## Directory Structure

```
falcondb-poc-pgbench/
├── README.md                         ← You are here
├── run_benchmark.sh / .ps1           ← One-command benchmark
├── conf/
│   ├── falcon.bench.toml             ← FalconDB config
│   └── postgres.bench.conf           ← PostgreSQL config
├── scripts/
│   ├── check_env.sh / .ps1           ← Verify prerequisites
│   ├── start_falcondb.sh / .ps1      ← Start FalconDB
│   ├── start_postgres.sh / .ps1      ← Start PostgreSQL
│   ├── init_pgbench.sh / .ps1        ← Initialize pgbench tables
│   ├── run_pgbench_falcon.sh / .ps1  ← Benchmark FalconDB
│   ├── run_pgbench_postgres.sh / .ps1← Benchmark PostgreSQL
│   ├── collect_results.sh / .ps1     ← Parse and compare
│   └── cleanup.sh / .ps1             ← Stop and clean
├── results/                          ← Output appears here
└── docs/
    └── benchmark_methodology.md      ← Full methodology
```

---

## Relationship to Other PoCs

| PoC | What It Proves |
|-----|---------------|
| **PoC #1** (`falcondb-poc-dcg/`) | Durability: committed data survives crashes |
| **PoC #2** (`falcondb-poc-pgbench/`) | Performance: competitive OLTP throughput and latency |

PoC #1 proves FalconDB is *safe*. PoC #2 proves FalconDB is *fast*.
Together, they answer: **"Can I trust FalconDB with my transactional data?"**
