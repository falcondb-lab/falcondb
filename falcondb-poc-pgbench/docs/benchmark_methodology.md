# FalconDB vs PostgreSQL — Benchmark Methodology

This document describes the exact methodology used in the pgbench comparison
PoC. It is intended for technical due diligence and reproducibility.

---

## 1. Objective

Answer one question:

> "If I replace PostgreSQL with FalconDB for OLTP workloads, will I get
> better throughput and lower latency without sacrificing correctness
> or durability?"

This benchmark does NOT attempt to:
- Claim universal performance superiority
- Replace TPC-C or other industry-standard benchmarks
- Benchmark analytics, reporting, or complex queries
- Test distributed or multi-node performance

---

## 2. Benchmark Tool

**pgbench** — the standard PostgreSQL benchmarking tool.

- Ships with every PostgreSQL installation
- Well-understood by the industry
- Produces reproducible, comparable results
- Default `tpcb-like` workload models simple OLTP (UPDATE + SELECT + INSERT)

We use pgbench unmodified. No custom scripts, no source patches.

---

## 3. Fairness Constraints (Non-Negotiable)

| Rule | FalconDB | PostgreSQL | Enforced By |
|------|----------|------------|-------------|
| Same hardware | ✓ | ✓ | Both run on same machine |
| Same OS | ✓ | ✓ | Both run on same OS instance |
| Same dataset scale | ✓ | ✓ | `pgbench -i -s <SCALE>` |
| Same concurrency | ✓ | ✓ | `-c` and `-j` flags |
| Same duration | ✓ | ✓ | `-T` flag |
| WAL enabled | ✓ | ✓ | Config files |
| Durable writes (fsync) | ✓ (`fdatasync`) | ✓ (`fdatasync`) | Config files |
| No unsafe optimizations | ✓ | ✓ | Reviewed configs |
| Warm-up before measurement | ✓ | ✓ | 15s warm-up run |
| Multiple runs | ✓ (3 runs) | ✓ (3 runs) | Script enforced |
| Median selection | ✓ | ✓ | `collect_results.sh` |

**Violation of any rule invalidates the benchmark.**

---

## 4. Hardware Disclosure

Hardware details are captured automatically by `check_env.sh` and saved to
`results/parsed/environment.json`. The report must include:

- CPU model and core count
- RAM size
- Storage type (HDD / SSD / NVMe)
- OS name and version

If the benchmark is run on a cloud instance, the instance type must be recorded.

---

## 5. Software Versions

| Component | How to Check |
|-----------|-------------|
| FalconDB | `falcon_server --version` |
| PostgreSQL | `psql --version` or `SHOW server_version;` |
| pgbench | `pgbench --version` |
| OS | `uname -a` (Linux/macOS) or `winver` (Windows) |

All versions are recorded in `results/parsed/environment.json`.

---

## 6. Configuration

### FalconDB (`conf/falcon.bench.toml`)

```toml
[storage]
wal_enabled = true

[wal]
sync_mode = "fdatasync"
group_commit = true

[server]
max_connections = 200

[auth]
method = "trust"
```

### PostgreSQL (`conf/postgres.bench.conf`)

```
fsync = on
synchronous_commit = on
wal_sync_method = fdatasync
shared_buffers = 256MB
max_connections = 200
```

Both configurations are committed to the repository and can be inspected.

**Key equivalences**:
- FalconDB `sync_mode = "fdatasync"` ≡ PostgreSQL `wal_sync_method = fdatasync`
- FalconDB `wal_enabled = true` ≡ PostgreSQL default (WAL always on)
- Both use `trust` authentication for local benchmark connections

---

## 7. Dataset

pgbench initializes with `pgbench -i -s <SCALE>`:

| Scale | Accounts | Branches | Tellers | History (initial) |
|------:|---------:|---------:|--------:|------------------:|
| 1 | 100,000 | 1 | 10 | 0 |
| 10 | 1,000,000 | 10 | 100 | 0 |
| 50 | 5,000,000 | 50 | 500 | 0 |
| 100 | 10,000,000 | 100 | 1,000 | 0 |

Default scale for this PoC: **10** (1M accounts, ~150 MB).

Both systems are initialized with the exact same `pgbench -i -s <SCALE>` command.
Row counts are verified after initialization.

---

## 8. Run Methodology

### Warm-up
- 1 warm-up run per system (15 seconds)
- Purpose: fill caches, trigger JIT/compilation, stabilize I/O
- **Not included in results**

### Measured Runs
- 3 runs per system
- Each run: `pgbench -c <CLIENTS> -j <JOBS> -T <DURATION>`
- Default: 10 clients, 2 jobs, 60 seconds

### Result Selection
- **Median** of 3 runs (not best, not average)
- All 3 runs are preserved in `results/raw/` for inspection
- The median is the least sensitive to outliers

### Run Order
1. FalconDB warm-up → 3 measured runs
2. PostgreSQL warm-up → 3 measured runs

Both systems are initialized and running before any benchmark begins.

---

## 9. Metrics Collected

| Metric | Source | Unit |
|--------|--------|------|
| Transactions per second (TPS) | pgbench stdout | tps |
| Average latency | pgbench stdout | ms |
| Errors | pgbench stdout | count |

TPS and latency are parsed directly from pgbench's output using regex.
No post-processing or adjustment is applied.

---

## 10. Anti-Cheating Checklist

- [ ] fsync is ON for both systems
- [ ] WAL is enabled for both systems
- [ ] synchronous_commit is ON for PostgreSQL
- [ ] No data preloading beyond pgbench defaults
- [ ] pgbench source is unmodified
- [ ] Best run is NOT cherry-picked (median used)
- [ ] All raw output is preserved
- [ ] Hardware info is recorded
- [ ] Configs are committed to repository

---

## 11. Known Limitations

1. **Single-node only** — This benchmark does not test distributed features.
   FalconDB's sharding and cross-shard transactions are not exercised.

2. **pgbench workload** — The `tpcb-like` workload is simple. Real applications
   have more complex query patterns, joins, and varying data sizes.

3. **Local connections** — Both servers run on localhost. Network latency is not
   a factor. In production, network effects matter.

4. **Warm cache** — After warm-up, both systems have data in memory. Cold-start
   performance may differ.

5. **Default PostgreSQL tuning** — PostgreSQL can be tuned further for specific
   workloads. The PoC uses reasonable defaults, not maximum tuning.

6. **Single workload mode** — Only `tpcb-like` by default. `read-only` and
   `simple-update` modes are available but not run by default.

---

## 12. Reproducibility

To reproduce on another machine:

```bash
# 1. Build FalconDB
cargo build -p falcon_server --release

# 2. Ensure PostgreSQL is installed
# 3. Run the benchmark
./run_benchmark.sh

# 4. Compare results/report.md with the original
```

The benchmark will produce identical parameters. TPS and latency will vary
based on hardware, but the relative comparison should be consistent.

---

## 13. Interpretation Guidance

### What the results mean

- **Higher TPS** = more transactions per second = higher throughput
- **Lower latency** = faster individual transaction response time
- **TPS ratio > 1.0** = FalconDB is faster
- **TPS ratio < 1.0** = PostgreSQL is faster

### What the results do NOT mean

- "FalconDB is X times faster in all cases" — pgbench is one workload
- "PostgreSQL is slow" — PostgreSQL is a mature, well-optimized system
- "These numbers apply to my application" — your workload may differ

### Recommended phrasing

> "In this pgbench comparison, FalconDB achieved [X] TPS compared to
> PostgreSQL's [Y] TPS on the same hardware, with both systems running
> durable writes (WAL + fsync). This suggests FalconDB can handle OLTP
> workloads with competitive or superior throughput without compromising
> durability."

### Forbidden phrasing

- "10x faster" (unless literally measured and qualified)
- "Zero latency"
- "Perfect benchmark"
- "Universally better"
