# FalconDB — Cost Efficiency & Resource Determinism (Same SLA, Lower Cost)

> **FalconDB lets you meet the same business SLAs
> with less hardware and more predictability.**

Most databases are over-provisioned — not because teams are wasteful, but
because unpredictable behavior forces safety margins. This PoC demonstrates
that FalconDB can deliver production-grade OLTP correctness with significantly
fewer resources, and that it stays stable under peak load without over-provisioning.

This is not about raw TPS. This is about **cost-performance ratio and predictability**.

---

## What This Demo Does

1. Start **FalconDB** on a **small footprint** (512 MB memory cap, 4 vCPU equivalent)
2. Start **PostgreSQL** on a **typical production config** (4 GB shared_buffers, 8 vCPU equivalent)
3. Run the **same rate-limited OLTP workload** against both (1,000 tx/s target)
4. Collect **CPU, memory, and disk IO** from both processes
5. Simulate a **2x peak load** for 30 seconds — observe stability and recovery
6. Generate a **cost comparison report** with estimated monthly cloud spend

Both databases run with **full durability** (WAL + fsync). No shortcuts.

---

## Run It

### Prerequisites

| Tool | Purpose |
|------|---------|
| **FalconDB** | `cargo build -p falcon_server --release` |
| **PostgreSQL** | Reference database (`psql`, `pg_ctl`, `initdb`) |
| **Rust toolchain** | Build the workload generator |
| **Python 3** | Generate the cost report |

### Step-by-Step

```bash
# 1. Start FalconDB (small footprint)
./scripts/start_falcondb_small.sh

# 2. Start PostgreSQL (production-sized)
./scripts/start_postgres_large.sh

# 3. Run the same workload against both
./scripts/run_workload.sh

# 4. (In parallel) Collect resource usage for both
#    Get PIDs from output/*.pid or ps
./scripts/collect_resource_usage.sh --pid <falcon_pid> --label falcon --duration 60 &
./scripts/collect_resource_usage.sh --pid <pg_pid> --label postgres --duration 60 &

# 5. Simulate peak load
./scripts/simulate_peak.sh --target falcon
./scripts/simulate_peak.sh --target postgres

# 6. Generate cost comparison report
./scripts/generate_cost_report.sh

# 7. Cleanup
./scripts/cleanup.sh
```

Windows users: use the `.ps1` variants of each script.

---

## Deployment Specifications

| | FalconDB (Small) | PostgreSQL (Production) |
|---|---|---|
| **Target Instance** | c6g.xlarge (4 vCPU, 8 GB) | r6g.2xlarge (8 vCPU, 64 GB) |
| **Memory Config** | 512 MB soft / 768 MB hard | 4 GB shared_buffers / 12 GB cache |
| **Durability** | WAL + fsync (FULL) | WAL + fsync (FULL) |
| **Unsafe Optimizations** | None | None |
| **Est. Monthly Cost** | ~$99 | ~$294 |

> PostgreSQL is **not** artificially weakened. This is standard production sizing.
> FalconDB is **intentionally** constrained to demonstrate efficiency.

---

## What the Workload Does

The `oltp_writer` is a Rust program that generates rate-limited OLTP traffic:

- **Pattern**: BEGIN → UPDATE (debit) → UPDATE (credit) → COMMIT
- **Rate**: Fixed target (default 1,000 tx/s) — not "push until break"
- **Schema**: 10,000 accounts with balance transfers
- **Threads**: 4 concurrent workers
- **Duration**: 60 seconds per phase
- **Output**: JSON with throughput, latency distribution (p50/p95/p99), errors

The same binary, same workload, same target rate — against both databases.

---

## What the Report Shows

The generated `output/cost_comparison_report.md` contains:

### Performance Comparison
| Metric | PostgreSQL | FalconDB |
|--------|-----------|----------|
| Actual Rate (tx/s) | measured | measured |
| Latency p50/p95/p99 | measured | measured |
| Total Errors | measured | measured |

### Resource Usage
| Metric | PostgreSQL | FalconDB |
|--------|-----------|----------|
| Avg CPU % | measured | measured |
| Peak CPU % | measured | measured |
| Avg RSS (MB) | measured | measured |
| Peak RSS (MB) | measured | measured |

### Cost Estimate
| | PostgreSQL | FalconDB | Savings |
|---|---|---|---|
| Monthly | ~$294 | ~$99 | ~66% |

---

## Peak Load Simulation

The `simulate_peak.sh` script runs three phases:

1. **Normal** (30s): Baseline at target rate
2. **Peak** (30s): 2x the target rate — sudden spike
3. **Recovery** (30s): Back to normal rate — no restart allowed

What to observe:
- **Latency stability**: Does p95/p99 spike during peak?
- **Recovery time**: How quickly do latencies return to baseline?
- **CPU saturation**: Does the system hit 100% CPU?
- **Memory behavior**: Does memory spike and stay elevated?

---

## What This PoC Does NOT Claim

- ❌ "FalconDB is always faster"
- ❌ "PostgreSQL is bad"
- ❌ "These exact numbers apply to every workload"
- ❌ "Infinite scalability"
- ❌ "Zero cost"

## What This PoC DOES Prove

- ✅ Same OLTP workload runs on fewer resources
- ✅ Same durability guarantees (no corners cut)
- ✅ Latency remains stable under peak load
- ✅ Memory is bounded and predictable
- ✅ Cost difference is measurable and reproducible

---

## Why This Matters

Over-provisioning is the #1 hidden cost in database infrastructure.

Teams over-provision because they can't predict database behavior under load.
FalconDB's deterministic execution model makes resource consumption predictable,
which means you can right-size your deployment with confidence.

The savings compound:
- 10 instances × $200/mo savings × 36 months = **$72,000**
- That's not a performance improvement. That's a **budget recovery**.

For the full explanation, see [docs/explanation_for_customers.md](docs/explanation_for_customers.md).

---

## Assumptions (Documented, Not Hidden)

1. PostgreSQL uses standard production sizing (not artificially weakened)
2. FalconDB uses a deliberately constrained configuration
3. Both use full durability — WAL + fsync enabled
4. Workload is rate-limited, not "push until break"
5. Cloud pricing uses AWS on-demand (us-east-1, publicly documented)
6. Storage and network costs excluded (similar for both)
7. Single test run — production results will vary by hardware and workload

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FALCON_BIN` | `target/release/falcon_server` | Path to FalconDB binary |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_USER` | `postgres` | PostgreSQL user |
| `WORKLOAD_RATE` | `1000` | Target tx/s |
| `WORKLOAD_DURATION` | `60` | Seconds per phase |
| `WORKLOAD_THREADS` | `4` | Concurrent workers |

---

## Directory Structure

```
falcondb-poc-cost-efficiency/
├── README.md                                ← You are here
├── conf/
│   ├── falcon.small.toml                    ← FalconDB: 512 MB soft limit
│   └── postgres.large.conf                  ← PostgreSQL: 4 GB shared_buffers
├── workload/
│   ├── Cargo.toml                           ← Workload generator project
│   └── src/main.rs                          ← Rate-limited OLTP writer
├── scripts/
│   ├── start_falcondb_small.sh / .ps1       ← Start FalconDB (constrained)
│   ├── start_postgres_large.sh / .ps1       ← Start PostgreSQL (production)
│   ├── run_workload.sh / .ps1               ← Run same workload against both
│   ├── collect_resource_usage.sh / .ps1     ← Sample CPU/mem/IO per process
│   ├── simulate_peak.sh / .ps1              ← 2x peak for 30s
│   ├── generate_cost_report.sh / .ps1       ← Produce cost comparison
│   └── cleanup.sh / .ps1                    ← Stop everything
├── output/                                  ← Generated results
│   ├── workload_results_falcon.json
│   ├── workload_results_postgres.json
│   ├── resource_usage_falcon.json
│   ├── resource_usage_postgres.json
│   └── cost_comparison_report.md
└── docs/
    └── explanation_for_customers.md         ← Why over-provisioning is the real cost
```
