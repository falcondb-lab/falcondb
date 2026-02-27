# FalconDB — Operability & Observability (Production Visibility Demo)

> **This demo proves that FalconDB is not a black box.
> You can see what it's doing, and you can control it.**

When something happens in production — a spike in traffic, a node going down,
a rebalance taking too long — you need to know immediately. Not from logs.
Not from guessing. From real-time metrics that tell you exactly what the
database is doing right now.

This PoC demonstrates:
- **Observability**: live metrics for WAL, replication, rebalancing, failover
- **Operability**: pause/resume rebalance, trigger operations, see the effect
- **Explainability**: a Grafana dashboard that anyone can read

---

## What Happens in This Demo

1. Start a **2-node FalconDB cluster** (primary + replica)
2. Start **Prometheus + Grafana** (pre-configured, auto-provisioned dashboard)
3. Generate **normal OLTP activity** (light writes, updates, reads)
4. **Watch the dashboard**: WAL throughput, replication lag, transaction rate
5. **Trigger a rebalance** → watch metrics react
6. **Pause the rebalance** → watch metrics freeze
7. **Resume the rebalance** → watch metrics recover
8. *(Optional)* **Kill the primary** → watch failover happen live
9. End with: *"I know exactly what the database is doing"*

---

## Run It

### What You Need

| Tool | What It Does |
|------|-------------|
| **FalconDB** | `cargo build -p falcon_server --release` |
| **psql** | PostgreSQL client |
| **Docker** | Runs Prometheus + Grafana |

### Step 1: Start the cluster

```bash
# Linux / macOS
./scripts/start_cluster.sh

# Windows
.\scripts\start_cluster.ps1
```

### Step 2: Start the monitoring stack

```bash
./scripts/start_monitoring.sh
```

Open **Grafana**: http://localhost:3000 (login: admin / admin)

The **"FalconDB Cluster Overview"** dashboard is pre-loaded and auto-refreshing.

### Step 3: Generate activity

```bash
# Runs until Ctrl+C
./scripts/generate_activity.sh
```

Watch the dashboard: transaction counters increase, WAL metrics move,
replication lag stays low.

### Step 4: Trigger operational events

Open a **second terminal** and try these, one at a time:

```bash
# Trigger a rebalance — watch rebalancer metrics spike
./scripts/trigger_rebalance.sh

# Pause the rebalance — watch rebalancer_paused go to 1
./scripts/pause_rebalance.sh

# Resume the rebalance — watch it resume
./scripts/resume_rebalance.sh
```

Each action is visible on the dashboard **within 5 seconds** (one scrape interval).

### Step 5 (Optional): Simulate failover

```bash
./scripts/simulate_failover.sh
```

Watch the dashboard:
- Leader changes counter increments
- Replication metrics reset
- Cluster health may briefly show "Degraded"

### Step 6: Cleanup

```bash
./scripts/cleanup.sh        # Stop everything
./scripts/cleanup.sh --all  # Also remove data + Docker volumes
```

---

## What the Dashboard Shows

The pre-built Grafana dashboard has these sections:

### Row 1: Cluster Status (at a glance)
| Panel | What It Answers |
|-------|----------------|
| **Cluster Health** | Is the system healthy right now? (green/yellow/red) |
| **Nodes Alive** | How many nodes are up? |
| **Active Connections** | How many clients are connected? |
| **Active Transactions** | How many transactions are in flight? |
| **Rebalancer Status** | Is data moving? Is it paused? |
| **Leader Changes** | Has a failover happened? |

### Row 2: Transaction Throughput
- Commits and aborts over time
- Query rate by type (SELECT, INSERT, UPDATE)

### Row 3: WAL (Write-Ahead Log)
- Fsync latency (average and maximum)
- WAL backlog bytes
- Group commit batch size

### Row 4: Replication
- Replication lag in milliseconds
- Segments streamed and bytes transferred
- Checksum failures and error rollbacks

### Row 5: Rebalancer
- Rows migrated over time
- Imbalance ratio (how uneven the data is)
- Move rate (rows per second)

### Row 6: Memory
- Total, MVCC, index, and write buffer memory
- Memory pressure state and admission rejections

### Row 7: Failover & Migrations
- Promotion count and leader changes
- Active, completed, and failed shard migrations

---

## Metrics Reference

Every metric on the dashboard comes directly from FalconDB's Prometheus
endpoint. Here's what they mean in plain language:

| Metric | What It Means |
|--------|-------------|
| `falcon_cluster_health_status` | 0 = healthy, 1 = degraded, 2 = critical |
| `falcon_txn_committed_total` | Total transactions successfully committed |
| `falcon_txn_aborted_total` | Total transactions that failed |
| `falcon_wal_fsync_avg_us` | Average time to flush a write to disk (microseconds) |
| `falcon_wal_backlog_bytes` | How much WAL data is waiting to be processed |
| `falcon_replication_lag_us` | How far behind the replica is (microseconds) |
| `falcon_rebalancer_running` | 1 = rebalancer is actively moving data |
| `falcon_rebalancer_paused` | 1 = rebalancer is paused by an operator |
| `falcon_rebalancer_rows_migrated_total` | Total rows moved during rebalancing |
| `falcon_rebalancer_imbalance_ratio` | How unevenly data is distributed (0 = perfect) |
| `falcon_replication_leader_changes` | Number of times a new leader was elected |
| `falcon_replication_promote_count` | Number of replica promotions |

All metrics are:
- **Real** — they reflect actual internal state, not estimates
- **Cheap** — collected with near-zero overhead
- **Prometheus-native** — standard scrape endpoint, standard format

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `FALCON_BIN` | `target/release/falcon_server` | Path to FalconDB binary |
| `ADMIN_PORT` | `8080` | Admin/metrics port for primary |
| `ACTIVITY_INTERVAL_MS` | `200` | Milliseconds between generated events |

---

## Without Docker

If you can't run Docker, you can still run the demo:

1. **Start the FalconDB cluster** normally (scripts work without Docker)
2. **Point your own Prometheus** at `http://localhost:8080/metrics` and `http://localhost:8081/metrics`
3. **Import the dashboard** from `dashboards/falcondb_overview.json` into your Grafana
4. Or just **curl the metrics** directly:
   ```bash
   curl http://localhost:8080/metrics | grep falcon_rebalancer
   ```

---

## Anti-Cheating: How We Keep This Honest

- Metrics reflect **real internal state**, not synthetic numbers
- No metric smoothing or hiding of unhealthy states
- No log parsing — all metrics are structured counters/gauges/histograms
- Rebalancer pause/resume is a real operational control, not a display trick
- Dashboard auto-refreshes from live Prometheus data

---

## For More Detail

See [docs/explanation_for_customers.md](docs/explanation_for_customers.md) for:
- Why observability matters more than logs
- "Black box DB" vs "Observable DB"
- How operators make decisions using this visibility
- Metric-to-real-world meaning mapping

---

## Directory Structure

```
falcondb-poc-observability/
├── README.md                               ← You are here
├── configs/
│   ├── node1.toml                          ← Primary config (port 5433, metrics 8080)
│   └── node2.toml                          ← Replica config (port 5434, metrics 8081)
├── docker/
│   ├── docker-compose.yml                  ← Prometheus + Grafana
│   ├── grafana-datasources.yml             ← Auto-provision Prometheus
│   └── grafana-dashboards.yml              ← Auto-provision dashboard
├── prom/
│   └── prometheus.yml                      ← Scrape config (5s interval)
├── dashboards/
│   └── falcondb_overview.json              ← Pre-built Grafana dashboard
├── scripts/
│   ├── start_cluster.sh / .ps1             ← Start 2-node cluster
│   ├── start_monitoring.sh / .ps1          ← Start Prometheus + Grafana
│   ├── generate_activity.sh / .ps1         ← Light OLTP workload
│   ├── trigger_rebalance.sh / .ps1         ← Force rebalance cycle
│   ├── pause_rebalance.sh / .ps1           ← Pause rebalancer
│   ├── resume_rebalance.sh / .ps1          ← Resume rebalancer
│   ├── simulate_failover.sh / .ps1         ← Kill primary + promote
│   └── cleanup.sh / .ps1                   ← Stop everything
├── output/                                 ← Logs, metrics snapshots, timeline
└── docs/
    └── explanation_for_customers.md
```
