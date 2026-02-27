# Why Observability Matters in a Database

This document explains — in plain language — why you should care about
database observability, what FalconDB exposes by default, and how it
changes the way you operate in production.

---

## The Problem: "What Is the Database Doing Right Now?"

Every production incident starts with the same question:

> "Is it the database?"

And the answer is almost always: "We don't know yet."

Traditional databases give you two things to work with:
1. **Logs** — thousands of lines, mostly noise, hard to search in real time
2. **External monitors** — checking if the port is open, maybe response time

Neither tells you what's happening *inside* the database. You're guessing.

---

## Black Box DB vs Observable DB

### Black Box Database

```
Application → [???] → Disk
              ↑
              "Is it working?"
              "I think so?"
```

With a black box database:
- You don't know if replication is falling behind
- You don't know if the WAL is backed up
- You don't know if a rebalance is stalled
- You find out something is wrong when users complain

### Observable Database (FalconDB)

```
Application → [FalconDB] → Disk
                  │
                  ├── falcon_txn_committed_total: 482,301
                  ├── falcon_replication_lag_us: 1,240
                  ├── falcon_wal_fsync_avg_us: 87
                  ├── falcon_rebalancer_running: 1
                  ├── falcon_rebalancer_paused: 0
                  └── falcon_cluster_health_status: 0 (healthy)
```

With FalconDB:
- You know exactly how many transactions are committed
- You know how far behind the replica is (in microseconds)
- You know if a rebalance is active, paused, or finished
- You know the health of every node

---

## Why Metrics Beat Logs

| | Logs | Metrics |
|---|---|---|
| **Speed** | Search through files | Instant dashboard query |
| **Structure** | Free-text, varies | Named, typed, labeled |
| **Real-time** | Usually delayed | Scraped every 5 seconds |
| **Alerting** | Regex-based, fragile | Threshold-based, reliable |
| **Historical** | Rotate and lose | Stored in Prometheus/InfluxDB |
| **Cost** | Disk-heavy, CPU for parsing | Lightweight gauge updates |

Logs tell you *what happened*. Metrics tell you *what is happening*.

---

## What FalconDB Exposes (By Default)

FalconDB publishes all metrics on a standard Prometheus endpoint.
No configuration needed. No plugins. No agents.

### Transaction Health
| Metric | What It Means |
|--------|-------------|
| `falcon_txn_committed_total` | Successful transactions (should be climbing) |
| `falcon_txn_aborted_total` | Failed transactions (should be low) |
| `falcon_txn_active_count` | Transactions in flight right now |
| `falcon_queries_total` | Queries by type (SELECT, INSERT, UPDATE) |

**What operators see**: "Is the database doing useful work?"

### Write-Ahead Log (WAL)
| Metric | What It Means |
|--------|-------------|
| `falcon_wal_fsync_avg_us` | Average time to flush a write to disk |
| `falcon_wal_fsync_max_us` | Worst-case flush time (spikes = disk issue) |
| `falcon_wal_backlog_bytes` | Unflushed data (growing = problem) |
| `falcon_wal_group_commit_avg_size` | How many writes are batched together |

**What operators see**: "Is the storage keeping up?"

### Replication
| Metric | What It Means |
|--------|-------------|
| `falcon_replication_lag_us` | How far behind the replica is |
| `falcon_repl_segments_streamed_total` | WAL segments sent to replicas |
| `falcon_repl_checksum_failures_total` | Data corruption in transit (should be 0) |
| `falcon_replication_leader_changes` | How many failovers have happened |

**What operators see**: "Is the backup up to date?"

### Rebalancer
| Metric | What It Means |
|--------|-------------|
| `falcon_rebalancer_running` | 1 = actively moving data between nodes |
| `falcon_rebalancer_paused` | 1 = paused by operator (intentional) |
| `falcon_rebalancer_imbalance_ratio` | How unevenly data is distributed |
| `falcon_rebalancer_rows_migrated_total` | Total rows moved |
| `falcon_rebalancer_move_rate_rows_per_sec` | Current migration speed |

**What operators see**: "Is data distribution healthy? Can I pause this safely?"

### Cluster Health
| Metric | What It Means |
|--------|-------------|
| `falcon_cluster_health_status` | 0 = healthy, 1 = degraded, 2 = critical |
| `falcon_cluster_alive_nodes` | Nodes currently responding |
| `falcon_cluster_total_nodes` | Expected cluster size |
| `falcon_active_connections` | Client connections right now |

**What operators see**: "Is everything OK?"

---

## How Operators Make Decisions

### Scenario 1: "Should I deploy during this maintenance window?"

Check the dashboard:
- `falcon_rebalancer_running` = 0 → No rebalance in progress
- `falcon_replication_lag_us` < 1000 → Replica is caught up
- `falcon_cluster_health_status` = 0 → All healthy

**Decision**: Safe to deploy.

### Scenario 2: "Users are reporting slowness"

Check the dashboard:
- `falcon_wal_fsync_max_us` spiking → Disk latency issue
- `falcon_txn_active_count` climbing → Transactions piling up
- `falcon_memory_pressure_state` = 2 → Memory pressure

**Decision**: Investigate disk I/O, possibly increase memory limits.

### Scenario 3: "We just added a node"

Check the dashboard:
- `falcon_rebalancer_running` = 1 → Data is being redistributed
- `falcon_rebalancer_move_rate_rows_per_sec` = 5,000 → Moving steadily
- `falcon_rebalancer_imbalance_ratio` dropping → Getting more balanced

**Decision**: Working as expected. No action needed.

### Scenario 4: "A node went down"

Check the dashboard:
- `falcon_cluster_alive_nodes` dropped by 1
- `falcon_replication_leader_changes` incremented
- `falcon_cluster_health_status` = 1 (degraded)

**Decision**: Failover completed. Investigate the downed node. Cluster is
still serving traffic.

---

## What This Demo Proves

This demo doesn't prove a number. It proves a capability:

> **When you look at the FalconDB dashboard, you can answer
> "Is the system healthy?" in under 5 seconds.**

That's not possible with a black box database.

### What operators can do in this demo:

1. **See** cluster health, transaction rate, replication lag — all at once
2. **Trigger** a rebalance and watch the effect in real time
3. **Pause** the rebalance and see it reflected immediately
4. **Resume** the rebalance and confirm it picks up where it left off
5. **Kill a node** and watch the failover happen on the dashboard

Every action has an immediate, visible metric response.

---

## Grafana Dashboard Layout

The pre-built dashboard ("FalconDB Cluster Overview") is organized for
quick scanning:

```
┌─────────────────────────────────────────────────────────────┐
│ Row 1: STATUS AT A GLANCE                                    │
│ [Health] [Nodes] [Connections] [Txns] [Rebalancer] [Leader]  │
├─────────────────────────────────────────────────────────────┤
│ Row 2: TRANSACTION THROUGHPUT                                │
│ [Commits/Aborts over time]    [Query rate by type]           │
├─────────────────────────────────────────────────────────────┤
│ Row 3: WAL (WRITE PATH)                                      │
│ [Fsync latency]    [Backlog]    [Group commit size]          │
├─────────────────────────────────────────────────────────────┤
│ Row 4: REPLICATION                                           │
│ [Lag]    [Segments streamed]    [Errors]                     │
├─────────────────────────────────────────────────────────────┤
│ Row 5: REBALANCER                                            │
│ [Rows migrated]    [Imbalance ratio]    [Move rate]          │
├─────────────────────────────────────────────────────────────┤
│ Row 6: MEMORY                                                │
│ [Usage by component]    [Pressure + rejections]              │
├─────────────────────────────────────────────────────────────┤
│ Row 7: FAILOVER & MIGRATIONS                                 │
│ [Promotions + leader changes]    [Shard migration status]    │
└─────────────────────────────────────────────────────────────┘
```

Every panel answers a specific question. No PromQL knowledge needed.

---

## How FalconDB Avoids Surprises

Traditional databases surprise operators with:
- Silent replication lag (no metric, found out too late)
- Stalled rebalances (running but not progressing)
- Hidden memory pressure (OOM kill with no warning)
- Failovers with no visible indicator

FalconDB prevents surprises because:
- **Every internal state is a metric** — not buried in a log
- **State changes are immediate** — paused/running visible within 5 seconds
- **Memory pressure is quantified** — not just "it crashed"
- **Failovers are counted** — `leader_changes` is always visible

---

## The Bottom Line

This demo answers one question:

> **"Can I operate this database with confidence?"**

FalconDB's answer:

> **Yes. Open the dashboard. Everything you need to know is right there.
> And if you need to act, the controls are immediate and visible.**

This is not about performance. This is about **operational confidence**.
