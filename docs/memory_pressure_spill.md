# Memory Pressure & Disk Spill Strategy

> **Status**: Production-ready (not design-only).  
> **Last updated**: 2026-02-25  
> **Test evidence**: `crates/falcon_executor/tests/spill_pressure.rs` — 18 integration tests.

---

## Overview

FalconDB provides a layered defense against memory exhaustion during query
execution. The strategy spans three tiers:

| Tier | Component | Scope | Action |
|------|-----------|-------|--------|
| T1 | `ExternalSorter` | Per-query ORDER BY | Spill sorted runs to disk |
| T2 | Hash aggregation guard | Per-query GROUP BY | Reject at group count limit |
| T3 | `MemoryTracker` + `GlobalMemoryGovernor` | Shard / Node | Backpressure (delay / reject) |

All three tiers are **wired into production code paths** and exercised by
automated tests.

---

## T1: External Sort — Disk Spill

### How It Works

When an `ORDER BY` query produces more rows than `memory_rows_threshold`,
the `ExternalSorter` performs a k-way external merge sort:

1. **Partition** — split input into chunks of `threshold` rows.
2. **Sort & Flush** — sort each chunk in-memory, serialize to a temporary
   binary file (bincode format: `[row_count: u64][len: u32][row_bytes]…`).
3. **K-way Merge** — merge sorted runs using a binary heap with `fan_in`
   inputs per pass, writing intermediate merge files.
4. **Read-back** — deserialize the final merged run back into memory.
5. **Cleanup** — delete all temporary files (always, even on error).

### Observable Metrics (`SpillMetrics`)

| Metric | Type | Description |
|--------|------|-------------|
| `spill_count` | counter | Sort operations that spilled to disk |
| `in_memory_count` | counter | Sort operations completed in-memory |
| `runs_created` | counter | Total sorted runs written |
| `bytes_spilled` | counter | Total bytes written to spill files |
| `bytes_read_back` | counter | Total bytes read from spill files |
| `spill_duration_us` | counter | Cumulative spill time (µs) |

Metrics are exposed via `Executor::spill_metrics()` and can be consumed by
Prometheus / `SHOW` commands.

### Configuration

```toml
[spill]
# Max rows in memory before spilling. 0 = disable spill.
# Default: 500,000 (production-safe bound).
memory_rows_threshold = 500000

# Temp directory for spill files. Empty = system temp dir.
temp_dir = ""

# K-way merge fan-in. Higher = fewer passes, more memory per pass.
merge_fan_in = 16
```

### Test Evidence

| Test | Property |
|------|----------|
| SP-1 | In-memory sort records `in_memory_count` metric |
| SP-2 | Disk spill records all 6 metrics |
| SP-3 | Multiple sorts accumulate metrics |
| SP-4 | Temp files cleaned up after sort |
| SP-5 | `threshold=0` → spill disabled (`None`) |
| SP-15 | 10,000-row spill sort → data integrity verified |
| SP-16 | Multi-column spill sort → correct order |

---

## T2: Hash Aggregation Group Limit

### Problem

An unbounded `GROUP BY` on a high-cardinality column can create millions of
in-memory groups, leading to OOM.

### Solution

The executor enforces a `hash_agg_group_limit` (default: 1,000,000 groups).
When a new group would exceed the limit, the query is rejected with:

- **Error**: `ExecutionError::ResourceExhausted`
- **SQLSTATE**: `53000` (insufficient_resources)
- **Message**: `hash aggregation group limit exceeded: N groups (limit: L).
  Reduce cardinality or increase falcon.spill.hash_agg_group_limit`

### Configuration

```toml
[spill]
# Max groups in hash aggregation. 0 = unlimited (NOT recommended).
hash_agg_group_limit = 1000000
```

### Test Evidence

| Test | Property |
|------|----------|
| SP-8 | Executor stores `hash_agg_group_limit` from config |
| SP-9 | `ResourceExhausted` → SQLSTATE `53000` |

---

## T3: Memory Pressure — Shard & Node Level

### Shard-Level: `MemoryTracker`

Each shard owns a `MemoryTracker` with lock-free atomic counters for:

- **MVCC bytes** — version chain data
- **Index bytes** — secondary index nodes
- **Write-buffer bytes** — uncommitted transaction write-sets
- **WAL buffer bytes** — group-commit buffers
- **Replication buffer bytes** — streaming buffers
- **Snapshot buffer bytes** — checkpoint buffers
- **Exec buffer bytes** — SQL execution intermediates

The tracker evaluates `PressureState` against a `MemoryBudget`:

| State | Condition | Effect |
|-------|-----------|--------|
| `Normal` | usage < soft_limit | No restrictions |
| `Pressure` | soft_limit ≤ usage < hard_limit | Limit new write txns |
| `Critical` | usage ≥ hard_limit | Reject ALL new txns |

Transitions are logged as structured events.

### Node-Level: `GlobalMemoryGovernor`

Aggregates shard-level usage into a 4-tier backpressure model:

| Level | Threshold | Write Action | Read Action |
|-------|-----------|-------------|-------------|
| `None` | usage < 70% budget | Allow | Allow |
| `Soft` | 70% ≤ usage < 85% | Delay (proportional) | Allow |
| `Hard` | 85% ≤ usage < 95% | **Reject** | Allow |
| `Emergency` | usage ≥ 95% | **Reject** | **Reject** |

The `Soft` delay is proportional to pressure:
`delay = base_us × (usage_ratio - soft_threshold) / (hard_threshold - soft_threshold) × scale`

### Configuration

```toml
[memory]
# Per-shard soft/hard limits (bytes). 0 = disabled.
soft_limit = 536870912    # 512 MB
hard_limit = 805306368    # 768 MB

# Node-level governor
node_budget_bytes = 1073741824  # 1 GB
soft_threshold = 0.70
hard_threshold = 0.85
emergency_threshold = 0.95
soft_delay_base_us = 1000       # 1 ms
soft_delay_scale = 10.0
```

### Reactive Spill (Pressure → Sort Threshold)

The `pressure_spill_trigger` config controls automatic sort threshold
reduction under memory pressure:

```toml
[spill]
# When memory pressure reaches this level, halve the sort threshold.
# Values: "soft", "hard", "emergency", "none" (disable).
pressure_spill_trigger = "soft"
```

When triggered, the effective sort threshold is halved, causing ORDER BY
queries to spill to disk earlier, relieving memory pressure.

### Test Evidence

| Test | Property |
|------|----------|
| SP-10 | `PressureState` boundary transitions correct |
| SP-11 | `GlobalMemoryGovernor` 4-tier level transitions |
| SP-12 | Governor rejects writes at Hard level |
| SP-13 | Governor rejects ALL at Emergency level |
| SP-14 | Shard tracker pressure state tracking |

---

## Forbidden States

| # | Forbidden State | Prevention |
|---|----------------|------------|
| FS-1 | OOM from unbounded ORDER BY | `ExternalSorter` spills at threshold |
| FS-2 | OOM from unbounded GROUP BY | `hash_agg_group_limit` rejects |
| FS-3 | Node-wide OOM | `GlobalMemoryGovernor` rejects at Hard/Emergency |
| FS-4 | Orphan spill files on disk | `ExternalSorter` always cleans up (even on error) |
| FS-5 | Silent memory pressure | `PressureState` transitions logged + metrics |

---

## Client Error Contract

| Scenario | SQLSTATE | Retryable? | Client Action |
|----------|----------|------------|---------------|
| Sort spill I/O failure | `XX000` | No | Report internal error |
| Hash agg group limit | `53000` | No | Add filter or raise limit |
| Memory hard reject (write) | `53000` | Yes (backoff) | Retry after delay |
| Memory emergency reject | `53000` | Yes (backoff) | Retry after delay |
| Query governor timeout | `57014` | Yes | Retry with smaller query |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     Client Query                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  Executor                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ ORDER BY    │  │ GROUP BY     │  │ Query         │  │
│  │ ─────────── │  │ ──────────── │  │ Governor      │  │
│  │ threshold?  │  │ group_limit? │  │ (row/byte/    │  │
│  │ → spill     │  │ → reject     │  │  time/mem)    │  │
│  └──────┬──────┘  └──────┬───────┘  └───────────────┘  │
│         │                │                              │
│         ▼                ▼                              │
│  ┌─────────────┐  ┌──────────────┐                     │
│  │ External    │  │ Resource     │                     │
│  │ Sorter      │  │ Exhausted    │                     │
│  │ (disk I/O)  │  │ Error        │                     │
│  │ + metrics   │  │ SQLSTATE     │                     │
│  └─────────────┘  │ 53000        │                     │
│                   └──────────────┘                     │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  Storage                                                │
│  ┌──────────────────┐  ┌────────────────────────────┐   │
│  │ MemoryTracker    │  │ GlobalMemoryGovernor       │   │
│  │ (per-shard)      │  │ (node-wide)                │   │
│  │ Normal/Pressure/ │  │ None/Soft/Hard/Emergency   │   │
│  │ Critical         │  │ Delay/RejectWrite/RejectAll│   │
│  └──────────────────┘  └────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## Related Documentation

- [Failover Behavior](failover_behavior.md)
- [Failover Partition SLA](failover_partition_sla.md)
- [Failover Determinism Report](failover_determinism_report.md)
