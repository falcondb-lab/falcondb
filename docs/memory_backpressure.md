# FalconDB — Memory Backpressure System (P1-1)

## Overview

FalconDB uses a **two-layer memory control** system that prevents OOM crashes,
provides predictable degradation under load, and ensures all memory decisions
are observable, configurable, and testable.

**Guarantee**: FalconDB will never silently OOM. Under memory pressure it will
throttle, reject, or shed load — and every such action is logged and metered.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   GlobalMemoryGovernor                        │
│  node_budget_bytes │ 4-tier: None → Soft → Hard → Emergency  │
│  report_usage()    │ check(is_write) → BackpressureAction     │
└──────────────────────────────────────────────────────────────┘
        ▲ aggregated usage
        │
┌───────┴──────────────────────────────────────────────────────┐
│                 MemoryTracker (per-shard)                      │
│  7 consumers:                                                  │
│    mvcc_bytes          — MVCC version chains                   │
│    index_bytes         — secondary index BTreeMap nodes        │
│    write_buffer_bytes  — uncommitted txn write-sets            │
│    wal_buffer_bytes    — WAL / group-commit buffers            │
│    replication_buffer_bytes — replication / streaming buffers  │
│    snapshot_buffer_bytes    — snapshot transfer buffers        │
│    exec_buffer_bytes        — SQL execution intermediates     │
│                                                                │
│  PressureState: Normal | Pressure | Critical                  │
│  Counters: rejected / delayed / gc_trigger                    │
└──────────────────────────────────────────────────────────────┘
```

## Memory Consumers (7 Categories)

| Consumer | Tracked By | Grows When | Shrinks When |
|----------|-----------|------------|--------------|
| **MVCC versions** | `alloc_mvcc` / `dealloc_mvcc` | Row insert/update | GC reclaims old versions |
| **Index** | `alloc_index` / `dealloc_index` | Index insert/split | Index delete / compaction |
| **Write buffer** | `alloc_write_buffer` / `dealloc_write_buffer` | Txn writes | Txn commit / abort |
| **WAL buffer** | `alloc_wal_buffer` / `dealloc_wal_buffer` | WAL append | Group commit flush |
| **Replication buffer** | `alloc_replication_buffer` / `dealloc_replication_buffer` | WAL shipping to replica | Replica acknowledges |
| **Snapshot buffer** | `alloc_snapshot_buffer` / `dealloc_snapshot_buffer` | Snapshot/bootstrap streaming | Transfer completes |
| **SQL exec buffer** | `alloc_exec_buffer` / `dealloc_exec_buffer` | Sort/agg/hash intermediates | Query completes |

## Shard-Level Backpressure (MemoryTracker)

### Configuration

```toml
[memory]
shard_soft_limit_bytes = 2147483648   # 2 GB — enters Pressure
shard_hard_limit_bytes = 3221225472   # 3 GB — enters Critical
```

### States

| State | Condition | Behavior |
|-------|-----------|----------|
| **Normal** | `total < soft_limit` | No restrictions |
| **Pressure** | `soft_limit ≤ total < hard_limit` | Limit new write txns |
| **Critical** | `total ≥ hard_limit` | Reject ALL new txns |

### State Transitions

Every `alloc_*` / `dealloc_*` call re-evaluates pressure. Transitions are logged:

- Normal → Pressure: `WARN` with total_bytes, limits
- Pressure → Critical: `ERROR` with total_bytes, limits
- Critical → Normal: `INFO` — "backpressure relieved"

## Node-Level Backpressure (GlobalMemoryGovernor)

### Configuration

```rust
GovernorConfig {
    node_budget_bytes: 8_589_934_592,  // 8 GB
    soft_threshold: 0.70,               // 70% → Soft
    hard_threshold: 0.85,               // 85% → Hard
    emergency_threshold: 0.95,          // 95% → Emergency
    soft_delay_base_us: 1000,           // 1ms base delay
    soft_delay_scale: 10.0,             // delay scales with pressure
}
```

### 4-Tier Response

| Level | Threshold | Reads | Writes | Action |
|-------|-----------|-------|--------|--------|
| **None** | < 70% | Allow | Allow | — |
| **Soft** | 70–85% | Allow | Delay | `BackpressureAction::Delay(duration)` |
| **Hard** | 85–95% | Allow | Reject | `BackpressureAction::RejectWrite` |
| **Emergency** | ≥ 95% | Reject | Reject | `BackpressureAction::RejectAll` + urgent GC |

### Delay Calculation (Soft Zone)

```
delay_us = soft_delay_base_us * soft_delay_scale * (usage_ratio - soft_threshold) / (hard_threshold - soft_threshold)
```

At 77.5% with defaults: `1000 * 10.0 * 0.5 = 5000 µs (5 ms)`

## Forbidden Behaviors

- ❌ **OOM crash** — system MUST throttle/reject before OOM
- ❌ **Implicit infinite growth** — every allocation tracked by category
- ❌ **Silent stall** — every throttle/reject is logged + metered
- ❌ **Consistency violation** — backpressure never breaks ACID

## Metrics (Prometheus)

| Metric | Type | Description |
|--------|------|-------------|
| `falcon_memory_mvcc_bytes` | gauge | MVCC version chain bytes |
| `falcon_memory_index_bytes` | gauge | Index bytes |
| `falcon_memory_write_buffer_bytes` | gauge | Write buffer bytes |
| `falcon_memory_total_bytes` | gauge | Sum of all 7 consumers |
| `falcon_memory_soft_limit_bytes` | gauge | Soft limit |
| `falcon_memory_hard_limit_bytes` | gauge | Hard limit |
| `falcon_memory_pressure_state` | gauge | 0=normal, 1=pressure, 2=critical |
| `falcon_memory_admission_rejections` | gauge | Cumulative rejected txns |

## Tests

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `crates/falcon_storage/src/memory.rs` | 20 unit tests | Budget eval, tracker accounting, governor tiers, stats |
| `crates/falcon_storage/tests/memory_backpressure.rs` | 16 integration tests | All 7 consumers, pressure transitions, governor decisions, concurrency, recovery |

## Troubleshooting

### Memory pressure keeps rising

1. Check `falcon_memory_total_bytes` — which consumer dominates?
2. If MVCC: GC may be lagging → check `falcon_gc_sweep_duration_us`
3. If WAL buffer: group commit stalled → check `falcon_wal_fsync_max_us`
4. If exec buffer: large query running → check active connections

### Txns being rejected

1. Check `falcon_memory_pressure_state` — if 2 (Critical), memory is full
2. Check `falcon_memory_admission_rejections` — rate of rejections
3. Reduce workload or increase `shard_hard_limit_bytes`
