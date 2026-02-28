# ADR-004: Memory-First Storage with Adaptive Garbage Collection

- **Status**: Accepted
- **Date**: 2024-07-25
- **Authors**: FalconDB Core Team

## Context

FalconDB targets low-latency OLTP where **sub-millisecond P99** is a hard requirement. Disk-based storage engines (B-tree, LSM) introduce I/O variance that is difficult to bound. However, a pure in-memory design must manage memory carefully to avoid OOM kills and ensure GC pauses do not spike tail latency.

## Decision

Adopt a **memory-first architecture** with the `MemTable` as the primary rowstore and a **three-level adaptive garbage collector**:

### Storage Architecture
- **MemTable**: In-memory `DashMap<PrimaryKey, VersionChain>` — the production OLTP engine.
- **WAL**: Durable append-only log for crash recovery (bincode, per ADR-003).
- **Checkpoint**: Periodic full snapshot (`bincode::serialize`) to reduce WAL replay length.
- **Optional engines**: ColumnStore, DiskRowstore, LSM — gated behind feature flags, not on the default OLTP path.

### Memory Accounting
- `MemoryTracker` maintains atomic counters for write-buffer and MVCC allocations.
- On commit, memory attribution migrates from write-buffer to MVCC pool.
- On abort, write-buffer allocation is released.

### Three-Level Adaptive GC

| Pressure State | GC Interval | Behavior |
|---|---|---|
| **Normal** | Configured interval (default 100ms) | Standard safepoint sweep |
| **Pressure** | 50% of normal interval | Aggressive sweep, larger batch size |
| **Critical** | 25% of normal interval | Emergency sweep, all tables |

- **Safepoint computation**: `min(min_active_txn_ts, replica_safe_ts)` — never collects versions that any active transaction or lagging replica might need.
- **Lock-free sweep**: GC traverses `VersionChain` using read locks, only acquires write lock for `truncate_prev()`. No global stop-the-world.
- **Rate limiting**: Consecutive sweeps that reclaim zero versions back off to avoid CPU waste.

### Backpressure
- When memory exceeds the high-water mark, new write transactions receive `StorageError::MemoryPressure`, signaling the client to retry after a brief delay.
- This prevents OOM while allowing the GC to catch up.

## Consequences

### Positive
- **Deterministic latency**: No disk I/O on the read/write path; P99 latency is bounded by memory access + WAL fsync.
- **No I/O amplification**: Unlike LSM, no compaction storms; unlike B-tree, no page splits on the hot path.
- **Graceful degradation**: Three-level GC adapts to workload intensity without manual tuning.

### Negative
- **Memory-bound capacity**: Dataset must fit in RAM (plus version overhead). Acceptable for the target workload (financial OLTP with moderate dataset sizes).
- **Checkpoint cost**: Full checkpoint serializes the entire dataset; mitigated by incremental delta checkpoints (planned).
- **Cold start**: Recovery requires WAL replay from last checkpoint; bounded by checkpoint frequency.

### Risks
- Replica lag can pin the GC safepoint, causing unbounded version chain growth. Mitigated by `max_replica_lag_threshold` configuration that disconnects extremely lagging replicas.
- Sudden workload spikes can trigger Critical pressure before GC catches up. Mitigated by write backpressure at the connection layer.

## Alternatives Considered

1. **Disk-based B-tree (PostgreSQL-style)**: Rejected — I/O variance violates the sub-ms P99 requirement.
2. **LSM with tiered compaction**: Available as optional engine (`--features lsm`) for larger-than-memory datasets, but not the default path.
3. **Off-heap memory pools (jemalloc arenas)**: Considered for future optimization; current `DashMap` + `Arc<Version>` allocation is sufficient for the target scale.
4. **Epoch-based reclamation (crossbeam-epoch)**: Evaluated but `VersionChain` GC needs safepoint-aware truncation (not just hazard pointers), making custom GC a better fit.
