# ADR-0003: MVCC GC Safepoint

## Status
Accepted (M1)

## Context
MVCC version chains grow unboundedly without garbage collection.
GC must be safe: never reclaim versions that active transactions or
lagging replicas might still read.

## Decision
- Safepoint = `min(min_active_ts, replica_safe_ts) - 1`.
- Versions committed at or before safepoint and not the newest in their
  chain can be reclaimed.
- Background `GcRunner` thread sweeps periodically (configurable interval).
- No global lock; per-key chain traversal (lock-free per key).
- Replication-safe: replica applied timestamp constrains GC.

## Consequences
- Memory remains stable under long-running OLTP workloads.
- Long-running transactions or lagging replicas delay GC (by design).
- Observable via `SHOW cedar.gc_stats`.
