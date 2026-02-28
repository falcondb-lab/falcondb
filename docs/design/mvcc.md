# MVCC (Multi-Version Concurrency Control) Design

## Overview

FalconDB uses MVCC with Optimistic Concurrency Control (OCC) to provide
non-blocking reads and serializable write conflict detection. Every row mutation
creates a new version in a per-key version chain, and readers see a consistent
snapshot without acquiring locks.

See [ADR-002](../adr/ADR-002-mvcc-occ-snapshot-isolation.md) for the decision
rationale.

## Goals

1. **Non-blocking reads**: readers never block writers; writers never block readers.
2. **Snapshot Isolation**: each transaction sees a consistent point-in-time snapshot.
3. **OCC validation**: write conflicts are detected at commit time, not during execution.
4. **Efficient GC**: old versions are reclaimed without stop-the-world pauses.

## Version Chain Structure

Each primary key maps to a `VersionChain` — a singly-linked list of versions
ordered from newest to oldest:

```
PK "user:42"
  │
  ▼
┌─────────────────────────┐
│ Version {                │
│   txn_id:    TxnId(5)   │
│   commit_ts: Ts(300)    │ ◄── HEAD (newest)
│   row:       Some(...)  │
│   next:      ──────────────┐
│ }                        │  │
└─────────────────────────┘  │
                              ▼
┌─────────────────────────┐
│ Version {                │
│   txn_id:    TxnId(3)   │
│   commit_ts: Ts(200)    │
│   row:       Some(...)  │
│   next:      ──────────────┐
│ }                        │  │
└─────────────────────────┘  │
                              ▼
                            NULL (chain end)
```

### Key Properties

- **Head insert**: new versions are always prepended (O(1)).
- **Commit timestamp**: `0` = uncommitted; `u64::MAX` = aborted; otherwise the
  commit timestamp.
- **Delete marker**: `row = None` represents a tombstone (logical delete).
- **Storage**: `DashMap<PkBytes, VersionChain>` — concurrent hash map, lock-free reads.

## Visibility Rules

### Read Committed (default)

A version is visible to timestamp `T` if:
1. `commit_ts > 0` (committed, not aborted)
2. `commit_ts <= T` (committed before the reader's snapshot)
3. It is the **newest** such version in the chain.

```rust
fn read_committed(&self, read_ts: Timestamp) -> Option<&OwnedRow> {
    // Walk chain from head; return first version with commit_ts in (0, read_ts]
}
```

### Read-Your-Own-Writes

A transaction can always see its own uncommitted version:

```rust
fn read_for_txn(&self, txn_id: TxnId, read_ts: Timestamp) -> Option<&OwnedRow> {
    // 1. Check if head is our own uncommitted write → visible
    // 2. Otherwise fall back to read_committed(read_ts)
}
```

## Write Path

### Insert

```
1. Executor calls storage.insert(table, pk, row, txn_id)
2. VersionChain::prepend(txn_id, Some(row))
   └── Atomically set new head version (CAS on head pointer)
3. WAL append: WalRecord::Insert { table_id, pk, row }
```

### Update

```
1. Executor calls storage.update(table, pk, new_row, txn_id)
2. VersionChain::prepend(txn_id, Some(new_row))
3. WAL append: WalRecord::Update { table_id, pk, old_row, new_row }
```

### Delete

```
1. Executor calls storage.delete(table, pk, txn_id)
2. VersionChain::prepend(txn_id, None)  // tombstone
3. WAL append: WalRecord::Delete { table_id, pk }
```

## OCC Commit Validation

At commit time, Snapshot Isolation requires checking that no other transaction
committed a conflicting write to any key in our write-set since our snapshot:

```
For each key K in write_set:
  Let V = current head of VersionChain(K)
  If V.commit_ts > my_start_ts AND V.txn_id != my_txn_id:
    → WriteConflict: abort
```

If validation passes:
1. Set `commit_ts` on all our versions.
2. WAL append `Commit { txn_id, commit_ts }`.
3. Fsync WAL (group commit).
4. Versions become visible to subsequent readers.

### Abort

On abort (explicit or conflict):
1. Set `commit_ts = u64::MAX` on all our versions (marks them as aborted).
2. WAL append `Abort { txn_id }`.
3. Aborted versions are invisible to all readers and eligible for GC.

## Fast Paths

### Zero-Copy Row Access

`with_visible_data` provides closure-based access to the visible row without
cloning. Used in the scan/filter hot path:

```rust
chain.with_visible_data(read_ts, |row| {
    // row: &OwnedRow — zero-copy reference, no allocation
    evaluate_filter(row)
})
```

### Streaming Iteration

`for_each_visible` iterates all visible rows across the entire DashMap without
collecting into a Vec first. Used by `compute_simple_aggs` for single-pass
aggregation.

### Arc-Clone Elimination

The head version's row is accessed via `Arc<OwnedRow>`. Visibility checks on the
head version avoid cloning the Arc when possible, reducing atomic reference
count contention.

## Garbage Collection

See [ADR-004](../adr/ADR-004-memory-first-adaptive-gc.md) for the GC strategy.

### Safepoint Calculation

```
gc_safepoint = min(
    min_active_transaction_start_ts,
    min_replica_applied_ts
) - 1
```

### Per-Chain Pruning

For each version chain, GC removes versions where:
- `commit_ts < gc_safepoint` AND there exists a newer committed version.
- `commit_ts == u64::MAX` (aborted — always reclaimable).

Pruning is per-chain via DashMap iteration — no global lock, no stop-the-world.

### GC Runner

`GcRunner` runs as a background tokio task:
- **Interval**: configurable (`gc.interval_secs`, default 10).
- **Batch size**: configurable (`gc.batch_size`, default 10000).
- **Observability**: `SHOW falcon.gc_stats` exposes safepoint, reclaimed
  versions/bytes, chain length distribution.

## Isolation Levels

| Level | Behavior |
|-------|----------|
| Read Committed | Each statement sees the latest committed state |
| Snapshot Isolation | Entire transaction sees a consistent snapshot at BEGIN time; OCC validation at COMMIT |

Configured per-session via `SET TRANSACTION ISOLATION LEVEL`.

## Concurrency Control Summary

| Operation | Mechanism | Blocking? |
|-----------|-----------|-----------|
| Read | Version chain traversal | No (lock-free) |
| Write | Head prepend (CAS) | No (wait-free per key) |
| Commit | OCC read-set validation | No (optimistic, abort on conflict) |
| GC | Background per-chain prune | No (incremental, no STW) |

## Invariants

1. Version chains are ordered by creation time (newest first).
2. At most one uncommitted version per transaction per key.
3. `commit_ts` is set exactly once (0 → real_ts or 0 → MAX).
4. No version with `commit_ts == 0` is visible to other transactions.
5. GC never removes the newest committed version for any key.
6. GC respects replica applied timestamps (replication-safe).
