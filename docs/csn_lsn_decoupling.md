# CSN/LSN Decoupling — Design Document

## Core Principle

> **LSN is a physical address; CSN is a logical visibility clock. Never conflate them.**

| Property | LSN (Log Sequence Number) | CSN (Commit Sequence Number) |
|----------|--------------------------|------------------------------|
| Purpose | WAL physical position | MVCC visibility order |
| Used by | WAL flush, replication, recovery | Snapshot reads, SQL semantics |
| Encoding | `segment_id << 28 \| offset` | Simple monotonic `u64` |
| Scope | Global (single WAL) | Per-shard (Scheme A) |
| Written to | WAL record position | WAL commit record payload |

## Why Decouple?

1. **Visibility ≠ Position:** Two transactions flushed to adjacent WAL positions may commit in different logical order (e.g., one aborts, one commits later)
2. **Replication clarity:** Followers stream WAL by LSN (physical data) but determine visibility by CSN (logical commit order)
3. **Cross-shard reads:** CSN vectors enable consistent multi-shard snapshots without a global WAL
4. **Future-proof:** Global CSN (Scheme B) can be added later without changing the visibility contract

## Scheme A: Per-Shard CSN (Recommended for v1.x)

Each shard maintains an independent, monotonically increasing CSN:

```
Shard 0: CSN 1, 2, 3, 4, ...
Shard 1: CSN 1, 2, 3, ...
Shard 2: CSN 1, 2, 3, 4, 5, ...
```

Cross-shard reads use a **CSN vector**:
```
CsnSnapshot { shard0=100, shard1=50, shard2=200 }
```

### Visibility Rule

```
row.commit_csn <= snapshot.csn_for_shard(row_shard)  →  VISIBLE
```

**Prohibited:** Using LSN for visibility checks.

## Commit Path Ordering

```
1. WAL append + flush        → commit_lsn is known (durable)
2. CsnGenerator.next()       → commit_csn assigned (atomic increment)
3. CommitTable.insert(...)    → record (txn_id, csn, lsn) for recovery
4. Version.set_commit_ts()   → publish to MVCC headers (atomic store, Release)
```

### Crash Recovery

| Crash point | State | Recovery |
|-------------|-------|----------|
| Before step 1 | WAL not flushed | Transaction lost (correct: not committed) |
| After step 1, before step 2 | WAL durable, no CSN | WAL commit record exists but no CSN → re-assign on recovery |
| After step 2, before step 4 | CSN assigned, not published | WAL replay rebuilds commit table → CSN recoverable |
| After step 4 | Fully committed | Normal visibility |

Recovery scans WAL commit records to rebuild:
- `CommitTable`: `txn_id → (csn, lsn)` mapping
- `last_committed_csn` per shard: resume point for `CsnGenerator`

## Data Structures

### CommitRecord (WAL payload)
```
[txn_id: u64][commit_csn: u64][commit_lsn: u64]  = 24 bytes
```

### CsnSnapshot (per-shard vector)
```
[shard_count: u32][taken_at_ns: u64][(shard_id: u64, csn: u64)...]
```

### CommitTable (in-memory)
```
HashMap<TxnId, (Csn, StructuredLsn)>
```
- GC'd when all active snapshots have advanced past entry's CSN

## Scheme A vs Scheme B

| Aspect | Scheme A (Per-Shard) | Scheme B (Global) |
|--------|---------------------|-------------------|
| CSN scope | Per-shard | Cluster-wide |
| Cross-shard read | CSN vector | Single global CSN |
| Coordination | None (independent shards) | Global sequencer (consensus/lease) |
| Tail latency | Unaffected | +1 RTT for global CSN |
| Implementation | v1.x | Future extension |

**Decision:** v1.x uses Scheme A for low tail latency and shared-nothing compatibility.

## Integration with Replication

Follower catch-up:
1. Stream WAL segments by LSN (physical data transfer)
2. Replay commit records to rebuild local `CommitTable`
3. Local CSN state is derived from WAL, not from leader's CSN directly

This ensures followers have a consistent CSN state that matches their WAL position.

## Metrics

| Metric | Description |
|--------|-------------|
| `csn_assigned_total` | Total CSNs assigned |
| `csn_commits_total` | Total commits processed |
| `csn_lookups_total` | Commit table lookups |
| `csn_gc_entries_total` | Entries GC'd from commit table |
