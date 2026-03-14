# WAL (Write-Ahead Log) Design

## Overview

FalconDB uses a custom Write-Ahead Log as the sole durability mechanism for its
in-memory storage engine. Every mutation is WAL-durable before the corresponding
MVCC version becomes visible, enforcing the
[Deterministic Commit Guarantee](../adr/ADR-001-deterministic-commit-guarantee.md).

## Goals

1. **Correctness**: WAL-first ordering; no visible state without a durable WAL record.
2. **Performance**: Group commit to amortize fsync; async I/O on Windows (IOCP).
3. **Recoverability**: Full crash recovery from WAL replay; checkpoint acceleration.
4. **Replication**: WAL records are the unit of replication (WAL-shipping over gRPC).

## Segment Layout

```
falcon_data/
├── falcon_000000.wal   # segment 0 (64 MB default)
├── falcon_000001.wal   # segment 1
├── ...
└── checkpoint.bin      # latest checkpoint snapshot
```

- **Segment size**: 64 MB (configurable via `wal.segment_size`).
- **Segment rotation**: when a segment exceeds the size threshold, a new segment
  is created and the WAL offset advances.
- **Purge policy**: segments older than the oldest active transaction's LSN are
  eligible for purge, subject to replication lag constraints.

## Record Format

Each WAL record is serialized with `bincode` (see
[ADR-003](../adr/ADR-003-wal-bincode-serialization.md)) and framed as:

```
┌──────────┬──────────┬──────────┬───────────────┬──────────┐
│ len (u32)│ crc (u32)│ lsn (u64)│ payload (var) │ pad      │
└──────────┴──────────┴──────────┴───────────────┴──────────┘
```

- **len**: total record length including header (little-endian).
- **crc**: CRC32 of the payload bytes (crc32fast).
- **lsn**: monotonically increasing Log Sequence Number.
- **payload**: bincode-serialized `WalRecord` enum variant.
- **pad**: zero-fill to 8-byte alignment (optional, for mmap friendliness).

### WalRecord Variants

| Variant | Description |
|---------|-------------|
| `Insert { table_id, pk, row }` | New row inserted |
| `Update { table_id, pk, old_row, new_row }` | Row updated (old image for undo) |
| `Delete { table_id, pk }` | Row deleted |
| `Commit { txn_id, commit_ts }` | Transaction committed |
| `Abort { txn_id }` | Transaction aborted |
| `CreateTable { table_id, schema }` | DDL: table created |
| `DropTable { table_id }` | DDL: table dropped |
| `Checkpoint { lsn }` | Marks a consistent checkpoint boundary |

## Write Path

```
Executor commit
  │
  ▼
WalWriter::append(record)
  │
  ├── Serialize with bincode
  ├── Compute CRC32
  ├── Append to in-memory buffer
  │
  ▼
Group Commit (batched fsync)
  │
  ├── Collect pending records from N concurrent writers
  ├── Single fsync() / FlushFileBuffers() call
  ├── Wake all waiters with durable LSN
  │
  ▼
MVCC version made visible
  │
  ▼
Client ACK
```

### Sharded Insertion

WAL append uses 8 shards (`WalShard`) to reduce contention:

```
Thread A → pick_shard() → shard[3].lock() → append to shard buf
Thread B → pick_shard() → shard[7].lock() → append to shard buf
...
flush() → lock all 8 shards → collect buffers → release shard locks → write to file
```

- **Thread-local affinity**: `pick_shard()` uses round-robin initial assignment
  cached per-thread. Same thread always uses same shard, preserving record
  ordering within a thread (inserts must precede their commit record).
- **Flush atomicity**: `flush()` and `flush_split()` lock all shards in a scope
  block, collect buffers, release shard locks, then take inner lock for file I/O.

### Group Commit

Multiple concurrent transactions append to sharded buffers. A single leader
thread performs the fsync and wakes all followers. This amortizes the cost of
fsync across N transactions.

- **Leader election**: first thread to enter the flush window becomes the leader.
- **Spin-wait fast path**: 500 iterations of `spin_loop()` before falling back
  to Condvar. FUA writes take ~21-30µs; most completions land within the spin window.
- **Double-buffer flush**: `flush_split()` swaps buffers under inner lock (instant),
  performs FUA write outside lock (~21µs). Concurrent appends proceed into the new buffer.

### Platform-Specific I/O

| Platform | Implementation | Notes |
|----------|---------------|-------|
| Linux/macOS | `std::fs::File` + `fdatasync` | Direct syscall; io_uring planned |
| Windows | `FILE_FLAG_WRITE_THROUGH` (FUA) | Per-write durability without `FlushFileBuffers`; avoids NVMe FLUSH command degradation |

## Recovery

On startup, the recovery sequence is:

1. **Find latest checkpoint** → load `checkpoint.bin` into memory.
2. **Locate WAL tail** → find the segment and offset after the checkpoint LSN.
3. **Replay forward** → for each valid WAL record after the checkpoint:
   - Verify CRC32.
   - Apply to in-memory storage engine.
   - Track committed vs uncommitted transactions.
4. **Abort uncommitted** → any transaction without a `Commit` record is rolled back.
5. **Set WAL write offset** → resume writing after the last valid record.

### Corruption Handling

- If a CRC mismatch is detected, replay stops at that point.
- Partial records at the tail (torn writes) are truncated.
- The database logs a warning and continues with the last known-good state.

## Replication Integration

WAL records are the replication unit for WAL-shipping mode:

```
Primary: WalWriter → WalChunk { start_lsn, end_lsn, crc, records[] }
              │
              ▼ (gRPC SubscribeWal stream)
Replica: apply_wal_record_to_engine() → MemTable + Indexes
```

- **WalChunk**: batches multiple `WalRecord`s with a covering CRC.
- **Transport**: `ReplicationTransport` trait with `InProcessTransport` (tests)
  and `GrpcTransport` (production via tonic).
- **Commit scheme**: Scheme A (async, default) or Scheme B (sync quorum ack).

## Checkpoint

Checkpoints accelerate recovery by snapshotting the full in-memory state:

- **Trigger**: configurable interval (`wal.checkpoint_interval_secs`, default 300).
- **Content**: serialized table schemas + all committed row versions.
- **Atomicity**: written to a temp file, then atomically renamed.
- **WAL purge**: segments before the checkpoint LSN become purgeable.

## Configuration

```toml
[wal]
data_dir = "falcon_data"
segment_size = "64MB"
sync_mode = "fsync"            # fsync | fdatasync | none
group_commit_timeout_us = 100
group_commit_max_batch = 64
checkpoint_interval_secs = 300
```

## Invariants

1. No MVCC version is visible before its WAL record is durable.
2. LSN is monotonically increasing; no gaps.
3. CRC protects against silent corruption.
4. Checkpoint LSN ≤ oldest WAL segment start LSN (no gap between checkpoint and WAL).
5. Replica applied LSN is tracked; WAL purge respects replication lag.
