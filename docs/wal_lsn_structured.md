# Structured LSN (Segment + Offset) & Reservation-Based Allocation

## Why LSN = Segment + Offset

A Log Sequence Number in FalconDB is **not** an opaque counter. It is a
**physical address** into the WAL with ordering guarantees.

```
LSN (64-bit) = (segment_id << 28) | offset
               ├── 36 bits ──┤├─ 28 bits ─┤
```

- **segment_id** — logical WAL segment number (monotonically increasing)
- **offset** — byte position within the segment (0-based, max 256 MB)

### Invariants

| # | Invariant |
|---|-----------|
| 1 | LSN is globally monotonically increasing |
| 2 | Within a segment, offset is monotonically increasing |
| 3 | On segment switch, offset resets to 0 |
| 4 | LSN uniquely determines the WAL physical position |
| 5 | No per-record atomic operation — batch reservation only |

### Benefits

- **Crash recovery**: scan segment headers → rebuild last valid LSN
- **Replication**: subscriber sends LSN, sender seeks directly to position
- **Snapshot/PITR**: checkpoint stores LSN, recovery seeks to exact position
- **Observability**: LSN encodes both "which file" and "where in file"

## Segment Physical Layout

Every WAL segment file starts with a **4K-aligned header**:

```
Offset  Size  Field
──────  ────  ─────
0       8     magic ("FWAL_V02")
8       4     version (2)
12      4     (padding)
16      8     segment_id
24      8     segment_size
32      8     start_lsn (raw u64)
40      8     last_valid_offset
48      4     CRC32 checksum
52      4     (padding)
56..4095      (reserved, zero-filled)
4096          ← first record starts here
```

**`last_valid_offset`** is the offset of the last completely written record.
Updated on flush/seal. Recovery stops scanning at this offset.

### Record Layout

Records do **NOT** store their own LSN. The LSN is computed from the
record's physical position:

```
Offset  Size  Field
──────  ────  ─────
0       4     record_len (payload only)
4       2     flags
6       2     (reserved)
8       4     CRC32 of payload
12      N     payload (bincode-serialized WalRecord)
```

Flags:
- `0x0000` — normal record
- `0x0001` — payload starts with a transaction ID
- `0x8000` — padding record (skip during replay)

## LSN → Physical Position

The **only** way to locate a WAL record:

```rust
fn lsn_to_physical(lsn: StructuredLsn) -> PhysicalPosition {
    PhysicalPosition {
        segment_id: lsn.segment_id(),       // which file
        file_offset: 4096 + lsn.offset(),   // where in file
    }
}
```

**Prohibited:**
- ❌ Side tables mapping LSN → position
- ❌ Inferring position from current file pointer

## Reservation-Based Allocation

### The Problem

Per-record `AtomicU64::fetch_add(len)`:
- Cache line bouncing under high concurrency
- p99/p999 latency spikes
- Hostile to group commit (each record in a batch is a separate atomic)

### The Solution

Writers request a **reservation** — a contiguous LSN range — from the
global allocator. Within the reservation, allocation is a local pointer
bump with **zero global synchronization**.

```
Writer Thread                    Global Allocator
─────────────                    ────────────────
reserve(64KB) ─────────────────► [mutex: advance offset by 64KB]
                                 ◄── Reservation { base=0/0, limit=0/65536 }

allocate(128) → LSN 0/0         (local pointer bump, no atomic)
allocate(256) → LSN 0/128       (local pointer bump, no atomic)
allocate(128) → LSN 0/384       (local pointer bump, no atomic)
...
allocate(128) → None             (reservation exhausted)

reserve(64KB) ─────────────────► [mutex: advance offset by 64KB]
                                 ◄── Reservation { base=0/65536, limit=0/131072 }
```

### Guarantees

| Property | Guarantee |
|----------|-----------|
| Uniqueness | No two reservations overlap (mutex-serialized) |
| Ordering | Reservations are globally ordered by base LSN |
| No cross-segment | A reservation never crosses a segment boundary |
| Atomic ops | ≥90% fewer than per-record allocation |

### Segment Rollover

When a reservation request cannot fit in the current segment:

1. Seal the current segment (update `last_valid_offset` in header)
2. Allocate new segment ID (increment by 1)
3. Reset offset to 0
4. Return reservation from the new segment + the sealed segment info

Rollover is serialized inside the allocator mutex.

## Group Commit Integration

Group commit uses `reserve_exact()` — the batch knows its exact size:

```rust
// 32 records × 256 bytes = 8 KB
let batch_size = 32 * 256;
let result = allocator.reserve_exact(batch_size);

// Single mutex acquisition for the entire batch
let mut cursor = WriterCursor::new(result.reservation());
for record in batch {
    let lsn = cursor.allocate(record.len());  // zero atomics
    write_record_at(lsn, record);
}

// Single fsync covers all records up to reservation limit
fsync();
```

**Result:** 1 atomic touch per batch, not 1 per record.

## Alignment Strategy

Record offsets are aligned to a configurable boundary:

| Alignment | Use Case |
|-----------|----------|
| 8 bytes | Default, cache-friendly |
| 64 bytes | Cache-line aligned |
| 4096 bytes | `FILE_FLAG_NO_BUFFERING` / `O_DIRECT` / raw disk |

`segment_size` must be a multiple of the alignment granularity.

## Crash Recovery

Recovery requires **only** segment headers + sequential scan:

1. **Scan** all segment files, read first 4096 bytes of each
2. **Validate** header: magic, version, CRC32
3. **Sort** by segment_id
4. **Resume** from the `last_valid_offset` of the highest valid segment
5. **Sequential scan** from `last_valid_offset` forward to find any records
   written after the last flush (torn write detection via CRC32)

```rust
let state = recover_from_headers(&headers);
let allocator = LsnAllocator::new(config, 0, 0);
for seg in &state.segments {
    allocator.recover_from_header(&seg.header);
}
// Allocator now resumes from the correct position
```

**No external state needed.** The WAL is self-describing.

## Observability

| Metric | Description |
|--------|-------------|
| `wal_lsn_reservation_batches_total` | Total reservations handed out |
| `wal_lsn_reserved_bytes_total` | Total bytes reserved |
| `wal_lsn_reservation_avg_bytes` | Average reservation size |
| `wal_segment_rollover_total` | Segment switches |
| `wal_segment_utilization_ratio` | Average segment fill before rollover |

### What to watch for

- **`reservation_avg_bytes` too small** → increase `default_reservation_bytes`
- **`segment_utilization_ratio` < 0.5** → reservations are too large relative to segment
- **`rollover_total` growing fast** → increase `segment_size`

## Long-Term Value

| Feature | How Structured LSN Helps |
|---------|--------------------------|
| **Replication** | Subscriber sends LSN, sender seeks directly — no index lookup |
| **PITR** | Target LSN encodes exact segment + position |
| **Snapshot** | Checkpoint stores LSN, recovery starts at exact byte |
| **Parallel recovery** | Each segment can be replayed independently |
| **Raw disk WAL** | 4K-aligned reservations map directly to disk sectors |
| **Cloud WAL** | Segment = object in S3/GCS, offset = byte range |
