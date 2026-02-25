# Replication & Snapshot via Segment-Level Streaming

## Why Not Record Replay?

Traditional replication replays WAL records one by one on the follower.
This has fundamental problems at scale:

| Problem | Record Replay | Segment Streaming |
|---------|--------------|-------------------|
| CPU cost | Deserialize + re-execute every record | Zero — raw byte copy |
| I/O pattern | Random (record-by-record apply) | Sequential (segment-at-a-time) |
| Leader overhead | Must read + serialize records | Send sealed file as-is |
| Network efficiency | Small messages, high overhead | Large chunks, minimal framing |
| Recovery | Must replay from checkpoint | Resume from last sealed segment |
| Determinism | Depends on execution order | Bit-for-bit identical WAL |

## Architecture

```
Leader                              Follower
──────                              ────────
WAL Segment 0 (sealed) ──────────► Write segment 0, seal
WAL Segment 1 (sealed) ──────────► Write segment 1, seal
WAL Segment 2 (sealed) ──────────► Write segment 2, seal
WAL Segment 3 (active)  ·········► Tail stream: offset N → current
                    ↑                        ↑
            append-only, immutable     sequential write only
```

## Two Replication Paths

### Segment Streaming (Fast Path)

**When:** Follower lags ≥1 complete sealed segment.

1. Follower sends `ReplicationHello` with its sealed segment set
2. Leader computes missing segments
3. Leader streams each sealed segment as 4–16 MB chunks
4. Follower writes chunks sequentially, verifies CRC32, seals
5. Repeat until all sealed segments are transferred

### Tail Streaming (Slow Path)

**When:** Follower is on the same segment as leader, just behind on offset.

1. Leader reads from follower's offset to its current write position
2. Sends as a `TailBatch` (typically 64 KB)
3. Follower appends to its active segment

Tail streaming is the **supplementary** path, not the main one.

## Follower State Machine

```
INIT → HANDSHAKING → SEGMENT_STREAMING → TAIL_STREAMING → CAUGHT_UP
                          ↓                    ↓
                        ERROR ←──────────── ERROR
                          ↓
                    (rollback to last sealed, re-handshake)
```

### Follower State

| Field | Description |
|-------|-------------|
| `local_last_lsn` | Last fully applied structured LSN |
| `local_segment_id` | Current segment being written |
| `local_segment_offset` | Offset within current segment |
| `sealed_segments` | Set of fully persisted segment IDs |

## Handshake Protocol

```
Follower → Leader:
  ReplicationHello {
    node_id: 42,
    last_lsn: 3/0,
    last_segment_id: 3,
    last_segment_offset: 0,
    sealed_segment_ids: [0, 1, 2]
  }

Leader evaluates:
  - sealed_segments = {0, 1, 2, 3, 4}
  - follower has {0, 1, 2}
  - missing: {3, 4}
  → ReplicationPath::SegmentStreaming { segments_to_send: [3, 4] }
```

## Segment Streaming Protocol

Each segment is sent as a sequence of chunks:

```
StreamSegment {
  segment_id: 3,
  segment_size: 268435456,
  last_valid_offset: 150000,
  chunks: [
    { offset: 0,     data: [4MB], checksum: 0x..., is_last: false },
    { offset: 4MB,   data: [4MB], checksum: 0x..., is_last: false },
    ...
    { offset: 148KB, data: [2KB], checksum: 0x..., is_last: true  },
  ]
}
```

**Invariants:**
- Chunk data is raw bytes from the segment file (no transformation)
- Each chunk has a CRC32 checksum
- `is_last = true` on the final chunk triggers segment seal on follower

## Snapshot = Segment Cut + Metadata

A snapshot is NOT a data export. It is:

```
SnapshotManifest {
  snapshot_id: 1,
  snapshot_lsn: 5/0,            // always a segment boundary
  sealed_segments: [0, 1, 2, 3, 4],
  schema_metadata: <catalog bytes>,
  txn_metadata: None,
}
```

To restore a follower from a snapshot:
1. Stream all segments in the manifest
2. No record replay needed
3. Follower starts tail streaming from `snapshot_lsn`

## Backpressure

The sender tracks in-flight chunks:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_inflight_chunks` | 8 | Max unacked chunks before pausing |
| `chunk_size` | 4 MB | Bytes per chunk |

When inflight reaches the limit, the sender pauses until the follower acks.
Supports cancel/resume for graceful interruption.

## Error Handling

| Error | Recovery Action |
|-------|----------------|
| Checksum mismatch | Retry the specific chunk |
| Disk full | Abort replication |
| Network interrupted | Rollback to last sealed, re-handshake |
| Leader changed | Rollback to last sealed, re-handshake |

On error, the follower rolls back to its last sealed segment boundary:
```
state.rollback_to_last_sealed()
// → local_segment_id = last_sealed + 1
// → local_segment_offset = 0
// → re-handshake with new/recovered leader
```

## Invariants (Non-Negotiable)

- ❌ Replication never modifies WAL
- ❌ Snapshot never writes to WAL
- ✅ WAL segments are immutable once sealed
- ✅ Replication is a read-only view of WAL
- ✅ Follower writes are sequential (no random I/O)

## Relationship to Structured LSN

Segment streaming depends on the structured LSN design:

- `StructuredLsn = (segment_id << 28) | offset` uniquely identifies position
- Segment headers are the recovery anchor points
- The `LsnAllocator` manages segment rollover; replication reads sealed segments
- Tail streaming uses the LSN offset to know exactly where to resume

## Metrics

| Metric | Description |
|--------|-------------|
| `replication_segment_streamed_total` | Sealed segments sent to followers |
| `replication_segment_bytes_total` | Total bytes via segment streaming |
| `replication_tail_bytes_total` | Total bytes via tail streaming |
| `replication_lag_segments` | Per-follower segment lag |
| `replication_lag_bytes` | Per-follower estimated byte lag |
| `replication_checksum_failures` | CRC32 mismatches detected |
| `replication_error_rollbacks` | Follower error rollbacks |

## Future Compatibility

This design naturally supports:

| Feature | How |
|---------|-----|
| **Raw Disk WAL** | Segments = physical disk blocks, streamed as-is |
| **Compressed segments** | Seal → compress → stream compressed bytes |
| **Multi-follower parallel** | Each follower independently streams missing segments |
| **Heterogeneous nodes** | Cold nodes receive segments without executing anything |
| **Cloud WAL (S3/GCS)** | Segment = object, offset = byte range |
| **Snapshot + replication unified** | Snapshot manifest lists segments; streaming is the same path |
