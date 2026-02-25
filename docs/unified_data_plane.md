# Unified Data Plane — Design Document

> **Segments are the only physical truth. Manifest describes truth; streaming moves truth.**

## Core Principle

WAL, Cold Store, and Snapshot all share the same segment format, IO interface,
and streaming protocol. Recovery, replication, and rebalance use one code path.

## Segment Kinds

| Kind | Purpose | Mutability | Codec |
|------|---------|-----------|-------|
| `WAL_SEGMENT` | Append-only WAL records, LSN = segment + offset | Append until sealed | None |
| `COLD_SEGMENT` | Immutable compressed row/version payloads | Immutable after write | LZ4/Zstd |
| `SNAPSHOT_SEGMENT` | Snapshot data chunks | Immutable | None/LZ4 |

All three share the same `UnifiedSegmentHeader` (4K-aligned) and are managed
by the same `SegmentStore` API.

## Unified Segment Header (4K aligned)

```
Field               Size    Description
─────────────────   ──────  ─────────────────────────────────────
magic               4B      0x46535547 ("FSUG")
version             2B      Header format version (currently 1)
kind                1B      0=WAL, 1=Cold, 2=Snapshot
segment_id          8B      Unique segment identifier
segment_size        8B      Total segment size (incl. header)
created_at_epoch    8B      Creation time (unix seconds)
codec               1B      0=None, 1=LZ4, 2=Zstd
checksum            4B      CRC32 of header fields
last_valid_offset   8B      Last valid data offset
logical_range       var     Kind-specific routing metadata
  WAL:    [start_lsn: u64, end_lsn: u64]
  Cold:   [table_id: u64, shard_id: u64, min_key, max_key]
  Snap:   [snapshot_id: u64, chunk_index: u32]
(zero-padded to 4096 bytes)
```

## Manifest

The Manifest describes "which segments constitute the current database state":

- **Append-only**: entries added, never removed in-place
- **Epoch-versioned**: every mutation increments the epoch
- **Checksummed**: verifiable integrity
- **Portable**: replicated alongside segments

### Manifest Contents

| Field | Description |
|-------|-------------|
| `epoch` | Monotonically increasing version counter |
| `segments` | BTreeMap of segment_id → ManifestEntry |
| `snapshot_cutpoint` | Current snapshot LSN/CSN cut (if any) |
| `history` | Append-only delta log for audit/replication |

### ManifestEntry

```rust
struct ManifestEntry {
    segment_id: u64,
    kind: SegmentKind,      // Wal / Cold / Snapshot
    size_bytes: u64,
    codec: SegmentCodec,
    logical_range: LogicalRange,
    sealed: bool,
}
```

## SegmentStore API

Unified IO substrate for all segment kinds:

| Method | Description |
|--------|-------------|
| `create_segment(header)` | Create a new segment |
| `open_segment(id)` | Get header of existing segment |
| `read_chunk(id, offset, len)` | Read bytes from a segment |
| `write_chunk(id, data)` | Append to a segment (WAL) |
| `write_chunk_at(id, offset, data)` | Write at specific offset (streaming receive) |
| `seal_segment(id)` | Mark segment immutable |
| `verify_segment(id)` | Verify header checksum |
| `delete_segment(id)` | Delete segment (GC) |

Backend-pluggable: file (default), Windows async, raw disk (future).

## Streaming Protocol

All data movement uses one wire format:

```
StreamBegin     { manifest_epoch, stream_id, protocol_version }
StreamSegHeader { segment_id, kind, size, codec, checksum }
StreamChunk     { segment_id, chunk_index, data, crc, is_last }
StreamEnd       { stream_id, segments_transferred, bytes_transferred }
```

Supports: backpressure, resume (segment_id + chunk_index), cancel, version negotiation.

## Replication = Manifest + Segments

### Handshake

```
Follower → Leader:
  last_manifest_epoch
  have_segments (compact set)

Leader → Follower:
  manifest_delta (or full manifest)
  required_segments (missing list)
  tail_streaming (active WAL segment info, if applicable)
```

### Flow

1. Leader computes `missing_segments = leader.segments - follower.have`
2. Stream sealed segments first (bulk catch-up)
3. If lag is too large → send snapshot segments first
4. Tail stream active WAL segment for near-real-time replication
5. Follower applies manifest, verifies segments, enters normal replication

### Bootstrap

Empty follower can bootstrap entirely through this protocol — no special path needed.

## Snapshot = Manifest Cut + Segment Set

A snapshot is **not** a separate data export. It is:

```
SnapshotDefinition {
    snapshot_id,
    manifest_epoch,
    cutpoint: { cut_lsn, cut_csn },
    wal_segments: [sealed WAL segment IDs],
    cold_segments: [cold segment IDs],
    snapshot_segments: [snapshot-specific chunks],
}
```

### Restore

1. Receive manifest + segment set via streaming protocol
2. Unified recovery path: load manifest → verify → fetch missing → replay tail → serve

## Cold Store Integration

Cold compactor generates `COLD_SEGMENT` entries:

- Immutable once sealed — never modified
- Manifest tracks: key-range, table/shard scope, codec, compression ratio
- Hot data holds `UnifiedColdHandle { segment_id, offset, len }` references
- Compaction = create new cold segment → update manifest → GC old segment

## Unified Recovery / Bootstrap

**One code path** for all scenarios:

```
1. Load manifest (local or from leader)
2. Verify required segments exist and pass checksum
3. If missing → enter FETCH_MISSING phase → stream from leader
4. Replay WAL tail (if needed)
5. Ready to serve
```

This handles: new node join, crash recovery, snapshot restore, catch-up after partition.

## Segment GC

| Segment Kind | GC Eligible When |
|-------------|-----------------|
| WAL | All followers' durable_lsn past segment's end_lsn |
| Cold | Replaced in manifest (compaction) and no references |
| Snapshot | Snapshot expired and not a recovery anchor |

**Safety**: Segments currently being streamed are protected from GC via `mark_streaming()`.

### CLI

```
falconctl segments ls          # list all segments
falconctl segments gc --dry-run  # preview GC candidates
falconctl segments verify      # verify all checksums
```

## Metrics

| Metric | Description |
|--------|-------------|
| `segment_store_bytes_total{kind}` | Total bytes by segment kind |
| `segment_stream_bytes_total{kind}` | Bytes streamed by kind |
| `segment_stream_resume_total` | Stream resume count |
| `manifest_epoch` | Current manifest epoch |
| `segment_gc_bytes_total` | Bytes freed by GC |
| `cold_compression_ratio` | Cold store compression ratio |
| `segments_created/sealed/deleted` | Lifecycle counters |

## Future Extensions

- **Raw Disk backend**: SegmentStore backend using O_DIRECT
- **Encryption**: per-segment encryption with key rotation via manifest
- **Deduplication**: content-addressed cold segments
- **Tiered storage**: S3/OSS cold tier via SegmentStore backend plugin
