# Unified Data Plane — Full Implementation Design

> **"System only has Segments and Manifest; everything else is a view."**
>
> **Since v1.2**: All segment-level compression is handled by the `falcon_segment_codec` crate
> (built on `zstd-safe`). Upper layers access compression exclusively through the
> `SegmentCodecImpl` trait.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Manifest (SSOT)                       │
│  epoch │ segments{} │ cutpoint │ pins │ anchors │ LSNs  │
└──────────────────────┬──────────────────────────────────┘
                       │ describes
┌──────────────────────▼──────────────────────────────────┐
│                  Segment Store                           │
│  WAL_SEGMENT │ COLD_SEGMENT │ SNAPSHOT_SEGMENT           │
│  create │ read │ write │ seal │ verify │ delete          │
└──────────────────────┬──────────────────────────────────┘
                       │ moves via
┌──────────────────────▼──────────────────────────────────┐
│              Segment Streaming Protocol                   │
│  StreamBegin → SegHeader → Chunk(CRC) → StreamEnd        │
│  Backpressure │ Resume │ Cancel                          │
└─────────────────────────────────────────────────────────┘
```

## Part A — Cold Store Full Segmentization

### Transformation

| Before | After |
|--------|-------|
| Cold data = compressed blocks + private logic | Cold data = `COLD_SEGMENT` (immutable, streamable, replicable, GC-able) |
| Proprietary block format | Unified header (4K) + ColdBlock body |
| Private lifecycle | Manifest-managed lifecycle |

### Cold Segment Physical Format

```
┌─────────────────────────────── 4K ──────────────────────┐
│ UnifiedSegmentHeader                                     │
│   magic=FSUG │ kind=COLD │ codec │ logical_range(Cold)  │
├─────────────────────────────────────────────────────┤
│ block_count: u32 LE                                      │
│ CompressedBlock 0 (falcon_segment_codec::BlockHeader)    │
│   [uncompressed_len:u32][compressed_len:u32][crc:u32]    │
│   [compressed_data: compressed_len bytes]                 │
│ CompressedBlock 1 ...                                    │
│ CompressedBlock N                                        │
└─────────────────────────────────────────────────────┘
```

### Segment Codecs (`falcon_segment_codec::CodecId`)

| Code | Codec | Implementation | Default For |
|------|-------|---------------|-------------|
| 0 | None | `NoneCodec` | WAL |
| 1 | LZ4 | `Lz4BlockCodec` (lz4_flex) | Streaming (same-DC) |
| 2 | Zstd | `ZstdBlockCodec` (zstd-safe) | **Cold (default)**, Snapshot |

All codecs implement `SegmentCodecImpl` trait. No code outside `falcon_segment_codec` calls zstd/lz4.

### Legacy ColdBlock Encodings (columnar)

| Code | Encoding | Use Case |
|------|----------|----------|
| 3 | Dict | Dictionary-based |
| 4 | RLE | Run-length |
| 5 | Delta | Numeric columns |
| 6 | BitPack | Low-cardinality |

### Dictionary Segments (DICT_SEGMENT)

Zstd dictionaries are stored as special segments in SegmentStore:
- Manifest tracks `dictionary_id → segment_id`
- Cold/Snapshot segment headers record `dictionary_id` + `dictionary_checksum`
- `falcon_segment_codec::DictionaryRegistry` manages loaded dictionaries
- Dictionary missing → segment not loadable (hard fail)
- See `docs/dictionary_in_segment_store.md`

### ColdCompactor Invariants (Hard Constraints)

1. **ALWAYS** creates a new cold segment — never in-place modify
2. **NEVER** overwrites an existing segment body
3. **NEVER** bypasses Manifest for cold data references
4. After compaction: update Manifest → delay GC old segments

### Compaction Flow

```
1. ColdCompactor.compact(store, table, shard, rows, codec, replaced_ids)
2.   → store.next_segment_id()
3.   → store.create_segment(COLD header)
4.   → codec = falcon_segment_codec::create_codec(CodecId::Zstd, config, dict)
5.   → for each row: CompressedBlock::compress(codec, row) → store.write_chunk()
6.   → store.seal_segment()
7.   → return CompactionResult { new_segment_id, replaced_ids, stats }
8. ColdCompactor.apply_to_manifest(manifest, result)
9.   → manifest.add_segment(new cold entry)
10.  → (old segments remain until GC sweeps them)
```

## Part B — Manifest as SSOT

### ManifestSsot Contents

| Field | Description |
|-------|-------------|
| `manifest` | Core manifest (segments, epoch, cutpoint, history) |
| `snapshot_pinned` | Segments pinned by active snapshots |
| `catchup_anchors` | Per-follower segment sets needed for catch-up |
| `gc_safe_epoch` | Earliest manifest epoch that must be retained |
| `follower_durable_lsn` | Per-follower WAL progress for GC boundary |

### Reachability Rule

A segment is reachable if ANY of:
1. Present in `manifest.segments`
2. In `snapshot_pinned`
3. In any `catchup_anchors` entry

### Forbidden Patterns

| ❌ Forbidden | ✅ Required |
|-------------|------------|
| Implicit state | All state in Manifest |
| Memory-only topology | Manifest-tracked |
| Structural changes without Manifest write | Every change → epoch bump |

## Part C — Full Bootstrap

### Definition

Bootstrap = Manifest + Segments + Tail Replay → Serviceable Node

### BootstrapCoordinator Phases

```
INIT → FETCH_MANIFEST → COMPUTE_MISSING → STREAM_SEGMENTS
  → VERIFY_SEGMENTS → REPLAY_TAIL → READY
```

### Unified Path

| Scenario | Entry Point | Path |
|----------|------------|------|
| New node join | `new_empty_disk()` | Full path |
| Crash recovery | `from_local_manifest()` | Skip to COMPUTE_MISSING |
| Snapshot restore | `receive_manifest(snapshot_manifest)` | Full path |
| Catch-up after partition | `from_local_manifest()` | Partial fetch |

All scenarios use **the same coordinator**. No special code paths.

## Part D — Unified Replication & Snapshot

### Replication = Manifest Diff + Segment Streaming

```
Follower → Leader:  ReplicationHandshake { have_segments, epoch }
Leader  → Follower: ReplicationResponse  { required_segments, manifest }
Leader  → Follower: StreamBegin → Segments → StreamEnd
```

### Snapshot = Manifest Cut + Segment Set

A snapshot IS a manifest. Restore = replace manifest + fetch segments.

```rust
snapshot = SnapshotDefinition {
    manifest_epoch,
    cutpoint: { cut_lsn, cut_csn },
    wal_segments + cold_segments + snapshot_segments
}
```

## Part E — Two-Phase GC

### Safety Proof

A segment can be GC'd **if and only if**:
1. Not in `manifest.segments` (removed)
2. Not in `snapshot_pinned`
3. Not in any `catchup_anchors`
4. Not currently being streamed

### Phase 1: Mark (Dry-Run Safe)

```
falconctl gc plan
```

Produces a `GcPlan`:
- `eligible`: segments safe to delete + reason
- `deferred`: segments protected (streaming, pinned, anchor)
- `kept`: count of live segments
- `freeable_bytes`: total bytes recoverable

### Phase 2: Sweep (Destructive)

```
falconctl gc apply
```

1. Verify plan epoch matches current manifest epoch (stale plan → refuse)
2. Remove eligible segments from manifest
3. Double-check reachability (safety net)
4. Delete from segment store
5. Update metrics

### Rollback

```
falconctl gc rollback
```

No-op if sweep hasn't run. If sweep ran, segments are already deleted
(but manifest still has history for audit).

### GC Eligibility by Kind

| Kind | Eligible When |
|------|--------------|
| WAL | `end_lsn <= min_follower_durable_lsn` |
| Cold | Replaced by newer compaction (removed from manifest) |
| Snapshot | `snapshot_id < current_cutpoint.snapshot_id` |

## Part G — Admin Status

### /admin/status Response

```json
{
  "manifest_epoch": 42,
  "wal_segments_count": 15,
  "cold_segments_count": 8,
  "snapshot_segments_count": 2,
  "current_snapshot_id": 3,
  "sealed_segments_count": 23,
  "total_segment_bytes": 1073741824,
  "replication_lag_segments": 2,
  "bootstrap_state": null,
  "gc_state": { "mark_runs": 10, "sweep_runs": 8, "bytes_freed": 536870912 },
  "cold_compression_ratio": 3.2,
  "segment_live_bytes": 805306368,
  "segment_gc_bytes": 268435456,
  "segment_stream_bytes": 2147483648
}
```

## Test Matrix

### H1 — Correctness
- Empty-disk bootstrap (WAL + Cold)
- Cold compaction + replication consistency
- Snapshot restore → data consistent
- Manifest replay idempotent

### H2 — Fault Injection
- Streaming interrupt + resume
- Leader switch (epoch detection)
- GC + streaming concurrent safety
- Compactor crash restart
- CRC fault on cold blocks

### H3 — Performance Guardrails
- Bootstrap throughput ≥ 100 MB/s
- Cold compaction ≥ 10K rows/sec
- GC mark < 10ms for 1000 segments
