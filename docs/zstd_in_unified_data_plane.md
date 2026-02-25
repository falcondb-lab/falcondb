# Zstd in the Unified Data Plane

> **"Zstd is not a row compressor. It is a segment-level infrastructure primitive."**

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Codec Policy                          │
│  WAL=none/lz4 │ Cold=zstd │ Snapshot=zstd │ Stream=adaptive │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              Segment-Level Compression                    │
│  ZstdBlock: [uncompressed_len][compressed_len][enc][data][crc] │
│  write_zstd_cold_segment() / read_zstd_cold_segment()   │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              Dictionary Store                             │
│  train → register → store_as_segment → Manifest track    │
│  Per-table │ Per-schema-version │ Versioned               │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│           Decompression Isolation                         │
│  DecompressPool (concurrency limit) + DecompressCache (LRU) │
│  NEVER on OLTP executor thread                            │
└─────────────────────────────────────────────────────────┘
```

## Part A — Compression Scope

### Where Zstd Is Forbidden

| Location | Codec | Reason |
|----------|-------|--------|
| Active WAL segment (tail) | none/lz4 | Latency-critical append path |
| OLTP hot row / hot version | none | MVCC visibility must be zero-copy |
| High-frequency point reads | none | p99 protection |

### Where Zstd Is Required/Recommended

| Location | Codec | Reason |
|----------|-------|--------|
| COLD_SEGMENT | zstd (default) | Best ratio for immutable data |
| SNAPSHOT_SEGMENT | zstd (forced) | Snapshots are read-rarely, compress well |
| Segment Streaming | adaptive | Cross-DC benefits from zstd; same-DC uses lz4 |
| Sealed WAL (long-term) | lz4 (optional) | Speed over ratio for recent WAL |

### Default Configuration

```
wal.segment.codec       = none        # NEVER zstd
cold.segment.codec      = zstd        # default level 3
snapshot.segment.codec  = zstd        # forced, level 5
streaming.codec         = adaptive    # negotiated per-connection
```

## Part B — Block-Level Compression

### ZstdBlock Wire Format

```
┌──────────────────────────────────────────────────┐
│ uncompressed_len: u32 LE                          │
│ compressed_len:   u32 LE                          │
│ encoding:         u8 (Raw=0/Lz4=1/Zstd=2/...)   │
│ compressed_data:  [u8; compressed_len]            │
│ crc:              u32 LE (djb2 of compressed_data)│
└──────────────────────────────────────────────────┘
```

### Cold Segment Layout

```
[4K UnifiedSegmentHeader (kind=COLD, codec=ZSTD)]
[block_count: u32 LE]
[ZstdBlock 0]
[ZstdBlock 1]
...
[ZstdBlock N-1]
```

### Compression Flow

```
1. ColdCompactor collects rows for compaction
2. write_zstd_cold_segment(store, table, shard, rows, level, dict, dict_id)
3.   → create new segment (NEVER modify existing)
4.   → write block_count
5.   → for each row: zstd_compress_block(row, level, dict) → write ZstdBlock
6.   → seal segment
7. Caller updates Manifest atomically
```

## Part C — Dictionary Lifecycle

See `docs/dictionary_lifecycle.md` for full details.

Summary:
- Dictionaries are trained from recent cold segment samples
- Stored as special segments in SegmentStore
- Tracked in Manifest via `dictionary_id → segment_id`
- Replicated via same Segment Streaming protocol
- Bootstrap fetches dict segments BEFORE data segments

## Part D — Streaming Codec Negotiation

```
Sender   → StreamingCodecCaps { supported, preferred, bandwidth_estimate }
Receiver → StreamingCodecCaps { supported, preferred, bandwidth_estimate }
         → negotiate_streaming_codec() → agreed codec
```

| Scenario | Result |
|----------|--------|
| Same datacenter (10 GB/s) | LZ4 (speed) |
| Cross datacenter (100 MB/s) | Zstd (ratio) |
| Mixed preferences | Zstd wins if either prefers it |

## Part E — Decompression Isolation

### Hard Rule

Zstd decompression **NEVER** runs on the OLTP executor thread.

### DecompressPool

- Configurable `max_concurrent` limit
- Rejects with error when overloaded (backpressure)
- Tracks `decompress_total`, `decompress_ns_total`, `decompress_errors`

### DecompressCache

- LRU eviction, byte-capacity limited
- Key: `(segment_id, block_index)`
- Metrics: `hit_rate`, `evictions`, `bytes_cached`

## Part F — GC Rewrite = Recompression

When GC merges cold segments or a dictionary upgrades:

```
1. Read all blocks from source segment (decompress with old dict)
2. Recompress with new codec/level/dict
3. Write to new segment
4. Update Manifest (add new, schedule GC for old)
```

Reasons: `GcMerge`, `DictUpgrade`, `CodecChange`, `Manual`

## Part G — Metrics

### /admin/status Zstd Section

```json
{
  "compress_total": 1500,
  "compress_bytes_in": 150000000,
  "compress_bytes_out": 45000000,
  "compress_ratio": 3.33,
  "decompress_total": 8000,
  "decompress_avg_us": 120.5,
  "decompress_errors": 0,
  "cache_hit_rate": 0.85,
  "cache_used_bytes": 33554432,
  "dict_count": 3,
  "dict_bytes_total": 32768,
  "dict_hit_rate": 0.95
}
```

### CLI Commands

```bash
falconctl segments ls --codec=zstd
falconctl dictionary ls
falconctl dictionary stats
falconctl segments recompress --codec=zstd --level=5 --dict=new
```

## Test Matrix

| Category | Test | Status |
|----------|------|--------|
| H1.1 | Checksum failure → rejected | ✅ |
| H1.2 | Dictionary required for read | ✅ |
| H1.3 | Dictionary segment roundtrip | ✅ |
| H1.4 | Crash during compaction → consistent | ✅ |
| H1.5 | All codecs roundtrip | ✅ |
| H1.6 | Codec policy enforcement | ✅ |
| H2.1 | Cached cold read < 1ms | ✅ |
| H2.2 | Repetitive data ratio ≥ 3x | ✅ |
| H2.3 | Streaming throughput ≥ 100 MB/s | ✅ |
| H2.4 | Pool overload protection | ✅ |
| H2.5 | Compaction ≥ 5K rows/sec | ✅ |
| H3.1 | Full dictionary lifecycle | ✅ |
| H3.2 | Recompression with dict upgrade | ✅ |
| H3.3 | Streaming negotiation E2E | ✅ |
| H3.4 | Cache effectiveness | ✅ |
| H3.5 | Metrics completeness | ✅ |
