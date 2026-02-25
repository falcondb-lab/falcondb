# Cold Segment Format

## Overview

A Cold Segment is a `COLD_SEGMENT` in the unified data plane. It is:
- **Immutable** after seal — never modified
- **Checksum-verifiable** — header CRC + per-block CRC
- **Addressable** — `segment_id + block_index` locates any block
- **Streamable** — transferred via segment streaming protocol
- **Manifest-managed** — lifecycle controlled by Manifest
- **Codec-abstracted** — all compression via `falcon_segment_codec::SegmentCodecImpl` trait

> **Since v1.2**: Compression is handled exclusively by the `falcon_segment_codec` crate
> (built on `zstd-safe`, not the high-level `zstd` crate). No code outside this crate
> calls zstd or lz4 directly.

## Physical Layout

```
Offset 0x0000 ─── UnifiedSegmentHeader (4096 bytes) ───────────
  magic:            0x46535547 ("FSUG")
  version:          1
  kind:             1 (COLD)
  segment_id:       u64
  segment_size:     u64
  created_at_epoch: u64 (unix seconds)
  codec:            u8 (0=None, 1=LZ4, 2=Zstd)
  checksum:         u32 (CRC32 of header fields)
  last_valid_offset: u64
  logical_range:    Cold {
    table_id:       u64
    shard_id:       u64
    min_key_len:    u32
    min_key:        [u8]
    max_key_len:    u32
    max_key:        [u8]
  }
  (zero-padded to 4096 bytes)

Offset 0x1000 ─── Block Count ─────────────────────────────────
  block_count:      u32 LE    (number of blocks in segment body)

Offset 0x1004 ─── CompressedBlock 0 (falcon_segment_codec::BlockHeader + data)
  uncompressed_len: u32 LE    (original data size)
  compressed_len:   u32 LE    (compressed data size)
  block_crc:        u32 LE    (djb2 CRC of compressed data)
  compressed_data:  [u8; compressed_len]

Offset 0x1004+N ─── CompressedBlock 1 ──────────────────────────
  ...

Offset ... ─── CompressedBlock K ───────────────────────────────
  ...
```

## Codec Table (`falcon_segment_codec::CodecId`)

| Code | Name | Description | Default For |
|------|------|-------------|-------------|
| 0 | None | Uncompressed passthrough | WAL segments |
| 1 | LZ4 | LZ4 via `lz4_flex` (fast, ~2-3x ratio) | Streaming (same-DC) |
| 2 | Zstd | Zstandard via `zstd-safe` (high ratio, 3-8x) | **Cold segments (default)**, Snapshot segments |

All codecs implement the `SegmentCodecImpl` trait. Upper layers never call zstd/lz4 directly.

### Legacy Block Encodings (`ColdBlockEncoding`)

The `unified_data_plane_full::ColdBlockEncoding` enum retains additional encodings
for specialized columnar use cases:

| Code | Name | Description |
|------|------|-------------|
| 3 | Dict | Dictionary-based compression (dictionary_id in header) |
| 4 | RLE | Run-length encoding |
| 5 | Delta | Delta encoding for numeric sequences |
| 6 | BitPack | Bit-packing for low-cardinality values |

## CRC Computation

Per-block CRC uses djb2 hash over `[encoding_byte] + payload`:

```rust
fn compute_block_crc(encoding: u8, payload: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    hash = hash.wrapping_mul(33).wrapping_add(encoding as u32);
    for &b in payload {
        hash = hash.wrapping_mul(33).wrapping_add(b as u32);
    }
    hash
}
```

## Block Read Path

```
1. Read at (segment_id, offset)
2. Parse: payload_len (4B) → encoding (1B) → payload → crc (4B)
3. Verify CRC
4. Decode payload according to encoding
5. Return row data
```

## Zstd Dictionary Support

Cold segments can optionally use a Zstd dictionary for improved compression ratio.

- Dictionary stored as `DICT_SEGMENT` in SegmentStore
- Manifest tracks `dictionary_id → segment_id`
- Segment header records: `dictionary_id`, `dictionary_checksum`
- Dictionary **missing** → segment **not loadable** (hard fail)
- Dictionary is **immutable** once created; versioned by `dictionary_id`
- Managed via `falcon_segment_codec::DictionaryRegistry` (load/use only; training is external)

See `docs/dictionary_in_segment_store.md` for full lifecycle.

## Decompression Isolation

Zstd decompression **never** runs on the OLTP executor thread.

- `falcon_segment_codec::DecompressPool` enforces concurrency limit
- `falcon_segment_codec::DecompressCache` provides LRU block cache keyed by `(segment_id, block_index)`
- Overloaded pool returns error (backpressure, never blocks OLTP)

## Hot → Cold Migration

The ColdCompactor creates new cold segments:

```
1. Collect rows from hot store (MVCC versions past retention)
2. Create codec: falcon_segment_codec::create_codec(CodecId::Zstd, config, dict)
3. For each row batch:
   a. CompressedBlock::compress(codec, row_data)
   b. Append to segment body via write_blocks()
4. Seal segment
5. Update Manifest (add new, mark old for GC)
```

### Invariants

- **New segment only** — compactor NEVER modifies existing segments
- **Manifest-first** — new segment added to manifest before old is removed
- **Seal before reference** — segment is sealed before manifest entry marks it sealed
- **Atomic manifest update** — add new + (eventual) remove old in manifest operations

## Cold Handle

Hot data references cold data via:

```rust
struct UnifiedColdHandle {
    segment_id: u64,   // which cold segment
    offset: u64,       // byte offset within segment body
    len: u32,          // length of the cold block
}
```

## Compression Ratio Tracking

The `ColdSegmentDescriptor` tracks:
- `original_bytes`: uncompressed size
- `compressed_bytes`: on-disk size
- `compression_ratio()`: `original / compressed`

Additionally, `falcon_segment_codec::ZstdSegmentMeta` records per-segment:
- `codec_id`, `codec_level`, `dictionary_id`, `dictionary_checksum`
- `block_count`, `uncompressed_bytes`, `compressed_bytes`

Exposed via `/admin/status` as `cold_compression_ratio`.

See also: `docs/segment_codec_zstd.md`, `docs/segment_codec_matrix.md`.
