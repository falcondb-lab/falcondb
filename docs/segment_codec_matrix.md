# Segment Codec Matrix

## Codec Availability by Segment Kind

| Segment Kind | `none` | `lz4` | `zstd` | Default |
|-------------|--------|-------|--------|---------|
| WAL (active tail) | ✅ | ✅ | ❌ **forbidden** | `none` |
| WAL (sealed, long-term) | ✅ | ✅ | ❌ | `none` |
| COLD_SEGMENT | ✅ | ✅ | ✅ | `zstd` (level 3) |
| SNAPSHOT_SEGMENT | ❌ | ❌ | ✅ **forced** | `zstd` (level 5) |
| Streaming transport | ✅ | ✅ | ✅ | adaptive |

## Codec Properties

| Property | None | LZ4 | Zstd |
|----------|------|-----|------|
| Compression ratio | 1.0x | 2-3x | 3-8x |
| Compress speed | N/A | ~800 MB/s | ~300 MB/s (level 3) |
| Decompress speed | N/A | ~4 GB/s | ~1.2 GB/s |
| Dictionary support | N/A | No | Yes |
| CPU cost | Zero | Low | Medium |
| p99 impact | None | Minimal | Requires isolation |

## Zstd Compression Levels

| Level | Ratio | Speed | Use Case |
|-------|-------|-------|----------|
| 1 | ~2.5x | Fast | Streaming, real-time |
| 3 | ~3.5x | Medium | Cold segments (default) |
| 5 | ~4.0x | Slower | Snapshots |
| 9 | ~4.5x | Slow | Archival (manual) |

## Configuration

```toml
[compression]
wal_segment_codec = "none"          # none | lz4
cold_segment_codec = "zstd"         # none | lz4 | zstd
snapshot_segment_codec = "zstd"     # zstd (forced)
zstd_cold_level = 3
zstd_snapshot_level = 5
zstd_streaming_level = 1

[compression.streaming]
codec = "adaptive"                  # none | lz4 | zstd | adaptive

[compression.cache]
capacity_bytes = 67108864           # 64 MB decompress cache
max_concurrent_decompress = 8
```

## Streaming Codec Negotiation

| Sender Preference | Receiver Preference | Negotiated |
|-------------------|---------------------|------------|
| LZ4 | LZ4 | LZ4 |
| Zstd | Zstd | Zstd |
| LZ4 | Zstd | Zstd (receiver wins) |
| Zstd | LZ4 | Zstd (sender wins) |
| None | Any | First common |

## Decompression Isolation

Zstd decompression **never** runs on the OLTP executor thread.

```
DecompressPool:
  max_concurrent: 8 (configurable)
  cache: LRU, 64 MB (configurable)
  key: (segment_id, block_index)
  rejection: error when max_concurrent reached
```

## Block Wire Format

```
[uncompressed_len: u32 LE]
[compressed_len:   u32 LE]
[encoding:         u8]
[compressed_data:  bytes]
[crc:              u32 LE]
```

## Dictionary Integration

| Segment Kind | Dictionary Support | Notes |
|-------------|-------------------|-------|
| WAL | No | WAL is never dict-compressed |
| COLD_SEGMENT | Yes | Per-table dictionary |
| SNAPSHOT_SEGMENT | Optional | Can use table dictionary |
| Dictionary segment | N/A | Contains dict data itself |

## Monitoring

```bash
# List segments by codec
falconctl segments ls --codec=zstd

# Show compression metrics
falconctl segments stats

# Dictionary management
falconctl dictionary ls
falconctl dictionary stats

# Recompress a segment
falconctl segments recompress --segment=42 --codec=zstd --level=5 --dict=new
```

## Safety Invariants

1. WAL active tail: **never** compressed with zstd
2. Segment compression: **always** creates new segment, never in-place
3. Dictionary: **immutable** once created, versioned by dictionary_id
4. Manifest: **always** updated atomically after compression
5. Decompression: **always** in isolated pool, never on executor thread
6. CRC: **always** verified before decompression
