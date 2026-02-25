# Streaming Codec Negotiation

## Overview

Streaming codec is **orthogonal** to segment codec:
- **Segment codec** decides how data is **stored** (on disk)
- **Streaming codec** decides how data is **transmitted** (over network)

They are independent — a Zstd-compressed segment can be streamed over LZ4 transport.

## Protocol

```
Sender   → StreamingCodecCaps { supported, preferred, estimated_bandwidth_bps }
Receiver → StreamingCodecCaps { supported, preferred, estimated_bandwidth_bps }
         → negotiate_streaming_codec() → agreed StreamingCodecId
```

## Negotiation Rules

1. Compute common codecs (intersection of supported lists)
2. If either side prefers Zstd and both support it → Zstd
3. Otherwise use sender's preference if common
4. Fallback to first common codec

| Sender | Receiver | Result |
|--------|----------|--------|
| LZ4 | LZ4 | LZ4 |
| Zstd | Zstd | Zstd |
| LZ4 | Zstd | Zstd (receiver wins) |
| Zstd | LZ4 | Zstd (sender wins) |
| None | None | None |

## Bandwidth Profiles

### High Bandwidth (same datacenter, ~10 GB/s)
- Supported: None, LZ4, Zstd
- Preferred: **LZ4** (speed over ratio)

### Low Bandwidth (cross-datacenter, ~100 MB/s)
- Supported: LZ4, Zstd
- Preferred: **Zstd** (ratio over speed)

## Chunk-Level Compression

**Hard requirement:** streaming compression happens at the **chunk** level.

- Each chunk is independently decompressible
- No state carried between chunks
- Does not affect segment checksums
- Chunk = transport unit, not storage unit

```rust
let compressed = compress_streaming_chunk(&data, codec, zstd_level)?;
let decompressed = decompress_streaming_chunk(&compressed, codec, max_size)?;
```

## Streaming vs Segment Codec Matrix

| Operation | Codec Used | Scope |
|-----------|-----------|-------|
| Write cold segment | Segment codec (Zstd) | Per-block, persisted |
| Read cold segment | Segment codec (Zstd) | Per-block, from disk |
| Stream segment to follower | Streaming codec (LZ4/Zstd) | Per-chunk, transient |
| Receive segment from leader | Streaming codec (decompress) | Per-chunk, transient |
| Store received segment | Already compressed on disk | No re-encode needed |

## Configuration

```toml
[streaming]
codec = "adaptive"     # none | lz4 | zstd | adaptive
zstd_level = 1         # streaming uses low level for speed
```

## Metrics

Streaming compression metrics flow through `CompressMetrics`:
- `compress_calls` / `compress_bytes_in` / `compress_bytes_out`
- `compress_cpu_ns`
- `ratio()` = bytes_in / bytes_out
