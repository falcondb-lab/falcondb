# Segment Codec: Zstd via zstd-safe

## Architecture

```
┌─────────────────────────────────────────────────┐
│  falcon_storage / falcon_server / ...           │
│  (upper layers NEVER call zstd directly)        │
└───────────────────┬─────────────────────────────┘
                    │ only via SegmentCodecImpl trait
┌───────────────────▼─────────────────────────────┐
│            falcon_segment_codec                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐│
│  │NoneCodec │ │Lz4Block  │ │ZstdBlockCodec    ││
│  │          │ │Codec     │ │(zstd-safe 7.x)   ││
│  └──────────┘ └──────────┘ └──────────────────┘│
│  ┌──────────────────────────────────────────────┤
│  │ DictionaryRegistry │ DecompressPool+Cache   ││
│  │ StreamingCodec     │ CompressMetrics         ││
│  └──────────────────────────────────────────────┘
└─────────────────────────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────┐
│              zstd-safe (safe Rust API)           │
│              zstd-sys (C bindings, vendored)     │
│              libzstd (C library)                 │
└─────────────────────────────────────────────────┘
```

## Why zstd-safe, Not the High-Level zstd Crate

| Concern | High-level `zstd` | `zstd-safe` |
|---------|-------------------|-------------|
| API control | Opinionated wrappers | Direct CCtx/DCtx access |
| Dictionary loading | Hidden behind helpers | Explicit `load_dictionary()` |
| Parameter tuning | Limited | Full `CParameter`/`DParameter` |
| libzstd version | Implicit | `version_number()` queryable |
| Upgrade path | Coupled | zstd-sys swappable |

## SegmentCodecImpl Trait

```rust
pub trait SegmentCodecImpl: Send + Sync {
    fn codec_id(&self) -> CodecId;
    fn compress_block(&self, input: &[u8]) -> Result<Vec<u8>, CodecError>;
    fn decompress_block(&self, input: &[u8], original_len: usize) -> Result<Vec<u8>, CodecError>;
}
```

**Hard rules:**
- Each `compress_block` call produces an **independent** zstd frame
- No streaming state carried across blocks
- Any block can be decompressed without context from other blocks
- Upper layers only see `SegmentCodecImpl`, never raw zstd API

## ZstdBlockCodec

### Initialization

```rust
let config = ZstdCodecConfig {
    level: 3,       // 1-22
    checksum: true, // zstd frame checksum
};

// Without dictionary
let codec = ZstdBlockCodec::new(config);

// With dictionary
let codec = ZstdBlockCodec::with_dictionary(config, dict_bytes);
```

### Internal Flow (zstd-safe)

**Compress:**
```
CCtx::create()
  → set_parameter(CompressionLevel)
  → set_parameter(ChecksumFlag)
  → load_dictionary(dict)     // optional
  → compress2(&mut output, input)
  → truncate to written bytes
```

**Decompress:**
```
DCtx::create()
  → load_dictionary(dict)     // optional
  → decompress(&mut output, input)
  → truncate to written bytes
```

## Block Wire Format

```
[BlockHeader: 12 bytes]
  uncompressed_len: u32 LE
  compressed_len:   u32 LE
  block_crc:        u32 LE (djb2 of compressed data)

[CompressedBlockBytes: compressed_len bytes]
```

- Block is the minimum decompression unit
- Supports partial/random read
- Supports cache by `(segment_id, block_index)`

## Segment Body Layout

```
[block_count: u32 LE]
[CompressedBlock 0]
[CompressedBlock 1]
...
[CompressedBlock N-1]
```

## Codec Availability

| Segment Kind | None | LZ4 | Zstd | Default |
|-------------|------|-----|------|---------|
| WAL | ✅ | ✅ | ❌ | None |
| Cold | ✅ | ✅ | ✅ | Zstd (level 3) |
| Snapshot | ❌ | ❌ | ✅ | Zstd (level 5) |

## Features

```toml
[features]
default = ["zstd"]
zstd = []  # enables ZstdBlockCodec
```

## Metrics

| Metric | Description |
|--------|-------------|
| `zstd_compress_cpu_ns_total` | Total CPU time in compression |
| `zstd_decompress_cpu_ns_total` | Total CPU time in decompression |
| `segment_compression_ratio{kind}` | Compression ratio by segment kind |
| `dictionary_hit_rate` | Dictionary lookup hit rate |
| `dictionary_bytes_total` | Total dictionary bytes loaded |

## libzstd Version

```rust
let version = falcon_segment_codec::zstd_version(); // e.g. 10506
```
