# FalconDB Compression Profiles

## Overview

FalconDB v1.0.8 introduces **compression profiles** — a single configuration knob that controls the entire hot/cold memory tiering system. Users don't need to understand cold store internals.

> **Since v1.2**: All codecs are provided by the `falcon_segment_codec` crate (built on `zstd-safe`).
> Zstd is now the default codec for Balanced and Aggressive profiles.

## Profiles

| Profile | Config Value | Cold Migration | Codec | Level | Cache Size | Use Case |
|---------|-------------|---------------|-------|-------|-----------|----------|
| Off | `"off"` | Disabled | None | — | 0 | Maximum throughput, no compression overhead |
| Balanced | `"balanced"` | 5 min age | **Zstd** | 3 | 16 MB | Default — good memory savings, moderate CPU |
| Aggressive | `"aggressive"` | 1 min age | **Zstd** | 5 | 64 MB | Maximum memory savings, higher CPU |
| Legacy-LZ4 | `"legacy-lz4"` | 5 min age | LZ4 | — | 16 MB | Pre-v1.2 behavior, LZ4 only |

## Configuration

```toml
# v1.0.8: single knob
compression_profile = "balanced"
```

That's it. No need to configure cold store, compactor, cache, or codec separately.

## Profile Details

### Off

```toml
compression_profile = "off"
```

- All data stays in hot memory (MemTable)
- No background compaction
- No block cache allocated
- Zero CPU overhead from compression
- **Best for**: Small datasets that fit in RAM, latency-critical workloads

Effective settings:
| Parameter | Value |
|-----------|-------|
| `compression_enabled` | false |
| `min_version_age` | ∞ (never migrate) |
| `codec` | none |
| `block_cache_capacity` | 0 |
| `compactor_batch_size` | 0 |
| `compactor_interval_ms` | 0 |

### Balanced (Default)

```toml
compression_profile = "balanced"
```

- Old MVCC versions (>5 min) migrated to cold store
- **Zstd compression** (level 3) via `falcon_segment_codec::ZstdBlockCodec`
- Typical ratio: 3–5x on repetitive data (up from 1.5–3x with LZ4)
- 16 MB block cache for read amortization
- Decompression isolated via `DecompressPool` (never on OLTP executor)
- Background compactor runs every 5 seconds
- **Best for**: Most workloads — saves 40–70% memory with minimal latency impact

Effective settings:
| Parameter | Value |
|-----------|-------|
| `compression_enabled` | true |
| `min_version_age` | 300 (~5 min) |
| `codec` | zstd |
| `zstd_level` | 3 |
| `block_cache_capacity` | 16 MB |
| `compactor_batch_size` | 1000 |
| `compactor_interval_ms` | 5000 |

### Aggressive

```toml
compression_profile = "aggressive"
```

- Old MVCC versions (>1 min) migrated to cold store
- **Zstd compression** (level 5) via `falcon_segment_codec::ZstdBlockCodec`
- Typical ratio: 4–8x on repetitive data
- 64 MB block cache (larger to handle more cold reads)
- Optional dictionary support for further ratio gains
- Background compactor runs every 1 second with larger batches
- **Best for**: Memory-constrained environments, large datasets, analytical workloads

Effective settings:
| Parameter | Value |
|-----------|-------|
| `compression_enabled` | true |
| `min_version_age` | 60 (~1 min) |
| `codec` | zstd |
| `zstd_level` | 5 |
| `block_cache_capacity` | 64 MB |
| `compactor_batch_size` | 5000 |
| `compactor_interval_ms` | 1000 |

## Observability

Monitor compression effectiveness via `GET /admin/status` → `memory` section:

```json
{
  "memory": {
    "hot_bytes": 52428800,
    "cold_bytes": 104857600,
    "cold_segments": 2,
    "compression_ratio": 0.42,
    "cold_read_total": 1500,
    "cold_decompress_avg_us": 12.5,
    "cold_decompress_peak_us": 89,
    "cold_migrate_total": 50000,
    "intern_hit_rate": 0.95
  }
}
```

### Key Indicators

| Metric | Healthy | Action if unhealthy |
|--------|---------|-------------------|
| `compression_ratio` | < 0.7 | Good savings. If > 0.9, data may not be compressible |
| `cold_decompress_avg_us` | < 50 | If high, increase `block_cache_capacity` |
| `cold_read_total` | Stable growth | Spikes may indicate cache misses |
| `intern_hit_rate` | > 0.8 | If low, string intern pool not effective for this workload |

## Changing Profiles

Profile changes take effect **without restart** for the compactor behavior. The block cache resizes on the next compaction cycle.

```toml
# Switch from balanced to aggressive
compression_profile = "aggressive"
```

### Migration Path

| From → To | Impact |
|-----------|--------|
| off → balanced | Cold migration starts after 5 min; gradual memory reduction |
| off → aggressive | Cold migration starts after 1 min; faster memory reduction |
| balanced → aggressive | Migration threshold lowers; more data moves to cold |
| aggressive → balanced | Migration slows; some cold data stays compressed |
| any → off | No new cold migration; existing cold data stays until GC |

## Performance Expectations

| Profile | Memory Savings | p99 Latency Impact | CPU Overhead | Compression Ratio |
|---------|---------------|-------------------|-------------|------------------|
| Off | 0% | 0% | 0% | 1.0x |
| Balanced (Zstd L3) | 40–70% | < 2% | < 5% | 3–5x |
| Aggressive (Zstd L5) | 55–80% | < 5% | < 10% | 4–8x |
| Legacy-LZ4 | 30–60% | < 2% | < 3% | 1.5–3x |

These are measured on typical OLTP workloads with mixed read/write patterns.
