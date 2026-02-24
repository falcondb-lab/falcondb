# FalconDB Compression Profiles

## Overview

FalconDB v1.0.8 introduces **compression profiles** — a single configuration knob that controls the entire hot/cold memory tiering system. Users don't need to understand cold store internals.

## Profiles

| Profile | Config Value | Cold Migration | Codec | Cache Size | Use Case |
|---------|-------------|---------------|-------|-----------|----------|
| Off | `"off"` | Disabled | None | 0 | Maximum throughput, no compression overhead |
| Balanced | `"balanced"` | 5 min age | LZ4 | 16 MB | Default — good memory savings, low CPU |
| Aggressive | `"aggressive"` | 1 min age | LZ4 | 64 MB | Maximum memory savings, higher CPU |

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
- LZ4 compression (fast, ~60% ratio on typical data)
- 16 MB block cache for read amortization
- Background compactor runs every 5 seconds
- **Best for**: Most workloads — saves 30–60% memory with minimal latency impact

Effective settings:
| Parameter | Value |
|-----------|-------|
| `compression_enabled` | true |
| `min_version_age` | 300 (~5 min) |
| `codec` | lz4 |
| `block_cache_capacity` | 16 MB |
| `compactor_batch_size` | 1000 |
| `compactor_interval_ms` | 5000 |

### Aggressive

```toml
compression_profile = "aggressive"
```

- Old MVCC versions (>1 min) migrated to cold store
- LZ4 compression
- 64 MB block cache (larger to handle more cold reads)
- Background compactor runs every 1 second with larger batches
- **Best for**: Memory-constrained environments, large datasets, analytical workloads

Effective settings:
| Parameter | Value |
|-----------|-------|
| `compression_enabled` | true |
| `min_version_age` | 60 (~1 min) |
| `codec` | lz4 |
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

| Profile | Memory Savings | p99 Latency Impact | CPU Overhead |
|---------|---------------|-------------------|-------------|
| Off | 0% | 0% | 0% |
| Balanced | 30–60% | < 2% | < 3% |
| Aggressive | 50–75% | < 5% | < 8% |

These are measured on typical OLTP workloads with mixed read/write patterns.
