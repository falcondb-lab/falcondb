# Memory Compression — Hot/Cold Tiering (v1.0.7)

## Overview

FalconDB v1.0.7 introduces **Hot/Cold memory tiering** to reduce RSS without degrading OLTP tail latency. Old MVCC version payloads are migrated from the hot in-memory store to a compressed **Cold Store**, freeing heap memory while keeping data accessible.

**Design principle**: compression that increases p99 is a bug, not a feature.

## Architecture

```
┌─────────────────────────┐
│       Hot Store          │  ← Current MemTable / VersionChain
│  (uncompressed, fast)    │     MVCC headers always here
│  DashMap<PK, VersionChain>│
└────────┬────────────────┘
         │ Compactor migrates old payloads
         ▼
┌─────────────────────────┐
│       Cold Store         │  ← Append-only segments
│  (LZ4 compressed)        │     Block cache for reads
│  Segment[0..N]           │
└─────────────────────────┘
```

### Hot Store

- Unchanged from previous versions
- `VersionChain` holds `Arc<Version>` linked list
- Each `Version` contains: `created_by`, `commit_ts`, `data: Option<OwnedRow>`, `prev`
- **MVCC headers remain in hot memory at all times**

### Cold Store

- Append-only segments with configurable max size (default: 64 MB)
- Block format: `[codec:u8][original_len:u32][compressed_len:u32][data...]`
- Row payloads serialized via `bincode`, then compressed
- Cold-migrated versions reference payload via `ColdHandle { segment_id, offset, len }`

## Compression

| Setting | Values | Default |
|---------|--------|---------|
| `compression.enabled` | `true` / `false` | `true` |
| `compression.codec` | `lz4` / `none` | `lz4` |
| `block_cache_capacity` | bytes | 16 MB |
| `max_segment_size` | bytes | 64 MB |

### LZ4 (default)

- Uses `lz4_flex` (pure Rust, no C dependency)
- Block-level compression (each row independently)
- Typical ratio: 1.5–3x for text-heavy rows
- Decompress latency: sub-microsecond for small blocks

### None (fallback)

- Raw bytes, no compression overhead
- Use when CPU is the bottleneck or data is already compressed
- Toggle at runtime by changing config and restarting

## Cold Migration (Compactor)

The Compactor is a background task that migrates eligible version payloads to cold storage.

### Migration Conditions

A version is eligible when ALL of:
1. `version_age > min_version_age` (default: 300 timestamp units ≈ 5 min)
2. `pin_count == 0` (no active transaction references)
3. Version is committed (not uncommitted or aborted)

### Compactor Config

| Setting | Default | Description |
|---------|---------|-------------|
| `min_version_age` | 300 | Min age before migration |
| `batch_size` | 1000 | Max versions per sweep |
| `interval_ms` | 5000 | Sweep interval |

### Safety Properties

- **Idempotent**: re-migrating an already-cold version is a no-op
- **Non-blocking**: migration never holds MemTable locks for more than one key
- **Failure-safe**: if migration fails, the hot version remains intact and readable
- **MVCC-preserving**: only payloads move; headers, visibility, and chain structure are unchanged

## Cold Read Path

1. Check **block cache** (LRU, byte-limited) → cache hit: return immediately
2. Cache miss: read compressed block from segment
3. Decompress (LZ4 or passthrough)
4. Populate cache
5. Deserialize `OwnedRow`

Decompress runs on the calling thread but is bounded: LZ4 decompression is ~4 GB/s on modern CPUs. The block cache ensures repeated reads (e.g., index lookups) are sub-microsecond.

## Transaction Semantics

**Cold migration does NOT affect transaction semantics.**

- `read_committed()`, `read_for_txn()`, `is_visible()` — all work identically
- MVCC headers (`created_by`, `commit_ts`, chain pointers) never leave hot memory
- Active transactions pin versions, preventing premature migration
- Crash recovery: cold segments are append-only and self-describing

## Observability

### Metrics (available via `/admin/status` and `/metrics`)

| Metric | Description |
|--------|-------------|
| `memory.hot_bytes` | MVCC bytes in hot store |
| `memory.cold_bytes` | Compressed bytes in cold store |
| `memory.cold_segments` | Number of cold segments |
| `memory.compression_ratio` | Original / compressed (> 1.0 = effective) |
| `memory.cold_read_total` | Total cold reads |
| `memory.cold_decompress_avg_us` | Average decompress latency (µs) |
| `memory.cold_decompress_peak_us` | Peak decompress latency (µs) |
| `memory.cold_migrate_total` | Total rows migrated to cold |
| `memory.intern_hit_rate` | String intern pool hit rate |

### Diagnostic Commands

```
falcon doctor        # includes cold store health
```

## Rollback / Disable

To disable cold storage entirely:

1. Set `compression.enabled = false` in `falcon.toml`
2. Restart the server
3. All new data stays in hot memory
4. Existing cold data remains readable (segments are never deleted)

To switch codec:

1. Change `compression.codec = "none"` in `falcon.toml`
2. Restart — new migrations use the new codec
3. Existing compressed blocks remain readable (codec tag is per-block)

## String Intern Pool

Low-cardinality string columns (status codes, region names, enum values) benefit from interning:

- `StringInternPool` deduplicates identical strings
- Returns `InternId(u32)` — 4 bytes instead of a heap-allocated `String`
- Thread-safe with read-biased `RwLock`
- Observable via `intern_hit_rate` metric

## Performance Expectations

| Metric | Target | Notes |
|--------|--------|-------|
| Memory reduction | ≥ 30% | For typical OLTP with repetitive text columns |
| p99 read latency | ≤ 5% increase | With block cache warm |
| Cold read (cached) | < 1 ms p99 | Sub-microsecond typical |
| Cold read (uncached) | < 10 ms p99 | LZ4 decompress + deserialize |
| Store throughput | > 10K rows/sec | Append-only, no contention |
