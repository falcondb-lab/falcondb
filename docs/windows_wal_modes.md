# Windows WAL Modes (v1.0.7)

## Overview

FalconDB supports multiple WAL (Write-Ahead Log) backend modes on Windows, each offering different tradeoffs between latency determinism, throughput, and operational complexity.

## Available Backends

### 1. `file` (Default)

Standard file I/O using Rust's `std::fs::File`.

```toml
[wal]
backend = "file"
sync_mode = "fdatasync"
```

**Characteristics:**
- Cross-platform, works everywhere
- OS page cache buffering (non-deterministic flush timing)
- Fsync/fdatasync for durability
- Good for development and most production workloads

**When to use:** Default choice. Use unless you need tighter latency control.

### 2. `win_async_file` (v1.0.6+)

Windows IOCP (I/O Completion Port) with overlapped I/O.

```toml
[wal]
backend = "win_async_file"
no_buffering = true
group_commit_window_us = 200
```

**Characteristics:**
- Uses `FILE_FLAG_OVERLAPPED` for asynchronous writes
- IOCP completion port for deterministic flush acknowledgement
- Optional `FILE_FLAG_NO_BUFFERING` bypasses OS page cache
- `FILE_FLAG_WRITE_THROUGH` ensures disk-level durability
- Bounded completion wait (5-second timeout)

**When to use:** Production Windows deployments needing predictable commit latency.

### 3. Raw Disk WAL (Experimental, v1.0.7+)

Direct disk access bypassing the filesystem entirely.

**Status:** Tech preview. Not recommended for production.

```
falconctl wal init --raw \\.\PhysicalDriveX
```

**Characteristics:**
- Superblock + segment layout on raw partition
- Aligned buffer allocator (sector-aligned I/O)
- Crash recovery via checksum scanning
- Requires administrator privileges
- Requires dedicated disk/partition

**When to use:** Only for benchmarking and evaluation. Production support planned for v1.1+.

## Configuration Reference

### `[wal]` Section

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `backend` | string | `"file"` | WAL backend: `file` or `win_async_file` |
| `no_buffering` | bool | `false` | Enable `FILE_FLAG_NO_BUFFERING` (Windows only) |
| `group_commit_window_us` | u64 | `200` | Group commit coalescing window (microseconds) |
| `sync_mode` | string | `"fdatasync"` | Sync mode: `fsync`, `fdatasync`, `none` |
| `segment_size_bytes` | u64 | `67108864` | Max WAL segment size (64 MB) |
| `group_commit` | bool | `true` | Enable group commit batching |
| `flush_interval_us` | u64 | `1000` | Group commit flush interval |

### NO_BUFFERING Requirements

When `no_buffering = true`:
- **NTFS required** — does not work on FAT32/exFAT
- **Sector-aligned writes** — the WAL device handles alignment automatically
- **Default sector size: 4096 bytes** — padding is applied transparently
- Run `falcon doctor` to verify compatibility

### Group Commit Window

The `group_commit_window_us` parameter controls how long the WAL syncer waits to coalesce pending writes before flushing:

| Value | Behavior | Use Case |
|-------|----------|----------|
| `0` | Immediate flush | Lowest latency, highest fsync rate |
| `200` (default) | 200µs coalescing | Balanced: good batching, low latency |
| `500–1000` | Aggressive batching | High throughput, slightly higher p99 |

## Diagnostics

### `falcon doctor`

The WAL I/O diagnostics section checks:

```
WAL I/O Diagnostics:
  [check] IOCP support ... OK
  [check] FILE_FLAG_NO_BUFFERING on C:\ProgramData\FalconDB\data ... OK (sector_size=4096)
  [check] Disk alignment ... OK (NTFS, sector=4096)

  [info]  Recommended WAL mode: WalDeviceWinAsync (IOCP + NO_BUFFERING)
  [info]  Raw Disk WAL: not enabled (future v1.1+)
```

### Interpreting Results

| Check | OK | WARN/FAIL |
|-------|----|----|
| IOCP support | Windows detected | Non-Windows (use `file` backend) |
| NO_BUFFERING | NTFS, aligned writes work | Not NTFS or alignment check failed |
| Disk alignment | Sector size detected, NTFS | Unknown filesystem |

## Recommendations

### Development / CI
```toml
[wal]
backend = "file"
sync_mode = "none"
```

### Production (Linux/macOS)
```toml
[wal]
backend = "file"
sync_mode = "fdatasync"
group_commit_window_us = 200
```

### Production (Windows)
```toml
[wal]
backend = "win_async_file"
no_buffering = true
sync_mode = "fsync"
group_commit_window_us = 200
```

### High-Throughput (Windows)
```toml
[wal]
backend = "win_async_file"
no_buffering = true
sync_mode = "fsync"
group_commit_window_us = 500
```

## Risk Matrix

| Backend | Latency Determinism | Throughput | Complexity | Risk |
|---------|-------------------|------------|------------|------|
| `file` | Medium | Good | Low | Low |
| `win_async_file` | High | Good | Medium | Low |
| `win_async_file` + NO_BUFFERING | Highest | Best | Medium | Medium (NTFS required) |
| Raw Disk (experimental) | Highest | Best | High | High (data loss on misconfiguration) |

## Rollback

To revert from `win_async_file` to `file`:

1. Stop the server
2. Change `wal.backend = "file"` in `falcon.toml`
3. Start the server

WAL segment format is backend-independent. No data migration is needed.
