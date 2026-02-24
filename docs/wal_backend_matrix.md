# FalconDB WAL Backend Matrix

## Overview

FalconDB v1.0.8 introduces a unified WAL backend selection via `wal_mode`. Users choose a mode; FalconDB handles the details.

## WAL Modes

| Mode | Config Value | Platform | Stability | Description |
|------|-------------|----------|-----------|-------------|
| Auto | `"auto"` | All | Stable | System selects best backend |
| POSIX | `"posix"` | All | Stable | Standard file I/O (fdatasync/fsync) |
| Windows Async | `"win_async"` | Windows | Stable | IOCP + optional NO_BUFFERING |
| Raw Experimental | `"raw_experimental"` | All | **EXPERIMENTAL** | Direct disk I/O, admin required |

## Configuration

```toml
# Top-level WAL mode (v1.0.8)
wal_mode = "auto"

# Detailed WAL config
[wal]
backend = "file"                    # Low-level backend (v1.0.7)
no_buffering = false                # Windows: FILE_FLAG_NO_BUFFERING
group_commit_window_us = 200        # Coalescing window
sync_mode = "fdatasync"             # fsync / fdatasync / none
```

### Relationship: `wal_mode` vs `wal.backend`

| `wal_mode` | Effective `wal.backend` | Notes |
|-----------|------------------------|-------|
| `auto` | Platform-dependent | Windows → `win_async_file`, Linux → `file` |
| `posix` | `file` | Standard POSIX file I/O |
| `win_async` | `win_async_file` | Requires Windows + NTFS |
| `raw_experimental` | `file` + raw flags | **Data loss risk on misconfiguration** |

`wal_mode` is the **user-facing** setting (v1.0.8). `wal.backend` is the **low-level** setting (v1.0.7). If both are set, `wal_mode` takes precedence.

## Mode Details

### Auto (Default)

```toml
wal_mode = "auto"
```

- **Windows**: selects `win_async` (IOCP-based, best throughput)
- **Linux/macOS**: selects `posix` (fdatasync, proven reliability)
- **Risk**: Low — always picks the safest performant option
- **Recommendation**: Use this unless you have specific requirements

### POSIX

```toml
wal_mode = "posix"
```

- Uses standard `open()` / `write()` / `fdatasync()` system calls
- Works on all platforms (Windows falls back to `fsync`)
- Sync modes: `fdatasync` (default), `fsync`, `none`
- **Risk**: Low
- **Best for**: Cross-platform compatibility, proven reliability

### Windows Async

```toml
wal_mode = "win_async"

[wal]
no_buffering = true     # Optional: bypass OS page cache
```

- Uses Windows IOCP (I/O Completion Ports) for overlapped writes
- Optional `FILE_FLAG_NO_BUFFERING` for direct I/O (requires NTFS, sector-aligned writes)
- `FlushFileBuffers` for durable sync
- **Risk**: Low-Medium (NO_BUFFERING requires NTFS + aligned writes)
- **Best for**: Windows deployments with high write throughput
- **Requirements**: Windows 10+ or Server 2016+, NTFS filesystem

### Raw Experimental

```toml
wal_mode = "raw_experimental"
```

- Direct disk I/O bypassing filesystem entirely
- Requires administrative privileges
- **Risk**: **HIGH** — data loss on misconfiguration
- **Best for**: Benchmarking only, never production
- **Requirements**: Admin privileges, dedicated disk partition

## Risk Matrix

| Mode | Data Loss Risk | Perf Overhead | Platform Req | Admin Req |
|------|---------------|---------------|-------------|-----------|
| Auto | None | Minimal | None | No |
| POSIX | None | Baseline | None | No |
| Win Async | None (with fsync) | -15% latency | Windows+NTFS | No |
| Win Async+NB | Low (alignment) | -30% latency | Windows+NTFS | No |
| Raw | **HIGH** | -50% latency | Dedicated disk | **Yes** |

## Diagnostics

Run `falcon doctor` to check WAL I/O capabilities:

```
=== WAL I/O Diagnostics ===
IOCP available:        true
NO_BUFFERING support:  true (sector size: 512)
Disk alignment:        512 bytes
Recommended mode:      win_async
```

## Rollback

To revert to safe defaults:

```toml
wal_mode = "auto"

[wal]
no_buffering = false
sync_mode = "fdatasync"
```

No restart required for `no_buffering` change. `wal_mode` change requires restart.
