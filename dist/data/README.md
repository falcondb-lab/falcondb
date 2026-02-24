# FalconDB Data Directory

This directory stores FalconDB's persistent data:

- **WAL segments** (`falcon_XXXXXX.wal`) — Write-Ahead Log for crash recovery
- **Snapshots** — Point-in-time database snapshots
- **SST files** — Sorted String Table files (if LSM mode is enabled)

## Important

- Do **not** delete files in this directory while FalconDB is running.
- Back up this directory regularly for disaster recovery.
- The data directory path is configured in `conf/falcon.toml` under `storage.data_dir`.
