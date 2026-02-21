# FalconDB Wire Compatibility Policy

## Overview

FalconDB maintains strict compatibility guarantees for on-disk formats, wire protocols, and configuration schemas to enable safe rolling upgrades and rollback.

---

## Version Numbering

```
MAJOR.MINOR.PATCH
  │      │     └── Bug fixes only, fully compatible
  │      └──────── New features, backward-compatible within same major
  └─────────────── Breaking changes (reserved for v1.0+)
```

| Version Range | Compatibility Guarantee |
|---------------|------------------------|
| Same `MAJOR.MINOR` | Fully compatible (rolling upgrade safe) |
| Same `MAJOR`, different `MINOR` | Compatible with warnings |
| Different `MAJOR` | Incompatible (requires migration) |

---

## WAL Format Compatibility

### Segment Header

Every WAL segment begins with an 8-byte header:

```
Offset  Size  Field
0       4     Magic bytes: "FALC" (0x46 0x41 0x4C 0x43)
4       4     WAL format version (little-endian u32)
```

| Constant | Value | Description |
|----------|-------|-------------|
| `WAL_MAGIC` | `b"FALC"` | Identifies a valid FalconDB WAL segment |
| `WAL_FORMAT_VERSION` | `3` | Current WAL record format version |
| `WAL_SEGMENT_HEADER_SIZE` | `8` | Header size in bytes |

### Version History

| WAL Version | FalconDB Version | Changes |
|-------------|-----------------|---------|
| 1 | v0.1.0 | Initial WAL format |
| 2 | v0.2.0 | Added `CoordinatorPrepare`, `CoordinatorCommit`, `CoordinatorAbort` |
| 3 | v0.3.0 | Added `CreateIndex`, `DropIndex` |

### Compatibility Rules

| Scenario | Behavior |
|----------|----------|
| Reader version = Writer version | Full compatibility |
| Reader version > Writer version | Compatible (newer reader can read older WAL) |
| Reader version < Writer version | **Warning**: may encounter unknown record types |
| Missing header (legacy segment) | Treated as version 0, parsed without header |

### Record Format

Each WAL record after the header:

```
Offset  Size      Field
0       4         Record length (little-endian u32)
4       4         CRC32 checksum of data
8       [length]  bincode-serialized WalRecord
```

---

## Snapshot Format Compatibility

### Version Header

Snapshots (checkpoint files) embed a `VersionHeader`:

```rust
pub struct VersionHeader {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub wal_format_version: u32,
    pub snapshot_format_version: u32,
}
```

| Constant | Value | Description |
|----------|-------|-------------|
| `SNAPSHOT_FORMAT_VERSION` | `1` | Current snapshot format |

### Compatibility Rules

| Scenario | Result |
|----------|--------|
| Same snapshot format version | `Compatible` |
| Snapshot from older version | `Compatible` (newer code reads older snapshots) |
| Snapshot from newer format version | `Incompatible` (cannot downgrade) |
| Snapshot from newer major version | `Incompatible` |

---

## PG Wire Protocol Compatibility

FalconDB implements the PostgreSQL wire protocol (v3.0):

| Feature | Compatibility |
|---------|--------------|
| Simple Query | Full (PG v7.4+) |
| Extended Query | Full (PG v7.4+) |
| COPY protocol | Full (text + CSV) |
| SSL/TLS handshake | Full (SSLRequest) |
| Auth: Trust | Full |
| Auth: MD5 | Full (PG v7.2+) |
| Auth: SCRAM-SHA-256 | Full (PG v10+) |
| Auth: Cleartext | Full |
| Cancel Request | Full |
| Parameter Status | Partial (server_version, server_encoding, client_encoding) |

### SHOW falcon.* Extensions

`SHOW falcon.*` commands are FalconDB-specific extensions. They return standard PG result sets (RowDescription + DataRow + CommandComplete) and are compatible with any PG client.

---

## Configuration Compatibility

### Deprecated Field Handling

FalconDB uses a `DeprecatedFieldChecker` to detect old config field names:

| Old Field | New Field | Deprecated Since | Removed In |
|-----------|-----------|-----------------|------------|
| `[cedar]` | `[server]` | v0.4.0 | v1.0.0 |
| `cedar_data_dir` | `storage.data_dir` | v0.4.0 | v1.0.0 |
| `wal.sync` | `wal.sync_mode` | v0.3.0 | v1.0.0 |
| `replication.master_endpoint` | `replication.primary_endpoint` | v0.2.0 | v1.0.0 |
| `replication.slave_mode` | `replication.role` | v0.2.0 | v1.0.0 |
| `storage.max_memory` | `memory.node_limit_bytes` | v0.6.0 | v1.0.0 |

**Behavior**: Deprecated fields emit a `WARN`-level log at startup but do **not** cause errors. This allows gradual migration.

### New Fields with Defaults

All new config fields added since v0.1.0 use `#[serde(default)]` to ensure old config files parse without error.

---

## Rolling Upgrade Policy

### Supported Upgrade Paths

| From | To | Method |
|------|----|--------|
| v0.x.y | v0.x.z (z > y) | In-place restart or rolling |
| v0.x.* | v0.(x+1).* | Rolling upgrade (one node at a time) |
| v0.x.* | v0.(x+2).* | **Not supported** — upgrade through intermediate version |

### Rolling Upgrade Procedure

1. **Pre-check**: `SHOW falcon.wire_compat` on all nodes
2. **Upgrade one node at a time** (replica first, primary last)
3. **Verify**: `SHOW falcon.health` on each upgraded node
4. **Monitor**: Check replication lag during upgrade
5. **Rollback**: If issues, downgrade the upgraded node (same-minor only)

### Verification

```bash
# Automated rolling upgrade smoke test
bash scripts/rolling_upgrade_smoke.sh
```

---

## Observability

| Command | Description |
|---------|-------------|
| `SHOW falcon.wire_compat` | Version, WAL format, snapshot format, min compatible version |
| `SHOW falcon.version` | Server version string |
| `SHOW falcon.compat` | General compatibility information |

---

## Invariants

1. **WAL segments always have a header** (v0.9.0+): Every new WAL segment starts with `FALC` + format version.
2. **Legacy segments are readable**: Segments without a header (pre-v0.9.0) are parsed starting at offset 0.
3. **Config never errors on deprecated fields**: Old field names produce warnings, not errors.
4. **Snapshot format is forward-incompatible**: A newer snapshot cannot be loaded by an older version.
5. **WAL format is backward-compatible**: A newer reader can always read an older WAL.
