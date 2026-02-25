# FalconDB Backup & Restore Guide

## Overview
FalconDB v1.1.0 provides enterprise backup/restore with full, incremental,
and point-in-time recovery (PITR) capabilities. Backups target local storage
or object stores (S3, OSS).

## Backup Types

| Type | Content | RPO | Use Case |
|------|---------|-----|----------|
| Full | Data + metadata + cold segments | N/A | Baseline, disaster recovery |
| Incremental | WAL / change log since last full | Minutes | Hourly schedule |
| WAL Archive | Continuous WAL streaming | Seconds | Real-time PITR |

## Storage Targets

### Local Filesystem
```
BackupTarget::Local { path: "/backup/falcondb" }
```

### Amazon S3
```
BackupTarget::S3 {
    bucket: "prod-backups",
    prefix: "falcondb/daily/",
    region: "us-east-1",
}
```

### Alibaba OSS
```
BackupTarget::Oss {
    bucket: "prod-backups",
    prefix: "falcondb/daily/",
    endpoint: "oss-cn-hangzhou.aliyuncs.com",
}
```

## Backup Workflow

### Schedule a Full Backup
```rust
let job_id = orchestrator.schedule_backup(
    EnterpriseBackupType::Full,
    BackupTarget::S3 { bucket: "...", prefix: "...", region: "..." },
    "admin",
);
```

### Lifecycle: Scheduled → Running → Completed/Failed
```rust
orchestrator.start_backup(job_id, current_wal_lsn);
// ... backup data ...
orchestrator.complete_backup(job_id, bytes_written, table_count, end_lsn, checksum);
// or on failure:
orchestrator.fail_backup(job_id, "disk full");
```

## Restore Workflow

### Restore Types
| Type | Description |
|------|-------------|
| Latest | Restore to the latest available state |
| ToLsn | Restore to a specific WAL LSN |
| ToTimestamp | Point-in-time recovery (PITR) |
| NewCluster | Restore into a brand-new cluster |

### Schedule a PITR Restore
```rust
let restore_id = orchestrator.schedule_restore(
    BackupTarget::S3 { ... },
    backup_id,
    RestoreType::ToTimestamp,
    None,                    // target_lsn
    Some(1718000000),        // target_timestamp (Unix epoch)
);
```

### Monitoring Restore Progress
```rust
let job = orchestrator.get_restore_job(restore_id);
println!("Status: {}", job.status);
println!("Bytes read: {}", job.bytes_read);
println!("Tables restored: {}", job.tables_restored);
```

## Retention Policy
- Configurable `retention_days` (default: 30)
- Backup history tracked with metadata (LSN range, byte count, checksum)
- `latest_full_backup()` returns the most recent completed full backup

## RPO / RTO Guidelines

| Scenario | RPO | RTO |
|----------|-----|-----|
| Full backup only | Hours | Minutes (restore time) |
| Full + hourly incremental | ~1 hour | Minutes |
| Full + WAL archive | Seconds | Minutes + WAL replay |

## Audit Integration
All backup/restore operations are recorded in the enterprise audit log:
- Category: `BACKUP_RESTORE`
- Includes: job ID, type, target, byte count, duration

## Metrics
- `backups_started` — total backup jobs scheduled
- `backups_completed` — successfully completed
- `backups_failed` — failed backup jobs
- `restores_started` / `restores_completed`
- `total_bytes_backed_up` — cumulative bytes
