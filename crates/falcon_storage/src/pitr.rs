//! Point-in-Time Recovery (PITR) — WAL-based recovery to any target timestamp.
//!
//! Enterprise feature for data protection and disaster recovery.
//!
//! Architecture:
//! - **WAL Archiving**: Completed WAL segments are copied to an archive directory
//!   (local filesystem or object store).
//! - **Base Backup**: A consistent snapshot of the data directory at a known LSN.
//! - **Recovery**: Replay WAL from the base backup up to a target timestamp or LSN.
//!
//! Recovery modes:
//! - **Latest**: Replay all available WAL (crash recovery).
//! - **Target Time**: Replay WAL until a specific wall-clock timestamp.
//! - **Target LSN**: Replay WAL until a specific WAL position.
//! - **Target XID**: Replay WAL until a specific transaction commits.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// WAL position (Log Sequence Number).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:08X}", self.0 >> 32, self.0 & 0xFFFFFFFF)
    }
}

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);
    pub const MAX: Lsn = Lsn(u64::MAX);
}

/// Recovery target specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryTarget {
    /// Replay all available WAL (full crash recovery).
    Latest,
    /// Replay until the given wall-clock timestamp (inclusive).
    Time(u64), // unix millis
    /// Replay until the given LSN (inclusive).
    Lsn(Lsn),
    /// Replay until the given transaction ID commits (inclusive).
    Xid(u64),
    /// Replay until the named restore point (created via `pg_create_restore_point`).
    RestorePoint(String),
}

impl fmt::Display for RecoveryTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => write!(f, "latest"),
            Self::Time(ms) => write!(f, "time:{}", ms),
            Self::Lsn(lsn) => write!(f, "lsn:{}", lsn),
            Self::Xid(xid) => write!(f, "xid:{}", xid),
            Self::RestorePoint(name) => write!(f, "restore_point:{}", name),
        }
    }
}

/// Metadata for a base backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseBackup {
    /// Unique backup label.
    pub label: String,
    /// LSN at the start of the backup.
    pub start_lsn: Lsn,
    /// LSN at the end of the backup (after pg_stop_backup equivalent).
    pub end_lsn: Lsn,
    /// Wall-clock time when the backup started.
    pub start_time_ms: u64,
    /// Wall-clock time when the backup finished.
    pub end_time_ms: u64,
    /// Path to the backup data directory.
    pub backup_path: PathBuf,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Whether the backup is consistent (all WAL between start/end is included).
    pub consistent: bool,
}

/// Metadata for an archived WAL segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedSegment {
    /// Segment filename (e.g., "falcon_000001.wal").
    pub filename: String,
    /// First LSN in this segment.
    pub start_lsn: Lsn,
    /// Last LSN in this segment.
    pub end_lsn: Lsn,
    /// Wall-clock time range covered by records in this segment.
    pub earliest_time_ms: u64,
    pub latest_time_ms: u64,
    /// Archive path.
    pub archive_path: PathBuf,
    /// Segment size in bytes.
    pub size_bytes: u64,
}

/// A named restore point (created via SQL: `SELECT pg_create_restore_point('name')`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestorePoint {
    pub name: String,
    pub lsn: Lsn,
    pub created_at_ms: u64,
}

/// Status of a PITR operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStatus {
    /// Not in recovery.
    Normal,
    /// Recovery in progress.
    InProgress {
        target: String,
        current_lsn: Lsn,
        segments_replayed: u64,
        records_replayed: u64,
    },
    /// Recovery completed successfully.
    Completed {
        target: String,
        final_lsn: Lsn,
        duration_ms: u64,
    },
    /// Recovery failed.
    Failed {
        target: String,
        error: String,
        last_lsn: Lsn,
    },
}

/// WAL Archive Manager — manages WAL segment archiving for PITR.
#[derive(Debug)]
pub struct WalArchiver {
    /// Directory where archived WAL segments are stored.
    archive_dir: PathBuf,
    /// Index of archived segments (ordered by start_lsn).
    segments: BTreeMap<Lsn, ArchivedSegment>,
    /// Named restore points.
    restore_points: Vec<RestorePoint>,
    /// Base backups (ordered by start_lsn).
    base_backups: Vec<BaseBackup>,
    /// Current archiving status.
    enabled: bool,
    /// Number of segments archived.
    segments_archived: u64,
    /// Total bytes archived.
    bytes_archived: u64,
}

impl WalArchiver {
    /// Create a new WAL archiver.
    pub fn new(archive_dir: &Path) -> Self {
        Self {
            archive_dir: archive_dir.to_path_buf(),
            segments: BTreeMap::new(),
            restore_points: Vec::new(),
            base_backups: Vec::new(),
            enabled: true,
            segments_archived: 0,
            bytes_archived: 0,
        }
    }

    /// Create a disabled archiver (archiving off).
    pub fn disabled() -> Self {
        Self {
            archive_dir: PathBuf::new(),
            segments: BTreeMap::new(),
            restore_points: Vec::new(),
            base_backups: Vec::new(),
            enabled: false,
            segments_archived: 0,
            bytes_archived: 0,
        }
    }

    /// Whether archiving is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Archive a completed WAL segment.
    pub fn archive_segment(
        &mut self,
        filename: &str,
        start_lsn: Lsn,
        end_lsn: Lsn,
        size_bytes: u64,
    ) -> ArchivedSegment {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let archive_path = self.archive_dir.join(filename);
        let segment = ArchivedSegment {
            filename: filename.to_string(),
            start_lsn,
            end_lsn,
            earliest_time_ms: now_ms,
            latest_time_ms: now_ms,
            archive_path,
            size_bytes,
        };

        self.segments.insert(start_lsn, segment.clone());
        self.segments_archived += 1;
        self.bytes_archived += size_bytes;
        segment
    }

    /// Create a named restore point.
    pub fn create_restore_point(&mut self, name: &str, lsn: Lsn) -> RestorePoint {
        let rp = RestorePoint {
            name: name.to_string(),
            lsn,
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.restore_points.push(rp.clone());
        rp
    }

    /// Register a base backup.
    pub fn register_base_backup(&mut self, backup: BaseBackup) {
        self.base_backups.push(backup);
    }

    /// Find the best base backup for recovery to a target.
    pub fn find_base_backup(&self, target: &RecoveryTarget) -> Option<&BaseBackup> {
        match target {
            RecoveryTarget::Latest => self.base_backups.last(),
            RecoveryTarget::Lsn(target_lsn) => {
                self.base_backups.iter().rev()
                    .find(|b| b.start_lsn <= *target_lsn)
            }
            RecoveryTarget::Time(target_ms) => {
                self.base_backups.iter().rev()
                    .find(|b| b.start_time_ms <= *target_ms)
            }
            RecoveryTarget::Xid(_) | RecoveryTarget::RestorePoint(_) => {
                // For XID/restore point, use the latest backup and scan forward
                self.base_backups.last()
            }
        }
    }

    /// Find WAL segments needed to recover from base_backup to target.
    pub fn segments_for_recovery(
        &self,
        base_backup: &BaseBackup,
        target: &RecoveryTarget,
    ) -> Vec<&ArchivedSegment> {
        let start = base_backup.start_lsn;
        let end = match target {
            RecoveryTarget::Latest => Lsn::MAX,
            RecoveryTarget::Lsn(lsn) => *lsn,
            _ => Lsn::MAX, // for time/xid targets, we need to scan all and stop when matched
        };

        self.segments.range(start..=end)
            .map(|(_, seg)| seg)
            .collect()
    }

    /// Find a restore point by name.
    pub fn find_restore_point(&self, name: &str) -> Option<&RestorePoint> {
        self.restore_points.iter().find(|rp| rp.name == name)
    }

    /// Calculate retention: remove archived segments older than the retention period.
    pub fn apply_retention(&mut self, retention: Duration) -> u64 {
        let cutoff_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = cutoff_ms.saturating_sub(retention.as_millis() as u64);

        let to_remove: Vec<Lsn> = self.segments.iter()
            .filter(|(_, seg)| seg.latest_time_ms < cutoff)
            .map(|(lsn, _)| *lsn)
            .collect();

        let count = to_remove.len() as u64;
        for lsn in to_remove {
            self.segments.remove(&lsn);
        }
        count
    }

    /// List all archived segments.
    pub fn list_segments(&self) -> Vec<&ArchivedSegment> {
        self.segments.values().collect()
    }

    /// List all base backups.
    pub fn list_base_backups(&self) -> Vec<&BaseBackup> {
        self.base_backups.iter().collect()
    }

    /// List all restore points.
    pub fn list_restore_points(&self) -> Vec<&RestorePoint> {
        self.restore_points.iter().collect()
    }

    /// Total number of archived segments.
    pub fn segment_count(&self) -> u64 {
        self.segments_archived
    }

    /// Total archived bytes.
    pub fn total_bytes(&self) -> u64 {
        self.bytes_archived
    }

    /// Archive directory path.
    pub fn archive_dir(&self) -> &Path {
        &self.archive_dir
    }
}

/// Recovery Executor — coordinates PITR recovery from base backup + WAL.
#[derive(Debug)]
pub struct RecoveryExecutor {
    pub status: RecoveryStatus,
    pub target: RecoveryTarget,
    pub records_replayed: u64,
    pub segments_replayed: u64,
    pub current_lsn: Lsn,
    pub start_time_ms: u64,
}

impl RecoveryExecutor {
    pub fn new(target: RecoveryTarget) -> Self {
        Self {
            status: RecoveryStatus::Normal,
            target,
            records_replayed: 0,
            segments_replayed: 0,
            current_lsn: Lsn::ZERO,
            start_time_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Begin recovery.
    pub fn begin(&mut self) {
        self.status = RecoveryStatus::InProgress {
            target: self.target.to_string(),
            current_lsn: self.current_lsn,
            segments_replayed: 0,
            records_replayed: 0,
        };
    }

    /// Record progress after replaying a WAL record.
    pub fn record_replayed(&mut self, lsn: Lsn) {
        self.records_replayed += 1;
        self.current_lsn = lsn;
    }

    /// Record progress after completing a WAL segment.
    pub fn segment_completed(&mut self) {
        self.segments_replayed += 1;
    }

    /// Check if the recovery target has been reached.
    pub fn target_reached(&self, record_lsn: Lsn, record_time_ms: u64, record_xid: u64) -> bool {
        match &self.target {
            RecoveryTarget::Latest => false, // never stop early
            RecoveryTarget::Lsn(target) => record_lsn >= *target,
            RecoveryTarget::Time(target_ms) => record_time_ms >= *target_ms,
            RecoveryTarget::Xid(target_xid) => record_xid >= *target_xid,
            RecoveryTarget::RestorePoint(_) => false, // handled separately
        }
    }

    /// Mark recovery as completed.
    pub fn complete(&mut self) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.status = RecoveryStatus::Completed {
            target: self.target.to_string(),
            final_lsn: self.current_lsn,
            duration_ms: now_ms.saturating_sub(self.start_time_ms),
        };
    }

    /// Mark recovery as failed.
    pub fn fail(&mut self, error: String) {
        self.status = RecoveryStatus::Failed {
            target: self.target.to_string(),
            error,
            last_lsn: self.current_lsn,
        };
    }

    /// Whether recovery is still in progress.
    pub fn is_in_progress(&self) -> bool {
        matches!(self.status, RecoveryStatus::InProgress { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_lsn_display() {
        assert_eq!(format!("{}", Lsn(0x100000080)), "1/00000080");
        assert_eq!(format!("{}", Lsn(0)), "0/00000000");
    }

    #[test]
    fn test_lsn_ordering() {
        assert!(Lsn(100) < Lsn(200));
        assert!(Lsn(200) > Lsn(100));
        assert_eq!(Lsn(100), Lsn(100));
    }

    #[test]
    fn test_archive_segment() {
        let dir = PathBuf::from("/tmp/wal_archive");
        let mut archiver = WalArchiver::new(&dir);
        let seg = archiver.archive_segment("falcon_000001.wal", Lsn(0), Lsn(1000), 4096);
        assert_eq!(seg.start_lsn, Lsn(0));
        assert_eq!(seg.end_lsn, Lsn(1000));
        assert_eq!(archiver.segment_count(), 1);
        assert_eq!(archiver.total_bytes(), 4096);
    }

    #[test]
    fn test_create_restore_point() {
        let dir = PathBuf::from("/tmp/wal_archive");
        let mut archiver = WalArchiver::new(&dir);
        let rp = archiver.create_restore_point("before_migration", Lsn(5000));
        assert_eq!(rp.name, "before_migration");
        assert_eq!(rp.lsn, Lsn(5000));
        assert!(archiver.find_restore_point("before_migration").is_some());
        assert!(archiver.find_restore_point("nonexistent").is_none());
    }

    #[test]
    fn test_find_base_backup() {
        let dir = PathBuf::from("/tmp/wal_archive");
        let mut archiver = WalArchiver::new(&dir);
        archiver.register_base_backup(BaseBackup {
            label: "backup_1".into(),
            start_lsn: Lsn(0),
            end_lsn: Lsn(1000),
            start_time_ms: 1000,
            end_time_ms: 2000,
            backup_path: PathBuf::from("/backups/1"),
            size_bytes: 1_000_000,
            consistent: true,
        });
        archiver.register_base_backup(BaseBackup {
            label: "backup_2".into(),
            start_lsn: Lsn(5000),
            end_lsn: Lsn(6000),
            start_time_ms: 5000,
            end_time_ms: 6000,
            backup_path: PathBuf::from("/backups/2"),
            size_bytes: 2_000_000,
            consistent: true,
        });

        let latest = archiver.find_base_backup(&RecoveryTarget::Latest);
        assert_eq!(latest.unwrap().label, "backup_2");

        let by_lsn = archiver.find_base_backup(&RecoveryTarget::Lsn(Lsn(3000)));
        assert_eq!(by_lsn.unwrap().label, "backup_1");

        let by_time = archiver.find_base_backup(&RecoveryTarget::Time(5500));
        assert_eq!(by_time.unwrap().label, "backup_2");
    }

    #[test]
    fn test_segments_for_recovery() {
        let dir = PathBuf::from("/tmp/wal_archive");
        let mut archiver = WalArchiver::new(&dir);
        for i in 0..5 {
            let start = Lsn(i * 1000);
            let end = Lsn((i + 1) * 1000 - 1);
            archiver.archive_segment(&format!("seg_{}.wal", i), start, end, 4096);
        }
        let backup = BaseBackup {
            label: "base".into(),
            start_lsn: Lsn(1000),
            end_lsn: Lsn(1500),
            start_time_ms: 0,
            end_time_ms: 0,
            backup_path: PathBuf::new(),
            size_bytes: 0,
            consistent: true,
        };

        let segs = archiver.segments_for_recovery(&backup, &RecoveryTarget::Lsn(Lsn(3500)));
        assert_eq!(segs.len(), 3); // segments 1, 2, 3
    }

    #[test]
    fn test_recovery_executor_lifecycle() {
        let mut exec = RecoveryExecutor::new(RecoveryTarget::Lsn(Lsn(5000)));
        assert!(!exec.is_in_progress());

        exec.begin();
        assert!(exec.is_in_progress());

        exec.record_replayed(Lsn(1000));
        exec.record_replayed(Lsn(2000));
        exec.segment_completed();
        assert_eq!(exec.records_replayed, 2);
        assert_eq!(exec.segments_replayed, 1);

        assert!(!exec.target_reached(Lsn(3000), 0, 0));
        assert!(exec.target_reached(Lsn(5000), 0, 0));

        exec.complete();
        assert!(!exec.is_in_progress());
        assert!(matches!(exec.status, RecoveryStatus::Completed { .. }));
    }

    #[test]
    fn test_recovery_target_time() {
        let exec = RecoveryExecutor::new(RecoveryTarget::Time(10_000));
        assert!(!exec.target_reached(Lsn(0), 5_000, 0));
        assert!(exec.target_reached(Lsn(0), 10_000, 0));
        assert!(exec.target_reached(Lsn(0), 15_000, 0));
    }

    #[test]
    fn test_recovery_target_xid() {
        let exec = RecoveryExecutor::new(RecoveryTarget::Xid(42));
        assert!(!exec.target_reached(Lsn(0), 0, 30));
        assert!(exec.target_reached(Lsn(0), 0, 42));
    }

    #[test]
    fn test_disabled_archiver() {
        let archiver = WalArchiver::disabled();
        assert!(!archiver.is_enabled());
    }

    #[test]
    fn test_recovery_target_display() {
        assert_eq!(RecoveryTarget::Latest.to_string(), "latest");
        assert_eq!(RecoveryTarget::Lsn(Lsn(100)).to_string(), "lsn:0/00000064");
        assert_eq!(RecoveryTarget::Xid(42).to_string(), "xid:42");
        assert_eq!(RecoveryTarget::RestorePoint("v1".into()).to_string(), "restore_point:v1");
    }
}
