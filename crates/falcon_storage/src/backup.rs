//! P2-4: Backup & Recovery infrastructure.
//!
//! Provides online full/incremental backup capabilities:
//! - Full snapshot backup (non-blocking, consistent point-in-time)
//! - Incremental WAL-based backup for PITR (Point-In-Time Recovery)
//! - Backup metadata with version, shard map, and tenant info
//! - Restore verification (integrity checksums)

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use falcon_common::types::Timestamp;

/// Backup type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    /// Full consistent snapshot.
    Full,
    /// Incremental WAL-based (since last full or incremental).
    Incremental,
}

impl std::fmt::Display for BackupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Incremental => write!(f, "incremental"),
        }
    }
}

/// Status of a backup operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupStatus {
    /// Backup is currently in progress.
    InProgress,
    /// Backup completed successfully.
    Completed,
    /// Backup failed.
    Failed,
}

impl std::fmt::Display for BackupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InProgress => write!(f, "in_progress"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Metadata for a completed or in-progress backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    /// Unique backup identifier.
    pub backup_id: u64,
    /// Type of backup.
    pub backup_type: BackupType,
    /// Current status.
    pub status: BackupStatus,
    /// Consistent snapshot timestamp (MVCC timestamp at backup start).
    pub snapshot_ts: Timestamp,
    /// WAL LSN at backup start (for incremental base).
    pub start_lsn: u64,
    /// WAL LSN at backup end (for incremental range).
    pub end_lsn: u64,
    /// Wall-clock time backup started (unix millis).
    pub started_at_ms: u64,
    /// Wall-clock time backup completed (unix millis, 0 if in-progress).
    pub completed_at_ms: u64,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Total bytes written.
    pub total_bytes: u64,
    /// Number of tables included.
    pub table_count: u32,
    /// Number of shards included.
    pub shard_count: u32,
    /// CRC32 checksum of the backup data (0 if not yet computed).
    pub checksum: u32,
    /// WAL format version at time of backup.
    pub wal_format_version: u32,
    /// Human-readable label (optional).
    pub label: String,
    /// Error message if status == Failed.
    pub error: Option<String>,
}

impl BackupMetadata {
    fn new(backup_id: u64, backup_type: BackupType, snapshot_ts: Timestamp, start_lsn: u64) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            backup_id,
            backup_type,
            status: BackupStatus::InProgress,
            snapshot_ts,
            start_lsn,
            end_lsn: 0,
            started_at_ms: now_ms,
            completed_at_ms: 0,
            duration_ms: 0,
            total_bytes: 0,
            table_count: 0,
            shard_count: 1,
            checksum: 0,
            wal_format_version: crate::wal::WAL_FORMAT_VERSION,
            label: String::new(),
            error: None,
        }
    }
}

/// PITR (Point-In-Time Recovery) target specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PitrTarget {
    /// Recover to a specific MVCC timestamp.
    Timestamp(Timestamp),
    /// Recover to a specific WAL LSN.
    Lsn(u64),
    /// Recover to the latest available state.
    Latest,
}

/// Result of a restore verification check.
#[derive(Debug, Clone)]
pub struct RestoreVerification {
    /// Whether the backup data passed integrity checks.
    pub checksum_valid: bool,
    /// Whether all referenced WAL segments are present (for PITR).
    pub wal_chain_complete: bool,
    /// Number of tables verified.
    pub tables_verified: u32,
    /// Number of rows verified.
    pub rows_verified: u64,
    /// Any warnings or issues found.
    pub warnings: Vec<String>,
}

/// Backup manager — orchestrates backup/restore operations.
///
/// In production, this would coordinate with the WAL writer and storage engine
/// to produce consistent snapshots. Here we provide the metadata management
/// and lifecycle tracking infrastructure.
pub struct BackupManager {
    next_backup_id: AtomicU64,
    /// History of backups (most recent first).
    history: Mutex<Vec<BackupMetadata>>,
    /// Maximum number of backup records to retain in history.
    history_capacity: usize,
    /// Total bytes backed up (monotonic counter).
    total_bytes_backed_up: AtomicU64,
    /// Total number of backups completed.
    total_backups_completed: AtomicU64,
    /// Total number of backups failed.
    total_backups_failed: AtomicU64,
}

impl BackupManager {
    pub fn new() -> Self {
        Self {
            next_backup_id: AtomicU64::new(1),
            history: Mutex::new(Vec::new()),
            history_capacity: 100,
            total_bytes_backed_up: AtomicU64::new(0),
            total_backups_completed: AtomicU64::new(0),
            total_backups_failed: AtomicU64::new(0),
        }
    }

    /// Start a full backup. Returns backup metadata with InProgress status.
    /// The caller is responsible for actually writing backup data and calling
    /// `complete_backup()` or `fail_backup()` when done.
    pub fn start_full_backup(&self, snapshot_ts: Timestamp, current_lsn: u64) -> BackupMetadata {
        let id = self.next_backup_id.fetch_add(1, Ordering::Relaxed);
        let meta = BackupMetadata::new(id, BackupType::Full, snapshot_ts, current_lsn);
        self.history.lock().push(meta.clone());
        meta
    }

    /// Start an incremental backup from a given LSN.
    pub fn start_incremental_backup(&self, snapshot_ts: Timestamp, start_lsn: u64) -> BackupMetadata {
        let id = self.next_backup_id.fetch_add(1, Ordering::Relaxed);
        let meta = BackupMetadata::new(id, BackupType::Incremental, snapshot_ts, start_lsn);
        self.history.lock().push(meta.clone());
        meta
    }

    /// Mark a backup as completed with final statistics.
    pub fn complete_backup(&self, backup_id: u64, end_lsn: u64, total_bytes: u64, table_count: u32, checksum: u32) -> bool {
        let mut history = self.history.lock();
        if let Some(meta) = history.iter_mut().find(|m| m.backup_id == backup_id) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            meta.status = BackupStatus::Completed;
            meta.end_lsn = end_lsn;
            meta.completed_at_ms = now_ms;
            meta.duration_ms = now_ms.saturating_sub(meta.started_at_ms);
            meta.total_bytes = total_bytes;
            meta.table_count = table_count;
            meta.checksum = checksum;
            self.total_bytes_backed_up.fetch_add(total_bytes, Ordering::Relaxed);
            self.total_backups_completed.fetch_add(1, Ordering::Relaxed);

            // Trim history if over capacity
            if history.len() > self.history_capacity {
                history.remove(0);
            }
            true
        } else {
            false
        }
    }

    /// Mark a backup as failed.
    pub fn fail_backup(&self, backup_id: u64, error: &str) -> bool {
        let mut history = self.history.lock();
        if let Some(meta) = history.iter_mut().find(|m| m.backup_id == backup_id) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            meta.status = BackupStatus::Failed;
            meta.completed_at_ms = now_ms;
            meta.duration_ms = now_ms.saturating_sub(meta.started_at_ms);
            meta.error = Some(error.to_string());
            self.total_backups_failed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get the latest completed full backup (for incremental base).
    pub fn latest_full_backup(&self) -> Option<BackupMetadata> {
        self.history.lock()
            .iter()
            .rev()
            .find(|m| m.backup_type == BackupType::Full && m.status == BackupStatus::Completed)
            .cloned()
    }

    /// Get backup history (most recent first).
    pub fn history(&self, limit: usize) -> Vec<BackupMetadata> {
        let history = self.history.lock();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get a specific backup by ID.
    pub fn get_backup(&self, backup_id: u64) -> Option<BackupMetadata> {
        self.history.lock().iter().find(|m| m.backup_id == backup_id).cloned()
    }

    /// Verify a backup's integrity (stub — in production would read and checksum the data).
    pub fn verify_backup(&self, backup_id: u64) -> Option<RestoreVerification> {
        let meta = self.get_backup(backup_id)?;
        Some(RestoreVerification {
            checksum_valid: meta.checksum != 0,
            wal_chain_complete: meta.status == BackupStatus::Completed,
            tables_verified: meta.table_count,
            rows_verified: 0, // would be populated by actual verification
            warnings: if meta.status == BackupStatus::Failed {
                vec![format!("Backup failed: {}", meta.error.as_deref().unwrap_or("unknown"))]
            } else {
                vec![]
            },
        })
    }

    /// Total bytes backed up across all backups.
    pub fn total_bytes_backed_up(&self) -> u64 {
        self.total_bytes_backed_up.load(Ordering::Relaxed)
    }

    /// Total number of completed backups.
    pub fn total_backups_completed(&self) -> u64 {
        self.total_backups_completed.load(Ordering::Relaxed)
    }

    /// Total number of failed backups.
    pub fn total_backups_failed(&self) -> u64 {
        self.total_backups_failed.load(Ordering::Relaxed)
    }
}

impl Default for BackupManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_full_backup() {
        let mgr = BackupManager::new();
        let meta = mgr.start_full_backup(Timestamp(100), 50);
        assert_eq!(meta.backup_id, 1);
        assert_eq!(meta.backup_type, BackupType::Full);
        assert_eq!(meta.status, BackupStatus::InProgress);
        assert_eq!(meta.snapshot_ts, Timestamp(100));
        assert_eq!(meta.start_lsn, 50);
        assert!(meta.started_at_ms > 0);
    }

    #[test]
    fn test_complete_backup() {
        let mgr = BackupManager::new();
        let meta = mgr.start_full_backup(Timestamp(100), 50);
        assert!(mgr.complete_backup(meta.backup_id, 200, 1024 * 1024, 5, 0xABCD));

        let completed = mgr.get_backup(meta.backup_id).unwrap();
        assert_eq!(completed.status, BackupStatus::Completed);
        assert_eq!(completed.end_lsn, 200);
        assert_eq!(completed.total_bytes, 1024 * 1024);
        assert_eq!(completed.table_count, 5);
        assert_eq!(completed.checksum, 0xABCD);
        assert!(completed.duration_ms >= 0);
        assert_eq!(mgr.total_backups_completed(), 1);
        assert_eq!(mgr.total_bytes_backed_up(), 1024 * 1024);
    }

    #[test]
    fn test_fail_backup() {
        let mgr = BackupManager::new();
        let meta = mgr.start_full_backup(Timestamp(100), 50);
        assert!(mgr.fail_backup(meta.backup_id, "disk full"));

        let failed = mgr.get_backup(meta.backup_id).unwrap();
        assert_eq!(failed.status, BackupStatus::Failed);
        assert_eq!(failed.error, Some("disk full".into()));
        assert_eq!(mgr.total_backups_failed(), 1);
    }

    #[test]
    fn test_incremental_backup() {
        let mgr = BackupManager::new();
        let full = mgr.start_full_backup(Timestamp(100), 0);
        mgr.complete_backup(full.backup_id, 100, 5000, 3, 0x1234);

        let incr = mgr.start_incremental_backup(Timestamp(200), 100);
        assert_eq!(incr.backup_type, BackupType::Incremental);
        assert_eq!(incr.start_lsn, 100);

        mgr.complete_backup(incr.backup_id, 200, 500, 3, 0x5678);
        assert_eq!(mgr.total_backups_completed(), 2);
    }

    #[test]
    fn test_latest_full_backup() {
        let mgr = BackupManager::new();
        assert!(mgr.latest_full_backup().is_none());

        let b1 = mgr.start_full_backup(Timestamp(100), 0);
        mgr.complete_backup(b1.backup_id, 100, 5000, 3, 0x1111);

        let b2 = mgr.start_full_backup(Timestamp(200), 100);
        mgr.complete_backup(b2.backup_id, 200, 6000, 4, 0x2222);

        let latest = mgr.latest_full_backup().unwrap();
        assert_eq!(latest.backup_id, b2.backup_id);
        assert_eq!(latest.checksum, 0x2222);
    }

    #[test]
    fn test_history_ordering() {
        let mgr = BackupManager::new();
        for i in 0..5 {
            let meta = mgr.start_full_backup(Timestamp(i * 100), i);
            mgr.complete_backup(meta.backup_id, (i + 1) * 100, 1000, 1, 0);
        }

        let history = mgr.history(3);
        assert_eq!(history.len(), 3);
        // Most recent first
        assert_eq!(history[0].backup_id, 5);
        assert_eq!(history[2].backup_id, 3);
    }

    #[test]
    fn test_verify_completed_backup() {
        let mgr = BackupManager::new();
        let meta = mgr.start_full_backup(Timestamp(100), 0);
        mgr.complete_backup(meta.backup_id, 100, 5000, 3, 0xABCD);

        let verification = mgr.verify_backup(meta.backup_id).unwrap();
        assert!(verification.checksum_valid);
        assert!(verification.wal_chain_complete);
        assert_eq!(verification.tables_verified, 3);
        assert!(verification.warnings.is_empty());
    }

    #[test]
    fn test_verify_failed_backup() {
        let mgr = BackupManager::new();
        let meta = mgr.start_full_backup(Timestamp(100), 0);
        mgr.fail_backup(meta.backup_id, "corruption detected");

        let verification = mgr.verify_backup(meta.backup_id).unwrap();
        assert!(!verification.wal_chain_complete);
        assert!(!verification.warnings.is_empty());
    }

    #[test]
    fn test_nonexistent_backup() {
        let mgr = BackupManager::new();
        assert!(mgr.get_backup(999).is_none());
        assert!(mgr.verify_backup(999).is_none());
        assert!(!mgr.complete_backup(999, 0, 0, 0, 0));
    }
}
