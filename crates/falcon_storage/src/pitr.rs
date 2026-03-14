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

use crate::object_store_backend::{LocalFsBackend, ObjectKey, ObjectStoreBackend};

/// WAL position (Log Sequence Number).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:08X}", self.0 >> 32, self.0 & 0xFFFFFFFF)
    }
}

impl Lsn {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);
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
            Self::Time(ms) => write!(f, "time:{ms}"),
            Self::Lsn(lsn) => write!(f, "lsn:{lsn}"),
            Self::Xid(xid) => write!(f, "xid:{xid}"),
            Self::RestorePoint(name) => write!(f, "restore_point:{name}"),
        }
    }
}

/// Metadata for a base backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseBackup {
    pub label: String,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub backup_path: PathBuf,
    pub size_bytes: u64,
    /// All WAL between start/end LSN is included.
    pub consistent: bool,
}

/// Metadata for an archived WAL segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedSegment {
    pub filename: String,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub earliest_time_ms: u64,
    pub latest_time_ms: u64,
    pub archive_path: PathBuf,
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
pub struct WalArchiver {
    /// Object storage backend (local FS / S3 / Azure).
    backend: Box<dyn ObjectStoreBackend>,
    /// Local directory path (used as fallback reference and for local backend).
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

impl std::fmt::Debug for WalArchiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalArchiver")
            .field("backend", &self.backend.uri())
            .field("archive_dir", &self.archive_dir)
            .field("enabled", &self.enabled)
            .field("segments_archived", &self.segments_archived)
            .finish()
    }
}

impl WalArchiver {
    /// Create a new WAL archiver with local filesystem backend.
    pub fn new(archive_dir: &Path) -> Self {
        Self {
            backend: Box::new(LocalFsBackend::new(archive_dir)),
            archive_dir: archive_dir.to_path_buf(),
            segments: BTreeMap::new(),
            restore_points: Vec::new(),
            base_backups: Vec::new(),
            enabled: true,
            segments_archived: 0,
            bytes_archived: 0,
        }
    }

    /// Create a WAL archiver with a custom object storage backend (S3, Azure, etc.).
    pub fn with_backend(archive_dir: &Path, backend: Box<dyn ObjectStoreBackend>) -> Self {
        Self {
            backend,
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
            backend: Box::new(LocalFsBackend::new(Path::new("/dev/null"))),
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
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Archive a completed WAL segment to the configured object store backend.
    ///
    /// Reads `source_path` from local disk and uploads via the backend.
    /// The `archive_path` field of the returned `ArchivedSegment` holds the object key.
    pub fn archive_segment(
        &mut self,
        source_path: &Path,
        filename: &str,
        start_lsn: Lsn,
        end_lsn: Lsn,
        size_bytes: u64,
    ) -> Result<ArchivedSegment, std::io::Error> {
        let data = std::fs::read(source_path)?;
        let key = ObjectKey::new(format!("wal/{filename}"));
        self.backend.put(&key, &data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // archive_path stores the object key as a PathBuf for backward compat.
        let archive_path = PathBuf::from(key.as_str());
        let segment = ArchivedSegment {
            filename: filename.to_owned(),
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
        tracing::debug!("PITR: archived {} → {}", filename, self.backend.uri());
        Ok(segment)
    }

    /// Create a named restore point.
    pub fn create_restore_point(&mut self, name: &str, lsn: Lsn) -> RestorePoint {
        let rp = RestorePoint {
            name: name.to_owned(),
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
            RecoveryTarget::Lsn(target_lsn) => self
                .base_backups
                .iter()
                .rev()
                .find(|b| b.start_lsn <= *target_lsn),
            RecoveryTarget::Time(target_ms) => self
                .base_backups
                .iter()
                .rev()
                .find(|b| b.start_time_ms <= *target_ms),
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
            RecoveryTarget::Lsn(lsn) => *lsn,
            _ => Lsn::MAX, // for time/xid/latest targets, scan all and stop when matched
        };

        self.segments
            .range(start..=end)
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

        let to_remove: Vec<Lsn> = self
            .segments
            .iter()
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
    pub const fn segment_count(&self) -> u64 {
        self.segments_archived
    }

    /// Total archived bytes.
    pub const fn total_bytes(&self) -> u64 {
        self.bytes_archived
    }

    /// Archive directory path.
    pub fn archive_dir(&self) -> &Path {
        &self.archive_dir
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// P1-10b: WAL Archiver Daemon — automatic background archiving
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for the automatic WAL archiving daemon.
#[derive(Debug, Clone)]
pub struct WalArchiverConfig {
    /// Archive destination directory (or S3/Azure URI prefix).
    pub archive_dir: PathBuf,
    /// How long to retain archived segments (default: 7 days).
    pub retention: Duration,
    /// Maximum number of in-flight archive operations.
    pub max_concurrent: usize,
    /// If true, compress segments with LZ4 before upload.
    pub compress: bool,
}

impl Default for WalArchiverConfig {
    fn default() -> Self {
        Self {
            archive_dir: PathBuf::from("/var/lib/falcondb/wal_archive"),
            retention: Duration::from_secs(7 * 24 * 3600),
            max_concurrent: 4,
            compress: false,
        }
    }
}

/// Background daemon that watches for completed WAL segments and archives them.
///
/// ## Trigger mechanism
/// `WalWriter::set_segment_rotate_callback` fires when a segment is sealed.
/// The callback sends the completed segment's (id, path, size) through a
/// bounded channel to this daemon thread, which then calls
/// `WalArchiver::archive_segment` asynchronously.
///
/// ## Shutdown
/// Call `WalArchiverDaemon::shutdown()` — sends a sentinel message and
/// joins the background thread.
pub struct WalArchiverDaemon {
    sender: std::sync::mpsc::SyncSender<ArchiverMsg>,
    handle: Option<std::thread::JoinHandle<()>>,
}

enum ArchiverMsg {
    /// A WAL segment completed rotation and is ready to archive.
    SegmentCompleted {
        segment_id: u64,
        wal_dir: PathBuf,
        size_bytes: u64,
    },
    /// Daemon shutdown signal.
    Shutdown,
}

impl WalArchiverDaemon {
    /// Spawn the archiver daemon and wire it to `wal_writer`'s rotation callback.
    ///
    /// Returns the daemon handle.  The caller must store it and call
    /// `shutdown()` before dropping the `WalWriter`.
    pub fn start(
        wal_writer: &std::sync::Arc<crate::wal::WalWriter>,
        config: WalArchiverConfig,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<ArchiverMsg>(64);

        // Wire the rotation callback into WalWriter.
        let tx_cb = tx.clone();
        let wal_dir = wal_writer.dir();
        wal_writer.set_segment_rotate_callback(std::sync::Arc::new(
            move |segment_id: u64, size_bytes: u64| {
                let _ = tx_cb.try_send(ArchiverMsg::SegmentCompleted {
                    segment_id,
                    wal_dir: wal_dir.clone(),
                    size_bytes,
                });
            },
        ));

        let handle = std::thread::Builder::new()
            .name("wal-archiver".into())
            .spawn(move || {
                let mut archiver = WalArchiver::new(&config.archive_dir);
                let mut last_retention_check = std::time::Instant::now();

                loop {
                    match rx.recv() {
                        Ok(ArchiverMsg::SegmentCompleted { segment_id, wal_dir, size_bytes }) => {
                            let filename = crate::wal::segment_filename(segment_id);
                            let source = wal_dir.join(&filename);

                            // Compute approximate LSN range from segment id.
                            let start_lsn = Lsn(segment_id * (64 * 1024 * 1024));
                            let end_lsn = Lsn(start_lsn.0 + size_bytes);

                            match archiver.archive_segment(&source, &filename, start_lsn, end_lsn, size_bytes) {
                                Ok(seg) => {
                                    tracing::info!(
                                        "WAL archiver: archived segment {} ({} bytes) → {:?}",
                                        filename, seg.size_bytes, seg.archive_path
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "WAL archiver: failed to archive {}: {}", filename, e
                                    );
                                }
                            }

                            // Periodic retention cleanup (every hour).
                            if last_retention_check.elapsed() > Duration::from_secs(3600) {
                                let removed = archiver.apply_retention(config.retention);
                                if removed > 0 {
                                    tracing::info!("WAL archiver: pruned {} expired segments", removed);
                                }
                                last_retention_check = std::time::Instant::now();
                            }
                        }
                        Ok(ArchiverMsg::Shutdown) | Err(_) => {
                            tracing::info!("WAL archiver: shutting down");
                            break;
                        }
                    }
                }
            })
            .expect("failed to spawn wal-archiver thread");

        Self { sender: tx, handle: Some(handle) }
    }

    /// Trigger archiving of a specific segment immediately (for testing or manual flush).
    pub fn archive_now(&self, segment_id: u64, wal_dir: PathBuf, size_bytes: u64) {
        let _ = self.sender.try_send(ArchiverMsg::SegmentCompleted {
            segment_id,
            wal_dir,
            size_bytes,
        });
    }

    /// Shutdown the daemon and wait for it to finish.
    pub fn shutdown(&mut self) {
        let _ = self.sender.send(ArchiverMsg::Shutdown);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for WalArchiverDaemon {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl std::fmt::Debug for WalArchiverDaemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WalArchiverDaemon(running={})", self.handle.is_some())
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
    pub const fn record_replayed(&mut self, lsn: Lsn) {
        self.records_replayed += 1;
        self.current_lsn = lsn;
    }

    /// Record progress after completing a WAL segment.
    pub const fn segment_completed(&mut self) {
        self.segments_replayed += 1;
    }

    /// Check if the recovery target has been reached.
    pub fn target_reached(&self, record_lsn: Lsn, record_time_ms: u64, record_xid: u64) -> bool {
        match &self.target {
            RecoveryTarget::Lsn(target) => record_lsn >= *target,
            RecoveryTarget::Time(target_ms) => record_time_ms >= *target_ms,
            RecoveryTarget::Xid(target_xid) => record_xid >= *target_xid,
            RecoveryTarget::Latest | RecoveryTarget::RestorePoint(_) => false, // never stop early; restore points handled separately
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
    pub const fn is_in_progress(&self) -> bool {
        matches!(self.status, RecoveryStatus::InProgress { .. })
    }
}

/// Plan for archive-based PITR recovery.
/// Produced by `WalArchiver::plan_recovery`, consumed by the engine.
#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub base_backup: BaseBackup,
    pub segments: Vec<ArchivedSegment>,
    pub target: RecoveryTarget,
}

impl WalArchiver {
    /// Build a recovery plan: select the best base backup and the WAL segments
    /// needed to reach the target. Returns `None` if no suitable backup exists.
    pub fn plan_recovery(&self, target: &RecoveryTarget) -> Option<RecoveryPlan> {
        let target = match target {
            RecoveryTarget::RestorePoint(name) => {
                let rp = self.find_restore_point(name)?;
                RecoveryTarget::Lsn(rp.lsn)
            }
            other => other.clone(),
        };

        let backup = self.find_base_backup(&target)?;
        let segments: Vec<ArchivedSegment> = self
            .segments_for_recovery(backup, &target)
            .into_iter()
            .cloned()
            .collect();

        Some(RecoveryPlan {
            base_backup: backup.clone(),
            segments,
            target,
        })
    }

    /// Stage archived WAL segments into `staging_dir` so the engine can replay them.
    /// Downloads each segment from the object store backend.
    /// Returns the number of segments staged.
    pub fn stage_segments(
        &self,
        plan: &RecoveryPlan,
        staging_dir: &Path,
    ) -> Result<usize, std::io::Error> {
        std::fs::create_dir_all(staging_dir)?;
        let mut count = 0;
        for seg in &plan.segments {
            let key = ObjectKey::new(format!("wal/{}", seg.filename));
            match self.backend.get(&key) {
                Ok(data) => {
                    let dest = staging_dir.join(&seg.filename);
                    std::fs::write(&dest, &data)?;
                    count += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "PITR: failed to fetch segment {} from {}: {}",
                        seg.filename,
                        self.backend.uri(),
                        e
                    );
                }
            }
        }
        tracing::info!("PITR: staged {} WAL segments into {:?}", count, staging_dir);
        Ok(count)
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
        let tmp = std::env::temp_dir().join("falcondb_pitr_test_archive");
        let _ = std::fs::remove_dir_all(&tmp);
        let wal_dir = tmp.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let archive_dir = tmp.join("archive");

        // Create a dummy WAL source file.
        let source = wal_dir.join("falcon_000001.wal");
        std::fs::write(&source, vec![0u8; 4096]).unwrap();

        let mut archiver = WalArchiver::new(&archive_dir);
        let seg = archiver
            .archive_segment(&source, "falcon_000001.wal", Lsn(0), Lsn(1000), 4096)
            .expect("archive should succeed");
        assert_eq!(seg.start_lsn, Lsn(0));
        assert_eq!(seg.end_lsn, Lsn(1000));
        assert_eq!(archiver.segment_count(), 1);
        assert_eq!(archiver.total_bytes(), 4096);
        // With local backend, the object is stored as wal/filename inside archive_dir.
        assert!(archive_dir.join("wal").join("falcon_000001.wal").exists());
        // archive_path holds the object key string.
        assert_eq!(seg.archive_path.to_str().unwrap(), "wal/falcon_000001.wal");

        let _ = std::fs::remove_dir_all(&tmp);
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
        let wal_src_dir = std::env::temp_dir().join("falcondb_pitr_test_recovery_src");
        let _ = std::fs::remove_dir_all(&wal_src_dir);
        std::fs::create_dir_all(&wal_src_dir).unwrap();
        for i in 0..5u64 {
            let fname = format!("seg_{}.wal", i);
            let src = wal_src_dir.join(&fname);
            std::fs::write(&src, vec![0u8; 4096]).unwrap();
            let start = Lsn(i * 1000);
            let end = Lsn((i + 1) * 1000 - 1);
            archiver
                .archive_segment(&src, &fname, start, end, 4096)
                .unwrap();
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
        assert_eq!(
            RecoveryTarget::RestorePoint("v1".into()).to_string(),
            "restore_point:v1"
        );
    }

    #[test]
    fn test_plan_recovery_by_lsn() {
        let tmp = std::env::temp_dir().join("falcondb_pitr_test_plan");
        let _ = std::fs::remove_dir_all(&tmp);
        let wal_src = tmp.join("wal_src");
        std::fs::create_dir_all(&wal_src).unwrap();
        let archive = tmp.join("archive");

        let mut archiver = WalArchiver::new(&archive);
        archiver.register_base_backup(BaseBackup {
            label: "base".into(),
            start_lsn: Lsn(0),
            end_lsn: Lsn(500),
            start_time_ms: 0,
            end_time_ms: 0,
            backup_path: PathBuf::new(),
            size_bytes: 0,
            consistent: true,
        });
        for i in 0..3u64 {
            let fname = format!("seg_{i}.wal");
            let src = wal_src.join(&fname);
            std::fs::write(&src, vec![0u8; 128]).unwrap();
            archiver
                .archive_segment(&src, &fname, Lsn(i * 1000), Lsn((i + 1) * 1000 - 1), 128)
                .unwrap();
        }

        let plan = archiver.plan_recovery(&RecoveryTarget::Lsn(Lsn(1500))).unwrap();
        assert_eq!(plan.base_backup.label, "base");
        assert_eq!(plan.segments.len(), 2); // seg_0, seg_1

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_plan_recovery_restore_point() {
        let archive = PathBuf::from("/tmp/pitr_rp_test");
        let mut archiver = WalArchiver::new(&archive);
        archiver.register_base_backup(BaseBackup {
            label: "base".into(),
            start_lsn: Lsn(0),
            end_lsn: Lsn(500),
            start_time_ms: 0,
            end_time_ms: 0,
            backup_path: PathBuf::new(),
            size_bytes: 0,
            consistent: true,
        });
        archiver.create_restore_point("before_deploy", Lsn(300));

        let plan = archiver
            .plan_recovery(&RecoveryTarget::RestorePoint("before_deploy".into()))
            .unwrap();
        // RestorePoint resolves to Lsn(300)
        assert!(matches!(plan.target, RecoveryTarget::Lsn(Lsn(300))));
    }

    #[test]
    fn test_plan_recovery_no_backup_returns_none() {
        let archive = PathBuf::from("/tmp/pitr_no_backup");
        let archiver = WalArchiver::new(&archive);
        assert!(archiver.plan_recovery(&RecoveryTarget::Latest).is_none());
    }

    #[test]
    fn test_stage_segments() {
        let tmp = std::env::temp_dir().join("falcondb_pitr_test_stage");
        let _ = std::fs::remove_dir_all(&tmp);
        let wal_src = tmp.join("wal_src");
        std::fs::create_dir_all(&wal_src).unwrap();
        let archive = tmp.join("archive");
        let staging = tmp.join("staging");

        let mut archiver = WalArchiver::new(&archive);
        archiver.register_base_backup(BaseBackup {
            label: "base".into(),
            start_lsn: Lsn(0),
            end_lsn: Lsn(500),
            start_time_ms: 0,
            end_time_ms: 0,
            backup_path: PathBuf::new(),
            size_bytes: 0,
            consistent: true,
        });
        for i in 0..2u64 {
            let fname = format!("seg_{i}.wal");
            let src = wal_src.join(&fname);
            std::fs::write(&src, vec![0xABu8; 64]).unwrap();
            archiver
                .archive_segment(&src, &fname, Lsn(i * 1000), Lsn((i + 1) * 1000 - 1), 64)
                .unwrap();
        }

        let plan = archiver.plan_recovery(&RecoveryTarget::Latest).unwrap();
        let count = archiver.stage_segments(&plan, &staging).unwrap();
        assert_eq!(count, 2);
        assert!(staging.join("seg_0.wal").exists());
        assert!(staging.join("seg_1.wal").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
