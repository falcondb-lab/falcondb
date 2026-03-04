//! # Module Status: PRODUCTION
//! Write-Ahead Log (WAL) — crash-safe durability for all committed transactions.
//! Core production path: every commit appends to the WAL before client ACK.
//!
//! ## Golden Path (OLTP Write — WAL segment)
//! ```text
//! StorageEngine.commit_txn()
//!   → WalWriter.append(WalRecord::InsertRow | UpdateRow | DeleteRow)
//!   → WalWriter.append(WalRecord::CommitTxn { txn_id, commit_ts })
//!   → fsync (per SyncMode / CommitPolicy)
//!   → WAL observer callback → Replication stream
//! ```
//!
//! ## Invariants (enforced, not advisory)
//! - WAL-1: Monotonic LSN — every record gets a strictly increasing LSN
//! - WAL-2: Idempotent replay — replaying a record twice = replaying once
//! - WAL-5: Recovery completeness — committed visible, uncommitted invisible
//! - WAL-6: CRC32 on every record — corruption detected, not skipped
//!
//! ## Prohibited Patterns
//! - Writing to MemTable without WAL append → violates crash-safety
//! - Client ACK before WAL fsync (under LocalWalSync policy) → data loss risk
//! - Replication stream emitting records not yet WAL-durable → phantom commits

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{TableId, Timestamp, TxnId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// WAL Encryption — transparent per-record AES-256-GCM encryption
// ═══════════════════════════════════════════════════════════════════════════

/// Cached encryption context for WAL records.
///
/// Holds the unwrapped AES-256-GCM cipher derived from a DEK so that
/// per-record encrypt/decrypt avoids repeated `KeyManager::unwrap_dek` calls.
///
/// When present in `WalWriter`, every serialized record is encrypted before
/// being written. The on-disk format stays `[len:4][crc:4][payload:len]`
/// where `payload` = `nonce(12) || ciphertext+tag` instead of raw bincode.
pub struct WalEncryption {
    cipher: aes_gcm::Aes256Gcm,
}

impl WalEncryption {
    /// Create from an unwrapped DEK's raw key bytes.
    pub fn from_key(key: &crate::encryption::EncryptionKey) -> Self {
        use aes_gcm::KeyInit;
        Self {
            cipher: aes_gcm::Aes256Gcm::new(key.as_bytes().into()),
        }
    }

    /// Create from a `KeyManager` + `DekId`. Returns `None` if the DEK
    /// cannot be unwrapped (wrong master key, missing DEK, etc.).
    pub fn from_key_manager(
        km: &mut crate::encryption::KeyManager,
        dek_id: crate::encryption::DekId,
    ) -> Option<Self> {
        let key = km.unwrap_dek(dek_id).ok()?;
        Some(Self::from_key(&key))
    }

    /// Encrypt `plaintext` → `nonce(12) || ciphertext+tag`.
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, StorageError> {
        use aes_gcm::aead::{Aead, OsRng};
        use rand::RngCore;

        let mut nonce_bytes = [0u8; crate::encryption::NONCE_LEN];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| StorageError::Wal("TDE: AES-256-GCM encryption failed".into()))?;

        let mut out = Vec::with_capacity(crate::encryption::NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt `nonce(12) || ciphertext+tag` → plaintext.
    fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>, StorageError> {
        use aes_gcm::aead::Aead;

        if encrypted.len() < crate::encryption::NONCE_LEN + crate::encryption::GCM_TAG_LEN {
            return Err(StorageError::Wal(
                "TDE: encrypted WAL record too short".into(),
            ));
        }
        let (nonce_bytes, ciphertext) = encrypted.split_at(crate::encryption::NONCE_LEN);
        let nonce = aes_gcm::Nonce::from_slice(nonce_bytes);
        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| StorageError::Wal("TDE: AES-256-GCM decryption failed (auth mismatch)".into()))
    }
}

impl std::fmt::Debug for WalEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WalEncryption(AES-256-GCM)")
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// WAL Device Trait — abstraction over the durable storage backend
// ═══════════════════════════════════════════════════════════════════════════

/// Abstraction over the physical WAL storage backend.
///
/// Decouples WAL record serialisation from the I/O path so that future
/// backends (Windows IOCP, O_DIRECT/raw disk, cloud block storage) can be
/// swapped in without touching the `WalWriter` logic.
///
/// **Invariant**: After `flush()` returns `Ok(())`, all bytes passed to
/// prior `append()` calls are durable (according to the configured
/// `SyncMode`).
pub trait WalDevice: Send + Sync {
    /// Append raw bytes to the device. Returns the number of bytes written.
    fn append(&self, data: &[u8]) -> Result<usize, StorageError>;

    /// Flush buffered writes and (depending on `SyncMode`) fsync to disk.
    fn flush(&self, sync_mode: SyncMode) -> Result<(), StorageError>;

    /// Read `len` bytes starting at `offset` from the device.
    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, StorageError>;

    /// Current size of the device in bytes.
    fn size(&self) -> u64;

    /// Rotate to a new segment / file. Returns the new segment identifier.
    /// Implementations that don't support segmentation can return Ok(0).
    fn rotate(&self) -> Result<u64, StorageError>;
}

/// File-based WAL device — the default implementation using POSIX/NTFS files.
///
/// This is a thin wrapper that captures the existing `BufWriter<File>` logic
/// behind the `WalDevice` trait, preserving all existing behaviour.
pub struct WalDeviceFile {
    inner: Mutex<WalDeviceFileInner>,
}

struct WalDeviceFileInner {
    writer: BufWriter<File>,
    dir: PathBuf,
    current_segment: u64,
    current_size: u64,
    max_segment_size: u64,
}

impl WalDeviceFile {
    /// Open (or create) a WAL device backed by segment files in `dir`.
    pub fn open(dir: &Path, max_segment_size: u64) -> Result<Self, StorageError> {
        fs::create_dir_all(dir)?;

        let latest = find_latest_segment_in(dir);
        let seg_id = latest.unwrap_or(0);
        let seg_path = dir.join(segment_filename(seg_id));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&seg_path)?;
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let is_new = file_len == 0;
        let mut size = file_len;

        let mut writer = BufWriter::new(file);
        if is_new {
            writer.write_all(WAL_MAGIC)?;
            writer.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
            writer.flush()?;
            size = WAL_SEGMENT_HEADER_SIZE as u64;
        }

        Ok(Self {
            inner: Mutex::new(WalDeviceFileInner {
                writer,
                dir: dir.to_path_buf(),
                current_segment: seg_id,
                current_size: size,
                max_segment_size,
            }),
        })
    }

    /// Current segment ID.
    pub fn current_segment_id(&self) -> u64 {
        self.inner.lock().current_segment
    }

    /// WAL directory path.
    pub fn dir(&self) -> PathBuf {
        self.inner.lock().dir.clone()
    }
}

impl WalDevice for WalDeviceFile {
    fn append(&self, data: &[u8]) -> Result<usize, StorageError> {
        let mut inner = self.inner.lock();

        // Check segment rotation
        if inner.current_size + data.len() as u64 > inner.max_segment_size {
            self.rotate_inner(&mut inner)?;
        }

        inner.writer.write_all(data)?;
        inner.current_size += data.len() as u64;
        Ok(data.len())
    }

    fn flush(&self, sync_mode: SyncMode) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        inner.writer.flush()?;
        match sync_mode {
            SyncMode::None => {}
            SyncMode::FSync | SyncMode::FDataSync => {
                inner.writer.get_ref().sync_data()?;
            }
        }
        Ok(())
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, StorageError> {
        let inner = self.inner.lock();
        let seg_path = inner.dir.join(segment_filename(inner.current_segment));
        let mut file = File::open(&seg_path)?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn size(&self) -> u64 {
        self.inner.lock().current_size
    }

    fn rotate(&self) -> Result<u64, StorageError> {
        let mut inner = self.inner.lock();
        self.rotate_inner(&mut inner)
    }
}

impl WalDeviceFile {
    fn rotate_inner(&self, inner: &mut WalDeviceFileInner) -> Result<u64, StorageError> {
        inner.writer.flush()?;
        inner.writer.get_ref().sync_data()?;

        inner.current_segment += 1;
        let new_path = inner.dir.join(segment_filename(inner.current_segment));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;
        inner.writer = BufWriter::new(file);
        inner.writer.write_all(WAL_MAGIC)?;
        inner.writer.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
        inner.current_size = WAL_SEGMENT_HEADER_SIZE as u64;

        tracing::debug!("WAL device rotated to segment {}", inner.current_segment);
        Ok(inner.current_segment)
    }
}

/// Helper: find the latest segment file in a directory.
fn find_latest_segment_in(dir: &Path) -> Option<u64> {
    let mut max_id = None;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("falcon_") && name.ends_with(".wal") {
                if let Ok(id) = name[7..name.len() - 4].parse::<u64>() {
                    max_id = Some(max_id.map_or(id, |cur: u64| cur.max(id)));
                }
            }
        }
    }
    max_id
}

/// WAL format version for compatibility checks during online upgrades.
///
/// Increment this when the WalRecord enum changes in a backward-incompatible way.
/// v3: Added WalRecord::CreateIndex and WalRecord::DropIndex variants.
/// v4: Eliminated nested JSON — CreateTable, AlterTable, AlterRole now use
///     native bincode-serialized structs instead of JSON strings.
pub const WAL_FORMAT_VERSION: u32 = 4;

/// Magic bytes written at the start of each WAL segment for validation.
pub const WAL_MAGIC: &[u8; 4] = b"FALC";

/// Size of the WAL segment header: magic (4) + format version (4) = 8 bytes.
pub const WAL_SEGMENT_HEADER_SIZE: usize = 8;

/// Typed ALTER TABLE operation for WAL serialization.
///
/// Replaces the previous `operation_json: String` field, eliminating
/// nested JSON inside bincode-serialized WAL records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableOp {
    /// Add a new column to the table.
    AddColumn { column: ColumnDef },
    /// Drop a column by name.
    DropColumn { column_name: String },
    /// Rename a column.
    RenameColumn { old_name: String, new_name: String },
    /// Rename the table itself.
    RenameTable { new_name: String },
}

/// Typed ALTER ROLE options for WAL serialization.
///
/// Replaces the previous `options_json: String` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterRoleOpts {
    pub password: Option<Option<String>>,
    pub can_login: Option<bool>,
    pub is_superuser: Option<bool>,
    pub can_create_db: Option<bool>,
    pub can_create_role: Option<bool>,
}

/// A single WAL record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    /// Begin a transaction.
    BeginTxn { txn_id: TxnId },
    /// Prepare a global transaction (2PC phase-1).
    PrepareTxn { txn_id: TxnId },
    /// Insert a row.
    Insert {
        txn_id: TxnId,
        table_id: TableId,
        row: OwnedRow,
    },
    /// Batch insert multiple rows (single WAL record for performance).
    BatchInsert {
        txn_id: TxnId,
        table_id: TableId,
        rows: Vec<OwnedRow>,
    },
    /// Update a row (full row replacement).
    Update {
        txn_id: TxnId,
        table_id: TableId,
        pk: Vec<u8>,
        new_row: OwnedRow,
    },
    /// Delete a row.
    Delete {
        txn_id: TxnId,
        table_id: TableId,
        pk: Vec<u8>,
    },
    /// Commit a transaction.
    CommitTxn { txn_id: TxnId, commit_ts: Timestamp },
    /// Commit a local fast-path transaction.
    CommitTxnLocal { txn_id: TxnId, commit_ts: Timestamp },
    /// Commit a global slow-path transaction.
    CommitTxnGlobal { txn_id: TxnId, commit_ts: Timestamp },
    /// Abort a transaction.
    AbortTxn { txn_id: TxnId },
    /// Abort a local fast-path transaction.
    AbortTxnLocal { txn_id: TxnId },
    /// Abort a global slow-path transaction.
    AbortTxnGlobal { txn_id: TxnId },
    /// DDL: create database.
    CreateDatabase { name: String, owner: String },
    /// DDL: drop database.
    DropDatabase { name: String },
    /// DDL: create table (schema serialized natively via bincode).
    CreateTable { schema: TableSchema },
    /// DDL: drop table.
    DropTable { table_name: String },
    /// DDL: create view.
    CreateView { name: String, query_sql: String },
    /// DDL: drop view.
    DropView { name: String },
    /// DDL: alter table (typed operation, bincode-serialized).
    AlterTable {
        table_name: String,
        op: AlterTableOp,
    },
    /// DDL: create sequence.
    CreateSequence { name: String, start: i64 },
    /// DDL: drop sequence.
    DropSequence { name: String },
    /// DDL: set sequence value.
    SetSequenceValue { name: String, value: i64 },
    /// DDL: create named index.
    CreateIndex {
        index_name: String,
        table_name: String,
        column_idx: usize,
        unique: bool,
    },
    /// DDL: drop named index.
    DropIndex {
        index_name: String,
        table_name: String,
        column_idx: usize,
    },
    /// DDL: truncate table.
    TruncateTable { table_name: String },
    /// DDL: create schema.
    CreateSchema { name: String, owner: String },
    /// DDL: drop schema.
    DropSchema { name: String },
    /// DDL: create role.
    CreateRole {
        name: String,
        can_login: bool,
        is_superuser: bool,
        can_create_db: bool,
        can_create_role: bool,
        password_hash: Option<String>,
    },
    /// DDL: drop role.
    DropRole { name: String },
    /// DDL: alter role (typed options, bincode-serialized).
    AlterRole {
        name: String,
        opts: AlterRoleOpts,
    },
    /// DCL: grant privilege.
    GrantPrivilege {
        grantee: String,
        privilege: String,
        object_type: String,
        object_name: String,
        grantor: String,
    },
    /// DCL: revoke privilege.
    RevokePrivilege {
        grantee: String,
        privilege: String,
        object_type: String,
        object_name: String,
    },
    /// DCL: grant role membership.
    GrantRole { member: String, group: String },
    /// DCL: revoke role membership.
    RevokeRole { member: String, group: String },
    /// Checkpoint marker.
    Checkpoint { timestamp: Timestamp },
    /// P0-3: Coordinator decision record for 2PC crash recovery.
    /// Written by the coordinator BEFORE sending commit/abort to participants.
    /// On coordinator crash recovery, replay scans for these records to resolve
    /// in-doubt transactions: if CoordinatorCommit exists → commit all shards,
    /// if only CoordinatorPrepare exists without a matching decision → abort.
    CoordinatorPrepare {
        txn_id: TxnId,
        /// Shard IDs that participated in PREPARE.
        participant_shards: Vec<u64>,
    },
    /// P0-3: Coordinator commits the global transaction (decision record).
    /// After this record is durable, the coordinator sends COMMIT to all shards.
    CoordinatorCommit { txn_id: TxnId, commit_ts: Timestamp },
    /// P0-3: Coordinator aborts the global transaction (decision record).
    CoordinatorAbort { txn_id: TxnId },
}

/// Callback invoked when a WAL segment is rotated (completed).
/// Arguments: (completed_segment_id, segment_size_bytes)
pub type SegmentRotateCallback = Arc<dyn Fn(u64, u64) + Send + Sync>;


/// WAL writer: append-only, with group commit and segment rotation.
pub struct WalWriter {
    inner: Mutex<WalWriterInner>,
    lsn: AtomicU64,
    atomic_pending: std::sync::atomic::AtomicUsize,
    encryption_enabled: std::sync::atomic::AtomicBool,
    sync_mode: SyncMode,
    /// Max bytes per WAL segment before rotating.
    max_segment_size: u64,
    /// Group commit: buffer up to this many records before flushing.
    group_commit_size: usize,
    /// Optional TDE encryption context. When `Some`, every record's serialized
    /// payload is encrypted with AES-256-GCM before being written to disk.
    /// Wrapped in `RwLock` for safe one-time initialization via `set_encryption(&self)`.
    encryption: parking_lot::RwLock<Option<WalEncryption>>,
    /// Optional callback invoked after each segment rotation (for PITR archiving).
    segment_rotate_callback: parking_lot::RwLock<Option<SegmentRotateCallback>>,
    flush_fd_cache: Mutex<Option<File>>,
    flush_buf: Mutex<Vec<u8>>,
    // Dedicated flush thread support
    pub(crate) flushed_lsn: AtomicU64,
    flush_mu: Mutex<()>,
    flush_cvar: parking_lot::Condvar,
    flush_shutdown: std::sync::atomic::AtomicBool,
}

struct WalWriterInner {
    file: File,
    buf: Vec<u8>,
    dir: PathBuf,
    current_segment: u64,
    current_segment_size: u64,
    pending_count: usize,
    max_lsn: u64,
}

use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum SyncMode {
    None,
    FSync,
    FDataSync,
}

/// Default segment size: 64 MB.
const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
/// Default group commit batch: 32 records.
const DEFAULT_GROUP_COMMIT_SIZE: usize = 32;

pub fn segment_filename(segment_id: u64) -> String {
    format!("falcon_{segment_id:06}.wal")
}

impl WalWriter {
    pub fn open(dir: &Path, sync_mode: SyncMode) -> Result<Self, StorageError> {
        Self::open_with_options(
            dir,
            sync_mode,
            DEFAULT_SEGMENT_SIZE,
            DEFAULT_GROUP_COMMIT_SIZE,
        )
    }

    /// Register a callback to be invoked on each WAL segment rotation.
    /// The callback receives (completed_segment_id, segment_size_bytes).
    /// Used by the PITR archiver to copy completed segments to the archive dir.
    pub fn set_segment_rotate_callback(&self, cb: SegmentRotateCallback) {
        *self.segment_rotate_callback.write() = Some(cb);
    }

    pub fn open_with_options(
        dir: &Path,
        sync_mode: SyncMode,
        max_segment_size: u64,
        group_commit_size: usize,
    ) -> Result<Self, StorageError> {
        fs::create_dir_all(dir)?;

        // Find the latest segment or create segment 0
        let latest_segment = Self::find_latest_segment(dir);
        let segment_id = latest_segment.unwrap_or(0);
        let seg_path = dir.join(segment_filename(segment_id));

        // Also handle legacy single-file WAL (falcon.wal)
        let legacy_path = dir.join("falcon.wal");
        if legacy_path.exists() && latest_segment.is_none() {
            let _ = fs::rename(&legacy_path, &seg_path);
        }

        let file = Self::open_wal_file(&seg_path)?;
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let is_new_file = file_len == 0;
        let mut current_segment_size = file_len;

        let mut buf = Vec::with_capacity(8 * 1024);
        if is_new_file {
            buf.extend_from_slice(WAL_MAGIC);
            buf.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
            current_segment_size = WAL_SEGMENT_HEADER_SIZE as u64;
        }

        Ok(Self {
            inner: Mutex::new(WalWriterInner {
                file,
                buf,
                dir: dir.to_path_buf(),
                current_segment: segment_id,
                current_segment_size,
                pending_count: 0,
                max_lsn: 0,
            }),
            segment_rotate_callback: parking_lot::RwLock::new(None),
            lsn: AtomicU64::new(0),
            atomic_pending: std::sync::atomic::AtomicUsize::new(0),
            encryption_enabled: std::sync::atomic::AtomicBool::new(false),
            sync_mode,
            max_segment_size,
            group_commit_size,
            encryption: parking_lot::RwLock::new(None),
            flush_fd_cache: Mutex::new(None),
            flush_buf: Mutex::new(Vec::with_capacity(8 * 1024)),
            flushed_lsn: AtomicU64::new(0),
            flush_mu: Mutex::new(()),
            flush_cvar: parking_lot::Condvar::new(),
            flush_shutdown: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Enable TDE for this WAL writer. All subsequent `append` calls will
    /// encrypt the serialized record payload with AES-256-GCM.
    /// Safe to call from `&self` — uses interior mutability (RwLock).
    pub fn set_encryption(&self, enc: WalEncryption) {
        *self.encryption.write() = Some(enc);
        self.encryption_enabled.store(true, Ordering::Relaxed);
    }

    /// Whether WAL encryption is currently active.
    pub fn is_encrypted(&self) -> bool {
        self.encryption.read().is_some()
    }

    /// Spawn a dedicated flush thread that does back-to-back FUA writes.
    /// Returns the JoinHandle. Caller should store it and call shutdown_flush_thread() on drop.
    pub fn start_flush_thread(self: &Arc<Self>) -> std::thread::JoinHandle<()> {
        let wal = Arc::clone(self);
        std::thread::Builder::new()
            .name("wal-flush".into())
            .spawn(move || {
                let mut spins = 0u32;
                while !wal.flush_shutdown.load(Ordering::Relaxed) {
                    if wal.atomic_pending.load(Ordering::Relaxed) == 0 {
                        spins += 1;
                        if spins < 64 {
                            std::hint::spin_loop();
                        } else {
                            std::thread::yield_now();
                            if spins > 256 {
                                std::thread::sleep(std::time::Duration::from_micros(50));
                            }
                        }
                        continue;
                    }
                    spins = 0;
                    match wal.flush_split() {
                        Ok(flushed) => {
                            wal.flushed_lsn.store(flushed, Ordering::Release);
                        }
                        Err(e) => {
                            tracing::error!("WAL flush thread error: {e}");
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            continue;
                        }
                    }
                    wal.flush_cvar.notify_all();
                }
            })
            .expect("spawn wal-flush thread")
    }

    pub fn shutdown_flush_thread(&self) {
        self.flush_shutdown.store(true, Ordering::Relaxed);
        self.flush_cvar.notify_all();
    }

    /// Block until the given LSN has been durably flushed.
    pub fn wait_flushed(&self, lsn: u64) {
        let mut spins = 0u32;
        loop {
            if self.flushed_lsn.load(Ordering::Acquire) >= lsn {
                return;
            }
            spins += 1;
            if spins < 32 {
                std::hint::spin_loop();
            } else if spins < 128 {
                std::thread::yield_now();
            } else {
                // Fallback to condvar for very long waits
                let mut guard = self.flush_mu.lock();
                if self.flushed_lsn.load(Ordering::Acquire) >= lsn {
                    return;
                }
                self.flush_cvar.wait(&mut guard);
            }
        }
    }

    #[cfg(target_os = "windows")]
    fn open_wal_file(path: &Path) -> Result<File, StorageError> {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_WRITE_THROUGH: u32 = 0x80000000;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .custom_flags(FILE_FLAG_WRITE_THROUGH)
            .open(path)?;
        Ok(file)
    }

    #[cfg(not(target_os = "windows"))]
    fn open_wal_file(path: &Path) -> Result<File, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(file)
    }

    fn find_latest_segment(dir: &Path) -> Option<u64> {
        let mut max_id = None;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("falcon_") && name.ends_with(".wal") {
                    if let Ok(id) = name[7..name.len() - 4].parse::<u64>() {
                        max_id = Some(max_id.map_or(id, |cur: u64| cur.max(id)));
                    }
                }
            }
        }
        max_id
    }

    /// Append a record to the WAL. Returns the LSN.
    /// Group commit: if pending records reach the threshold, auto-flush.
    ///
    /// When TDE is enabled (`set_encryption`), the serialized payload is
    /// encrypted with AES-256-GCM before being written. The on-disk format
    /// is unchanged: `[len:4][crc:4][payload:len]`, but `payload` contains
    /// `nonce(12) || ciphertext+tag` instead of raw bincode.
    pub fn append(&self, record: &WalRecord) -> Result<u64, StorageError> {
        thread_local! {
            static APPEND_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(128));
        }
        APPEND_BUF.with(|cell| -> Result<u64, StorageError> {
            let mut data = cell.borrow_mut();
            data.clear();
            bincode::serialize_into(&mut *data, record)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            if self.encryption_enabled.load(Ordering::Relaxed) {
                if let Some(enc) = self.encryption.read().as_ref() {
                    let enc_data = enc.encrypt(&data)?;
                    data.clear();
                    data.extend_from_slice(&enc_data);
                }
            }

            let checksum = crc32fast::hash(&data);
            let len = data.len() as u32;
            let record_size = 8 + data.len() as u64;

            let mut inner = self.inner.lock();
            let lsn = self.lsn.fetch_add(1, Ordering::Relaxed);

            if inner.current_segment_size + record_size > self.max_segment_size {
                self.rotate_segment(&mut inner)?;
            }

            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&len.to_le_bytes());
            header[4..].copy_from_slice(&checksum.to_le_bytes());
            inner.buf.extend_from_slice(&header);
            inner.buf.extend_from_slice(&data);
            inner.current_segment_size += record_size;
            inner.pending_count += 1;
            inner.max_lsn = lsn;
            self.atomic_pending.fetch_add(1, Ordering::Relaxed);

            if inner.pending_count >= self.group_commit_size {
                self.flush_inner(&mut inner)?;
            }

            Ok(lsn)
        })
    }

    /// Append multiple records under a single mutex hold.
    /// Serialization + encryption happen outside the mutex; only buf.extend is inside.
    pub fn append_multi(&self, records: &[&WalRecord]) -> Result<u64, StorageError> {
        let encrypted = self.encryption_enabled.load(Ordering::Relaxed);

        thread_local! {
            static SERIAL_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(256));
        }
        SERIAL_BUF.with(|cell| -> Result<u64, StorageError> {
            let mut serial_buf = cell.borrow_mut();
            serial_buf.clear();
            let mut index = [(0usize, 0usize, 0u32); 4];
            for (ri, rec) in records.iter().enumerate() {
                let start = serial_buf.len();
                bincode::serialize_into(&mut *serial_buf, rec)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                if encrypted {
                    let raw = &serial_buf[start..];
                    if let Some(enc) = self.encryption.read().as_ref() {
                        let encrypted_data = enc.encrypt(raw)?;
                        serial_buf.truncate(start);
                        serial_buf.extend_from_slice(&encrypted_data);
                    }
                }
                let end = serial_buf.len();
                let crc = crc32fast::hash(&serial_buf[start..end]);
                index[ri] = (start, end - start, crc);
            }

            let count = records.len() as u64;
            let mut inner = self.inner.lock();
            let base_lsn = self.lsn.fetch_add(count, Ordering::Relaxed);
            for i in 0..records.len() {
                let (offset, len, crc) = index[i];
                let record_size = 8 + len as u64;
                if inner.current_segment_size + record_size > self.max_segment_size {
                    self.rotate_segment(&mut inner)?;
                }
                let mut header = [0u8; 8];
                header[..4].copy_from_slice(&(len as u32).to_le_bytes());
                header[4..].copy_from_slice(&crc.to_le_bytes());
                inner.buf.extend_from_slice(&header);
                inner.buf.extend_from_slice(&serial_buf[offset..offset + len]);
                inner.current_segment_size += record_size;
                inner.pending_count += 1;
                self.atomic_pending.fetch_add(1, Ordering::Relaxed);
            }
            let last_lsn = base_lsn + count - 1;
            inner.max_lsn = last_lsn;
            Ok(last_lsn)
        })
    }

    /// Flush buffered writes and optionally sync to disk.
    pub fn flush(&self) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        self.flush_inner(&mut inner)
    }

    /// Double-buffer flush: swap buffer under WAL mutex (instant), then
    /// FUA write outside mutex so appenders continue concurrently.
    pub fn flush_split(&self) -> Result<u64, StorageError> {
        let mut flush_buf = self.flush_buf.lock();
        let flushed_lsn = {
            let mut inner = self.inner.lock();
            if inner.buf.is_empty() {
                return Ok(inner.max_lsn);
            }
            let flushed = inner.max_lsn;
            flush_buf.clear();
            std::mem::swap(&mut inner.buf, &mut *flush_buf);
            inner.pending_count = 0;
            self.atomic_pending.store(0, Ordering::Relaxed);
            flushed
        };
        let fd = self.flush_fd_cache.lock().take()
            .map_or_else(|| self.inner.lock().file.try_clone(), Ok)?;
        (&fd).write_all(&flush_buf)?;
        #[cfg(not(target_os = "windows"))]
        match self.sync_mode {
            SyncMode::None => {}
            SyncMode::FSync => fd.sync_all()?,
            SyncMode::FDataSync => fd.sync_data()?,
        }
        *self.flush_fd_cache.lock() = Some(fd);
        Ok(flushed_lsn)
    }

    fn flush_inner(&self, inner: &mut WalWriterInner) -> Result<(), StorageError> {
        if !inner.buf.is_empty() {
            inner.file.write_all(&inner.buf)?;
            inner.buf.clear();
        }
        inner.pending_count = 0;
        self.atomic_pending.store(0, Ordering::Relaxed);
        #[cfg(not(target_os = "windows"))]
        match self.sync_mode {
            SyncMode::None => {}
            SyncMode::FSync => inner.file.sync_all()?,
            SyncMode::FDataSync => inner.file.sync_data()?,
        }
        Ok(())
    }

    fn rotate_segment(&self, inner: &mut WalWriterInner) -> Result<(), StorageError> {
        self.flush_inner(inner)?;

        // Record completed segment info before rotating (for PITR callback)
        let completed_segment_id = inner.current_segment;
        let completed_segment_size = inner.current_segment_size;

        inner.current_segment += 1;
        let new_path = inner.dir.join(segment_filename(inner.current_segment));
        inner.file = Self::open_wal_file(&new_path)?;
        *self.flush_fd_cache.lock() = None;

        inner.buf.extend_from_slice(WAL_MAGIC);
        inner.buf.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        inner.current_segment_size = WAL_SEGMENT_HEADER_SIZE as u64;
        inner.pending_count = 0;

        tracing::debug!("WAL rotated to segment {}", inner.current_segment);

        // Invoke PITR archive callback (outside the lock for this segment)
        if let Some(ref cb) = *self.segment_rotate_callback.read() {
            cb(completed_segment_id, completed_segment_size);
        }

        Ok(())
    }

    pub fn current_lsn(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
    }

    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    pub fn pending_count(&self) -> usize {
        self.atomic_pending.load(Ordering::Relaxed)
    }

    /// Remove WAL segments older than the given segment ID.
    pub fn purge_segments_before(&self, segment_id: u64) -> Result<usize, StorageError> {
        let inner = self.inner.lock();
        let mut removed = 0;
        for id in 0..segment_id {
            let path = inner.dir.join(segment_filename(id));
            if path.exists() {
                fs::remove_file(&path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Get the current segment ID.
    pub fn current_segment_id(&self) -> u64 {
        self.inner.lock().current_segment
    }

    /// Get the WAL directory path.
    pub fn wal_dir(&self) -> PathBuf {
        self.inner.lock().dir.clone()
    }
}

/// WAL reader for crash recovery — reads all segments in order.
pub struct WalReader {
    dir: PathBuf,
}

impl WalReader {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    /// Read records from WAL segments starting at `from_segment_id`.
    /// Used for checkpoint-based recovery (skip segments before checkpoint).
    pub fn read_from_segment(&self, from_segment_id: u64) -> Result<Vec<WalRecord>, StorageError> {
        self.read_from_segment_encrypted(from_segment_id, None)
    }

    /// Read records from WAL segments starting at `from_segment_id`,
    /// decrypting with the given `WalEncryption` context if provided.
    pub fn read_from_segment_encrypted(
        &self,
        from_segment_id: u64,
        encryption: Option<&WalEncryption>,
    ) -> Result<Vec<WalRecord>, StorageError> {
        let mut records = Vec::new();
        let mut segment_ids = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy().to_string();
                if name.starts_with("falcon_") && name.ends_with(".wal") {
                    if let Ok(id) = name[7..name.len() - 4].parse::<u64>() {
                        if id >= from_segment_id {
                            segment_ids.push(id);
                        }
                    }
                }
            }
        }
        segment_ids.sort();

        for seg_id in segment_ids {
            let seg_path = self.dir.join(segment_filename(seg_id));
            if seg_path.exists() {
                let data = fs::read(&seg_path)?;
                Self::parse_records_encrypted(&data, &mut records, encryption)?;
            }
        }

        Ok(records)
    }

    /// Read all records from all WAL segments (and legacy single file).
    pub fn read_all(&self) -> Result<Vec<WalRecord>, StorageError> {
        self.read_all_encrypted(None)
    }

    /// Read all records, decrypting with the given `WalEncryption` context if provided.
    pub fn read_all_encrypted(
        &self,
        encryption: Option<&WalEncryption>,
    ) -> Result<Vec<WalRecord>, StorageError> {
        let mut records = Vec::new();

        // Try legacy single-file WAL first
        let legacy_path = self.dir.join("falcon.wal");
        if legacy_path.exists() {
            let data = fs::read(&legacy_path)?;
            Self::parse_records_encrypted(&data, &mut records, encryption)?;
        }

        // Read segmented WAL files in order
        let mut segment_ids = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy().to_string();
                if name.starts_with("falcon_") && name.ends_with(".wal") {
                    if let Ok(id) = name[7..name.len() - 4].parse::<u64>() {
                        segment_ids.push(id);
                    }
                }
            }
        }
        segment_ids.sort();

        for seg_id in segment_ids {
            let seg_path = self.dir.join(segment_filename(seg_id));
            if seg_path.exists() {
                let data = fs::read(&seg_path)?;
                Self::parse_records_encrypted(&data, &mut records, encryption)?;
            }
        }

        Ok(records)
    }

    /// Parse WAL records from raw bytes with optional TDE decryption.
    ///
    /// When `encryption` is `Some`, each record's on-disk payload is decrypted
    /// (AES-256-GCM) before bincode deserialization.
    fn parse_records_encrypted(
        data: &[u8],
        records: &mut Vec<WalRecord>,
        encryption: Option<&WalEncryption>,
    ) -> Result<(), StorageError> {
        // Skip segment header if present (magic + format version = 8 bytes)
        let mut pos = if data.len() >= WAL_SEGMENT_HEADER_SIZE && &data[0..4] == WAL_MAGIC.as_slice() {
            let _format_version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
            WAL_SEGMENT_HEADER_SIZE
        } else {
            0
        };
        while pos + 8 <= data.len() {
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let checksum =
                u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]]);
            pos += 8;

            if pos + len > data.len() {
                tracing::warn!("WAL truncated at position {}, stopping recovery", pos);
                break;
            }

            let record_data = &data[pos..pos + len];
            let actual_checksum = crc32fast::hash(record_data);
            if actual_checksum != checksum {
                tracing::warn!(
                    "WAL checksum mismatch at position {}, stopping recovery",
                    pos
                );
                break;
            }

            // Decrypt if TDE is active, otherwise use raw bytes
            let payload = match encryption {
                Some(enc) => match enc.decrypt(record_data) {
                    Ok(plain) => plain,
                    Err(e) => {
                        tracing::warn!("WAL TDE decryption error at position {}: {}", pos, e);
                        break;
                    }
                },
                None => record_data.to_vec(),
            };

            match bincode::deserialize::<WalRecord>(&payload) {
                Ok(record) => records.push(record),
                Err(e) => {
                    tracing::warn!("WAL deserialization error at position {}: {}", pos, e);
                    break;
                }
            }
            pos += len;
        }
        Ok(())
    }
}

/// Rows for a single table in a checkpoint: Vec<(pk_bytes, row)>.
pub type CheckpointTableRows = Vec<(Vec<u8>, falcon_common::datum::OwnedRow)>;

/// Checkpoint data: a snapshot of the entire database state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    /// The catalog (all table schemas) at checkpoint time.
    pub catalog: falcon_common::schema::Catalog,
    /// All committed rows per table: (table_id, rows).
    pub table_data: Vec<(TableId, CheckpointTableRows)>,
    /// The WAL segment ID at checkpoint time. Recovery replays from this segment onward.
    pub wal_segment_id: u64,
    /// The LSN at checkpoint time.
    pub wal_lsn: u64,
}

const CHECKPOINT_FILENAME: &str = "checkpoint.bin";

impl CheckpointData {
    /// Write checkpoint to a file in the given directory.
    ///
    /// Uses write-to-temp + rename for crash safety.
    /// On Windows, `fs::rename` fails if the target exists, so we remove it first.
    /// The window between remove and rename is acceptable: if we crash after remove
    /// but before rename, the next startup will find no checkpoint and replay the
    /// full WAL (safe, just slower).
    pub fn write_to_dir(&self, dir: &Path) -> Result<(), StorageError> {
        let path = dir.join(CHECKPOINT_FILENAME);
        let data =
            bincode::serialize(self).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let tmp_path = dir.join("checkpoint.tmp");
        fs::write(&tmp_path, &data)?;
        // Sync the temp file to ensure data is durable before replacing
        let f = fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);
        // On Windows fs::rename fails if target exists; remove it first
        if path.exists() {
            let _ = fs::remove_file(&path);
        }
        fs::rename(&tmp_path, &path)?;
        Ok(())
    }

    /// Read checkpoint from a directory, if it exists.
    pub fn read_from_dir(dir: &Path) -> Result<Option<Self>, StorageError> {
        let path = dir.join(CHECKPOINT_FILENAME);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(&path)?;
        let ckpt: Self =
            bincode::deserialize(&data).map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(Some(ckpt))
    }
}

/// No-op WAL for pure in-memory mode.
pub struct NullWal;

impl NullWal {
    pub const fn append(&self, _record: &WalRecord) -> Result<u64, StorageError> {
        Ok(0)
    }

    pub const fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
