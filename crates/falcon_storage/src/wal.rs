use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::types::{TableId, Timestamp, TxnId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// WAL format version for compatibility checks during online upgrades.
/// Increment this when the WalRecord enum changes in a backward-incompatible way.
/// v3: Added WalRecord::CreateIndex and WalRecord::DropIndex variants.
pub const WAL_FORMAT_VERSION: u32 = 3;

/// Magic bytes written at the start of each WAL segment for validation.
pub const WAL_MAGIC: &[u8; 4] = b"FALC";

/// Size of the WAL segment header: magic (4) + format version (4) = 8 bytes.
pub const WAL_SEGMENT_HEADER_SIZE: usize = 8;

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
    CommitTxn {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    /// Commit a local fast-path transaction.
    CommitTxnLocal {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    /// Commit a global slow-path transaction.
    CommitTxnGlobal {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    /// Abort a transaction.
    AbortTxn { txn_id: TxnId },
    /// Abort a local fast-path transaction.
    AbortTxnLocal { txn_id: TxnId },
    /// Abort a global slow-path transaction.
    AbortTxnGlobal { txn_id: TxnId },
    /// DDL: create table (schema stored as JSON).
    CreateTable { schema_json: String },
    /// DDL: drop table.
    DropTable { table_name: String },
    /// DDL: create view.
    CreateView { name: String, query_sql: String },
    /// DDL: drop view.
    DropView { name: String },
    /// DDL: alter table (operation stored as JSON for flexibility).
    AlterTable { table_name: String, operation_json: String },
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
    CoordinatorCommit {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    /// P0-3: Coordinator aborts the global transaction (decision record).
    CoordinatorAbort {
        txn_id: TxnId,
    },
}

/// WAL writer: append-only, with group commit and segment rotation.
pub struct WalWriter {
    inner: Mutex<WalWriterInner>,
    lsn: AtomicU64,
    sync_mode: SyncMode,
    /// Max bytes per WAL segment before rotating.
    max_segment_size: u64,
    /// Group commit: buffer up to this many records before flushing.
    group_commit_size: usize,
}

struct WalWriterInner {
    writer: BufWriter<File>,
    dir: PathBuf,
    current_segment: u64,
    current_segment_size: u64,
    pending_count: usize,
}

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

fn segment_filename(segment_id: u64) -> String {
    format!("falcon_{:06}.wal", segment_id)
}

impl WalWriter {
    pub fn open(dir: &Path, sync_mode: SyncMode) -> Result<Self, StorageError> {
        Self::open_with_options(dir, sync_mode, DEFAULT_SEGMENT_SIZE, DEFAULT_GROUP_COMMIT_SIZE)
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

        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&seg_path)?;
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let is_new_file = file_len == 0;
        let mut current_segment_size = file_len;

        // Also handle legacy single-file WAL (falcon.wal)
        let legacy_path = dir.join("falcon.wal");
        if legacy_path.exists() && latest_segment.is_none() {
            // Rename legacy WAL to segment 0
            let _ = fs::rename(&legacy_path, &seg_path);
        }

        let mut writer = BufWriter::new(file);

        // Write segment header for brand-new segments
        if is_new_file {
            writer.write_all(WAL_MAGIC)?;
            writer.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
            writer.flush()?;
            current_segment_size = WAL_SEGMENT_HEADER_SIZE as u64;
        }

        Ok(Self {
            inner: Mutex::new(WalWriterInner {
                writer,
                dir: dir.to_path_buf(),
                current_segment: segment_id,
                current_segment_size,
                pending_count: 0,
            }),
            lsn: AtomicU64::new(0),
            sync_mode,
            max_segment_size,
            group_commit_size,
        })
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
    pub fn append(&self, record: &WalRecord) -> Result<u64, StorageError> {
        let data = bincode::serialize(record)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let lsn = self.lsn.fetch_add(1, Ordering::SeqCst);
        let checksum = crc32fast::hash(&data);
        let len = data.len() as u32;
        let record_size = 8 + data.len() as u64; // header + data

        let mut inner = self.inner.lock();

        // Check segment rotation
        if inner.current_segment_size + record_size > self.max_segment_size {
            self.rotate_segment(&mut inner)?;
        }

        // Record format: [len:4][checksum:4][data:len]
        inner.writer.write_all(&len.to_le_bytes())?;
        inner.writer.write_all(&checksum.to_le_bytes())?;
        inner.writer.write_all(&data)?;
        inner.current_segment_size += record_size;
        inner.pending_count += 1;

        // Group commit: flush when batch is full
        if inner.pending_count >= self.group_commit_size {
            self.flush_inner(&mut inner)?;
        }

        Ok(lsn)
    }

    /// Flush buffered writes and optionally sync to disk.
    pub fn flush(&self) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        self.flush_inner(&mut inner)
    }

    fn flush_inner(&self, inner: &mut WalWriterInner) -> Result<(), StorageError> {
        inner.writer.flush()?;
        inner.pending_count = 0;
        match self.sync_mode {
            SyncMode::None => {}
            SyncMode::FSync | SyncMode::FDataSync => {
                inner.writer.get_ref().sync_data()?;
            }
        }
        Ok(())
    }

    fn rotate_segment(&self, inner: &mut WalWriterInner) -> Result<(), StorageError> {
        // Flush current segment
        inner.writer.flush()?;
        if matches!(self.sync_mode, SyncMode::FSync | SyncMode::FDataSync) {
            inner.writer.get_ref().sync_data()?;
        }

        // Open new segment
        inner.current_segment += 1;
        let new_path = inner.dir.join(segment_filename(inner.current_segment));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;
        inner.writer = BufWriter::new(file);

        // Write segment header
        inner.writer.write_all(WAL_MAGIC)?;
        inner.writer.write_all(&WAL_FORMAT_VERSION.to_le_bytes())?;
        inner.current_segment_size = WAL_SEGMENT_HEADER_SIZE as u64;
        inner.pending_count = 0;

        tracing::debug!("WAL rotated to segment {}", inner.current_segment);
        Ok(())
    }

    pub fn current_lsn(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
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
                Self::parse_records(&data, &mut records);
            }
        }

        Ok(records)
    }

    /// Read all records from all WAL segments (and legacy single file).
    pub fn read_all(&self) -> Result<Vec<WalRecord>, StorageError> {
        let mut records = Vec::new();

        // Try legacy single-file WAL first
        let legacy_path = self.dir.join("falcon.wal");
        if legacy_path.exists() {
            let data = fs::read(&legacy_path)?;
            Self::parse_records(&data, &mut records);
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
                Self::parse_records(&data, &mut records);
            }
        }

        Ok(records)
    }

    /// Parse WAL records from raw bytes, appending to the output vector.
    /// Automatically detects and skips the segment header (WAL_MAGIC + version).
    fn parse_records(data: &[u8], records: &mut Vec<WalRecord>) {
        let mut pos = 0;

        // Skip segment header if present (magic + format version = 8 bytes)
        if data.len() >= WAL_SEGMENT_HEADER_SIZE
            && &data[0..4] == WAL_MAGIC.as_slice()
        {
            let _format_version = u32::from_le_bytes([
                data[4], data[5], data[6], data[7],
            ]);
            pos = WAL_SEGMENT_HEADER_SIZE;
        }
        while pos + 8 <= data.len() {
            let len = u32::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
            ]) as usize;
            let checksum = u32::from_le_bytes([
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
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

            match bincode::deserialize::<WalRecord>(record_data) {
                Ok(record) => records.push(record),
                Err(e) => {
                    tracing::warn!("WAL deserialization error at position {}: {}", pos, e);
                    break;
                }
            }
            pos += len;
        }
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
    pub fn write_to_dir(&self, dir: &Path) -> Result<(), StorageError> {
        let path = dir.join(CHECKPOINT_FILENAME);
        let data = bincode::serialize(self)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        // Write atomically: write to temp file, then rename
        let tmp_path = dir.join("checkpoint.tmp");
        fs::write(&tmp_path, &data)?;
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
        let ckpt: Self = bincode::deserialize(&data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(Some(ckpt))
    }
}

/// No-op WAL for pure in-memory mode.
pub struct NullWal;

impl NullWal {
    pub fn append(&self, _record: &WalRecord) -> Result<u64, StorageError> {
        Ok(0)
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
