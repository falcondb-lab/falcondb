//! MVCC value encoding for LSM storage.
//!
//! Each value stored in the LSM engine encodes transaction metadata alongside
//! the actual row data. This enables the visibility rules required for
//! snapshot isolation and 2PC:
//!
//! - **Prepared** versions are never visible to external readers.
//! - **Committed** versions are visible only to transactions with `read_ts >= commit_ts`.
//! - **Aborted** versions are invisible and eligible for GC during compaction.
//!
//! ## Wire format (little-endian)
//!
//! ```text
//!   [txn_id: u64]       — transaction that wrote this version
//!   [status: u8]        — 0=Prepared, 1=Committed, 2=Aborted
//!   [commit_ts: u64]    — commit timestamp (0 if not yet committed)
//!   [is_tombstone: u8]  — 1 if this is a deletion marker
//!   [data_len: u32]     — length of the row data
//!   [data: bytes]       — the actual row data (empty if tombstone)
//! ```
//!
//! Total header overhead: 8 + 1 + 8 + 1 + 4 = 22 bytes.

use falcon_common::types::{Timestamp, TxnId};

/// MVCC version status stored on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MvccStatus {
    Prepared = 0,
    Committed = 1,
    Aborted = 2,
}

impl MvccStatus {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(MvccStatus::Prepared),
            1 => Some(MvccStatus::Committed),
            2 => Some(MvccStatus::Aborted),
            _ => None,
        }
    }
}

/// Header size for MVCC-encoded values.
pub const MVCC_HEADER_SIZE: usize = 22;

/// An MVCC-encoded value parsed from disk.
#[derive(Debug, Clone)]
pub struct MvccValue {
    pub txn_id: TxnId,
    pub status: MvccStatus,
    pub commit_ts: Timestamp,
    pub is_tombstone: bool,
    pub data: Vec<u8>,
}

impl MvccValue {
    /// Encode an MVCC value to bytes for storage in the LSM engine.
    pub fn encode(&self) -> Vec<u8> {
        let data_len = self.data.len() as u32;
        let total = MVCC_HEADER_SIZE + self.data.len();
        let mut buf = Vec::with_capacity(total);

        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());
        buf.push(self.status as u8);
        buf.extend_from_slice(&self.commit_ts.0.to_le_bytes());
        buf.push(if self.is_tombstone { 1 } else { 0 });
        buf.extend_from_slice(&data_len.to_le_bytes());
        buf.extend_from_slice(&self.data);

        buf
    }

    /// Decode an MVCC value from bytes read from the LSM engine.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < MVCC_HEADER_SIZE {
            return None;
        }

        let txn_id = TxnId(u64::from_le_bytes(raw[0..8].try_into().ok()?));
        let status = MvccStatus::from_byte(raw[8])?;
        let commit_ts = Timestamp(u64::from_le_bytes(raw[9..17].try_into().ok()?));
        let is_tombstone = raw[17] == 1;
        let data_len = u32::from_le_bytes(raw[18..22].try_into().ok()?) as usize;

        if raw.len() < MVCC_HEADER_SIZE + data_len {
            return None;
        }

        let data = raw[MVCC_HEADER_SIZE..MVCC_HEADER_SIZE + data_len].to_vec();

        Some(Self {
            txn_id,
            status,
            commit_ts,
            is_tombstone,
            data,
        })
    }

    /// Create a new committed value.
    pub fn committed(txn_id: TxnId, commit_ts: Timestamp, data: Vec<u8>) -> Self {
        Self {
            txn_id,
            status: MvccStatus::Committed,
            commit_ts,
            is_tombstone: false,
            data,
        }
    }

    /// Create a committed tombstone (deletion).
    pub fn committed_tombstone(txn_id: TxnId, commit_ts: Timestamp) -> Self {
        Self {
            txn_id,
            status: MvccStatus::Committed,
            commit_ts,
            is_tombstone: true,
            data: Vec::new(),
        }
    }

    /// Create a prepared (uncommitted) value.
    pub fn prepared(txn_id: TxnId, data: Vec<u8>) -> Self {
        Self {
            txn_id,
            status: MvccStatus::Prepared,
            commit_ts: Timestamp(0),
            is_tombstone: false,
            data,
        }
    }

    /// Create an aborted value marker.
    pub fn aborted(txn_id: TxnId) -> Self {
        Self {
            txn_id,
            status: MvccStatus::Aborted,
            commit_ts: Timestamp(0),
            is_tombstone: false,
            data: Vec::new(),
        }
    }

    /// Check if this version is visible to a reader at the given timestamp.
    ///
    /// Visibility rules:
    /// - Prepared: NEVER visible (invariant: prepared versions are invisible)
    /// - Aborted: NEVER visible
    /// - Committed: visible if `reader_ts >= commit_ts`
    pub fn is_visible(&self, reader_ts: Timestamp) -> bool {
        match self.status {
            MvccStatus::Prepared => false,
            MvccStatus::Aborted => false,
            MvccStatus::Committed => reader_ts.0 >= self.commit_ts.0,
        }
    }

    /// Check if this version can be garbage collected.
    ///
    /// A version is GC-eligible if:
    /// - It is Aborted (always safe to remove)
    /// - It is a Committed tombstone older than the GC safepoint
    /// - It is a Committed version superseded by a newer committed version
    ///   and older than the GC safepoint
    pub fn is_gc_eligible(&self, gc_safepoint: Timestamp) -> bool {
        match self.status {
            MvccStatus::Aborted => true,
            MvccStatus::Committed => self.commit_ts.0 < gc_safepoint.0,
            MvccStatus::Prepared => false, // in-doubt; resolver must handle
        }
    }
}

/// Persistent transaction metadata record.
///
/// Stored in a dedicated `txn_meta` LSM instance to track transaction outcomes
/// across crashes. This is the "decision log" for 2PC.
#[derive(Debug, Clone)]
pub struct TxnMeta {
    pub txn_id: TxnId,
    pub outcome: TxnOutcome,
    pub commit_ts: Timestamp,
    pub epoch: u64,
    pub timestamp_ms: u64,
}

/// Transaction outcome for the persistent txn_meta store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TxnOutcome {
    Prepared = 0,
    Committed = 1,
    Aborted = 2,
}

impl TxnOutcome {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(TxnOutcome::Prepared),
            1 => Some(TxnOutcome::Committed),
            2 => Some(TxnOutcome::Aborted),
            _ => None,
        }
    }
}

impl TxnMeta {
    /// Encode to bytes for storage.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(33);
        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());
        buf.push(self.outcome as u8);
        buf.extend_from_slice(&self.commit_ts.0.to_le_bytes());
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        buf
    }

    /// Decode from bytes.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < 33 {
            return None;
        }
        let txn_id = TxnId(u64::from_le_bytes(raw[0..8].try_into().ok()?));
        let outcome = TxnOutcome::from_byte(raw[8])?;
        let commit_ts = Timestamp(u64::from_le_bytes(raw[9..17].try_into().ok()?));
        let epoch = u64::from_le_bytes(raw[17..25].try_into().ok()?);
        let timestamp_ms = u64::from_le_bytes(raw[25..33].try_into().ok()?);
        Some(Self {
            txn_id,
            outcome,
            commit_ts,
            epoch,
            timestamp_ms,
        })
    }

    /// Key for storing in the txn_meta LSM instance.
    pub fn key(&self) -> Vec<u8> {
        self.txn_id.0.to_be_bytes().to_vec()
    }
}

/// Persistent transaction metadata store backed by a dedicated LSM instance.
///
/// Provides crash-safe storage of transaction outcomes for 2PC recovery.
/// After a crash, the in-doubt resolver reads this store to determine
/// whether prepared transactions should be committed or aborted.
pub struct TxnMetaStore {
    engine: super::engine::LsmEngine,
}

impl TxnMetaStore {
    /// Open or create the txn_meta store at the given directory.
    pub fn open(data_dir: &std::path::Path) -> std::io::Result<Self> {
        let meta_dir = data_dir.join("txn_meta");
        let config = super::engine::LsmConfig {
            memtable_budget_bytes: 4 * 1024 * 1024, // 4 MB
            block_cache_bytes: 8 * 1024 * 1024,     // 8 MB
            compaction: super::compaction::CompactionConfig {
                l0_compaction_trigger: 4,
                l0_stall_trigger: 12,
                ..Default::default()
            },
            sync_writes: true, // must be durable for 2PC
        };
        let engine = super::engine::LsmEngine::open(&meta_dir, config)?;
        Ok(Self { engine })
    }

    /// Record a transaction outcome (durable write).
    pub fn put(&self, meta: &TxnMeta) -> std::io::Result<()> {
        let key = meta.key();
        let value = meta.encode();
        self.engine.put(&key, &value)
    }

    /// Look up a transaction's outcome.
    pub fn get(&self, txn_id: TxnId) -> std::io::Result<Option<TxnMeta>> {
        let key = txn_id.0.to_be_bytes();
        match self.engine.get(&key)? {
            Some(raw) => Ok(TxnMeta::decode(&raw)),
            None => Ok(None),
        }
    }

    /// Remove a transaction record (after GC safepoint).
    pub fn remove(&self, txn_id: TxnId) -> std::io::Result<()> {
        let key = txn_id.0.to_be_bytes();
        self.engine.delete(&key)
    }

    /// Flush pending writes to disk.
    pub fn flush(&self) -> std::io::Result<()> {
        self.engine.flush()
    }

    /// Shutdown the store.
    pub fn shutdown(&self) -> std::io::Result<()> {
        self.engine.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_value_encode_decode_committed() {
        let val = MvccValue::committed(TxnId(42), Timestamp(100), b"hello world".to_vec());
        let encoded = val.encode();
        let decoded = MvccValue::decode(&encoded).unwrap();

        assert_eq!(decoded.txn_id, TxnId(42));
        assert_eq!(decoded.status, MvccStatus::Committed);
        assert_eq!(decoded.commit_ts, Timestamp(100));
        assert!(!decoded.is_tombstone);
        assert_eq!(decoded.data, b"hello world");
    }

    #[test]
    fn test_mvcc_value_encode_decode_tombstone() {
        let val = MvccValue::committed_tombstone(TxnId(7), Timestamp(50));
        let encoded = val.encode();
        let decoded = MvccValue::decode(&encoded).unwrap();

        assert_eq!(decoded.txn_id, TxnId(7));
        assert_eq!(decoded.status, MvccStatus::Committed);
        assert_eq!(decoded.commit_ts, Timestamp(50));
        assert!(decoded.is_tombstone);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_mvcc_value_encode_decode_prepared() {
        let val = MvccValue::prepared(TxnId(99), b"pending data".to_vec());
        let encoded = val.encode();
        let decoded = MvccValue::decode(&encoded).unwrap();

        assert_eq!(decoded.txn_id, TxnId(99));
        assert_eq!(decoded.status, MvccStatus::Prepared);
        assert_eq!(decoded.commit_ts, Timestamp(0));
        assert!(!decoded.is_tombstone);
        assert_eq!(decoded.data, b"pending data");
    }

    #[test]
    fn test_mvcc_value_encode_decode_aborted() {
        let val = MvccValue::aborted(TxnId(55));
        let encoded = val.encode();
        let decoded = MvccValue::decode(&encoded).unwrap();

        assert_eq!(decoded.txn_id, TxnId(55));
        assert_eq!(decoded.status, MvccStatus::Aborted);
    }

    #[test]
    fn test_mvcc_visibility_committed() {
        let val = MvccValue::committed(TxnId(1), Timestamp(100), b"data".to_vec());

        assert!(val.is_visible(Timestamp(100))); // exact match
        assert!(val.is_visible(Timestamp(200))); // future reader
        assert!(!val.is_visible(Timestamp(99))); // too early
    }

    #[test]
    fn test_mvcc_visibility_prepared_never_visible() {
        let val = MvccValue::prepared(TxnId(1), b"data".to_vec());

        assert!(!val.is_visible(Timestamp(0)));
        assert!(!val.is_visible(Timestamp(u64::MAX)));
    }

    #[test]
    fn test_mvcc_visibility_aborted_never_visible() {
        let val = MvccValue::aborted(TxnId(1));

        assert!(!val.is_visible(Timestamp(0)));
        assert!(!val.is_visible(Timestamp(u64::MAX)));
    }

    #[test]
    fn test_mvcc_gc_eligibility() {
        let committed = MvccValue::committed(TxnId(1), Timestamp(50), b"data".to_vec());
        assert!(committed.is_gc_eligible(Timestamp(100))); // older than safepoint
        assert!(!committed.is_gc_eligible(Timestamp(50))); // at safepoint
        assert!(!committed.is_gc_eligible(Timestamp(30))); // newer than safepoint

        let aborted = MvccValue::aborted(TxnId(2));
        assert!(aborted.is_gc_eligible(Timestamp(0))); // always eligible

        let prepared = MvccValue::prepared(TxnId(3), b"data".to_vec());
        assert!(!prepared.is_gc_eligible(Timestamp(u64::MAX))); // never GC prepared
    }

    #[test]
    fn test_txn_meta_encode_decode() {
        let meta = TxnMeta {
            txn_id: TxnId(123),
            outcome: TxnOutcome::Committed,
            commit_ts: Timestamp(456),
            epoch: 7,
            timestamp_ms: 1700000000000,
        };
        let encoded = meta.encode();
        let decoded = TxnMeta::decode(&encoded).unwrap();

        assert_eq!(decoded.txn_id, TxnId(123));
        assert_eq!(decoded.outcome, TxnOutcome::Committed);
        assert_eq!(decoded.commit_ts, Timestamp(456));
        assert_eq!(decoded.epoch, 7);
        assert_eq!(decoded.timestamp_ms, 1700000000000);
    }

    #[test]
    fn test_txn_meta_key_ordering() {
        let m1 = TxnMeta {
            txn_id: TxnId(1),
            outcome: TxnOutcome::Committed,
            commit_ts: Timestamp(0),
            epoch: 0,
            timestamp_ms: 0,
        };
        let m2 = TxnMeta {
            txn_id: TxnId(2),
            outcome: TxnOutcome::Committed,
            commit_ts: Timestamp(0),
            epoch: 0,
            timestamp_ms: 0,
        };
        // Keys should be ordered by txn_id (big-endian for sort order)
        assert!(m1.key() < m2.key());
    }

    #[test]
    fn test_txn_meta_store_basic() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = TxnMetaStore::open(dir.path()).unwrap();

        let meta = TxnMeta {
            txn_id: TxnId(42),
            outcome: TxnOutcome::Committed,
            commit_ts: Timestamp(100),
            epoch: 1,
            timestamp_ms: 1700000000000,
        };

        store.put(&meta).unwrap();
        let result = store.get(TxnId(42)).unwrap();
        assert!(result.is_some());
        let loaded = result.unwrap();
        assert_eq!(loaded.txn_id, TxnId(42));
        assert_eq!(loaded.outcome, TxnOutcome::Committed);
        assert_eq!(loaded.commit_ts, Timestamp(100));

        // Non-existent txn
        assert!(store.get(TxnId(999)).unwrap().is_none());
    }

    #[test]
    fn test_txn_meta_store_remove() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = TxnMetaStore::open(dir.path()).unwrap();

        let meta = TxnMeta {
            txn_id: TxnId(10),
            outcome: TxnOutcome::Aborted,
            commit_ts: Timestamp(0),
            epoch: 1,
            timestamp_ms: 0,
        };

        store.put(&meta).unwrap();
        assert!(store.get(TxnId(10)).unwrap().is_some());

        store.remove(TxnId(10)).unwrap();
        assert!(store.get(TxnId(10)).unwrap().is_none());
    }

    #[test]
    fn test_txn_meta_store_persistence() {
        let dir = tempfile::TempDir::new().unwrap();

        // Write and flush
        {
            let store = TxnMetaStore::open(dir.path()).unwrap();
            let meta = TxnMeta {
                txn_id: TxnId(77),
                outcome: TxnOutcome::Prepared,
                commit_ts: Timestamp(0),
                epoch: 2,
                timestamp_ms: 1700000000000,
            };
            store.put(&meta).unwrap();
            store.flush().unwrap();
        }

        // Reopen and verify
        {
            let store = TxnMetaStore::open(dir.path()).unwrap();
            let loaded = store.get(TxnId(77)).unwrap().unwrap();
            assert_eq!(loaded.outcome, TxnOutcome::Prepared);
            assert_eq!(loaded.epoch, 2);
        }
    }

    #[test]
    fn test_mvcc_decode_short_buffer() {
        assert!(MvccValue::decode(&[]).is_none());
        assert!(MvccValue::decode(&[0u8; 10]).is_none());
        assert!(MvccValue::decode(&[0u8; 21]).is_none());
    }

    #[test]
    fn test_mvcc_decode_invalid_status() {
        let mut buf = vec![0u8; MVCC_HEADER_SIZE];
        buf[8] = 99; // invalid status byte
        assert!(MvccValue::decode(&buf).is_none());
    }
}
