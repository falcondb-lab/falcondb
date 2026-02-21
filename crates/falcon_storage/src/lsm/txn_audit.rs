//! Transaction-level audit log — records txn lifecycle for accountability.
//!
//! Unlike the existing `audit.rs` (which records DDL/auth events), this module
//! records **every transaction's outcome** with affected keys, enabling:
//! - Reconciliation (对账)
//! - Accountability (问责)
//! - Incident replay (事故回放)
//!
//! Each audit record is immutable and stored in a dedicated LSM instance.
//! The audit log cannot be modified by business SQL.

use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use falcon_common::types::{Timestamp, TxnId};

use super::engine::{LsmConfig, LsmEngine};
use super::compaction::CompactionConfig;

/// Outcome of a transaction for audit purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuditOutcome {
    Committed = 0,
    Aborted = 1,
    RolledBack = 2,
}

impl AuditOutcome {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(AuditOutcome::Committed),
            1 => Some(AuditOutcome::Aborted),
            2 => Some(AuditOutcome::RolledBack),
            _ => None,
        }
    }
}

impl std::fmt::Display for AuditOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditOutcome::Committed => write!(f, "committed"),
            AuditOutcome::Aborted => write!(f, "aborted"),
            AuditOutcome::RolledBack => write!(f, "rolled_back"),
        }
    }
}

/// A single transaction audit record.
#[derive(Debug, Clone)]
pub struct TxnAuditRecord {
    /// Transaction ID.
    pub txn_id: TxnId,
    /// Transaction outcome.
    pub outcome: AuditOutcome,
    /// Commit timestamp (0 if not committed).
    pub commit_ts: Timestamp,
    /// Wall-clock timestamp when the record was created (ms since epoch).
    pub timestamp_ms: u64,
    /// Number of keys affected by this transaction.
    pub affected_key_count: u32,
    /// Affected keys (primary keys, serialized). Capped to avoid unbounded growth.
    pub affected_keys: Vec<Vec<u8>>,
    /// Optional user/session identifier.
    pub user_id: String,
    /// Epoch (for distributed consistency tracking).
    pub epoch: u64,
    /// Duration of the transaction in microseconds.
    pub duration_us: u64,
}

/// Maximum number of affected keys stored per audit record.
const MAX_AUDIT_KEYS: usize = 100;

impl TxnAuditRecord {
    /// Create a new audit record with the current wall-clock time.
    pub fn new(
        txn_id: TxnId,
        outcome: AuditOutcome,
        commit_ts: Timestamp,
        affected_keys: Vec<Vec<u8>>,
        user_id: String,
        epoch: u64,
        duration_us: u64,
    ) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let key_count = affected_keys.len() as u32;
        let capped_keys: Vec<Vec<u8>> = affected_keys.into_iter().take(MAX_AUDIT_KEYS).collect();

        Self {
            txn_id,
            outcome,
            commit_ts,
            timestamp_ms: now_ms,
            affected_key_count: key_count,
            affected_keys: capped_keys,
            user_id,
            epoch,
            duration_us,
        }
    }

    /// Encode to bytes for storage.
    pub fn encode(&self) -> Vec<u8> {
        let user_bytes = self.user_id.as_bytes();
        let mut buf = Vec::with_capacity(128);

        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());       // 8
        buf.push(self.outcome as u8);                                // 1
        buf.extend_from_slice(&self.commit_ts.0.to_le_bytes());     // 8
        buf.extend_from_slice(&self.timestamp_ms.to_le_bytes());    // 8
        buf.extend_from_slice(&self.affected_key_count.to_le_bytes()); // 4
        buf.extend_from_slice(&self.epoch.to_le_bytes());           // 8
        buf.extend_from_slice(&self.duration_us.to_le_bytes());     // 8

        // User ID
        buf.extend_from_slice(&(user_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(user_bytes);

        // Affected keys (capped)
        let stored_keys = self.affected_keys.len() as u32;
        buf.extend_from_slice(&stored_keys.to_le_bytes());
        for key in &self.affected_keys {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
        }

        buf
    }

    /// Decode from bytes.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < 45 { // minimum header size
            return None;
        }
        let mut pos = 0;

        let txn_id = TxnId(u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?));
        pos += 8;
        let outcome = AuditOutcome::from_byte(raw[pos])?;
        pos += 1;
        let commit_ts = Timestamp(u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?));
        pos += 8;
        let timestamp_ms = u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let affected_key_count = u32::from_le_bytes(raw[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let epoch = u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let duration_us = u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // User ID
        if pos + 4 > raw.len() { return None; }
        let user_len = u32::from_le_bytes(raw[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + user_len > raw.len() { return None; }
        let user_id = String::from_utf8(raw[pos..pos + user_len].to_vec()).ok()?;
        pos += user_len;

        // Affected keys
        if pos + 4 > raw.len() { return None; }
        let stored_keys = u32::from_le_bytes(raw[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        let mut affected_keys = Vec::with_capacity(stored_keys);
        for _ in 0..stored_keys {
            if pos + 4 > raw.len() { break; }
            let key_len = u32::from_le_bytes(raw[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + key_len > raw.len() { break; }
            affected_keys.push(raw[pos..pos + key_len].to_vec());
            pos += key_len;
        }

        Some(Self {
            txn_id,
            outcome,
            commit_ts,
            timestamp_ms,
            affected_key_count,
            affected_keys,
            user_id,
            epoch,
            duration_us,
        })
    }

    /// Storage key: big-endian timestamp + txn_id for chronological ordering.
    pub fn storage_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        key.extend_from_slice(&self.txn_id.0.to_be_bytes());
        key
    }
}

/// Persistent transaction audit log backed by a dedicated LSM instance.
///
/// Provides immutable, append-only storage of transaction outcomes.
/// Cannot be modified by business SQL — only the engine writes to it.
pub struct TxnAuditLog {
    engine: LsmEngine,
}

impl TxnAuditLog {
    /// Open or create the transaction audit log.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let audit_dir = data_dir.join("txn_audit");
        let config = LsmConfig {
            memtable_budget_bytes: 8 * 1024 * 1024, // 8 MB
            block_cache_bytes: 16 * 1024 * 1024,    // 16 MB
            compaction: CompactionConfig {
                l0_compaction_trigger: 4,
                l0_stall_trigger: 12,
                ..Default::default()
            },
            sync_writes: false, // audit is off the hot path
        };
        let engine = LsmEngine::open(&audit_dir, config)?;
        Ok(Self { engine })
    }

    /// Record a transaction audit entry.
    pub fn record(&self, record: &TxnAuditRecord) -> io::Result<()> {
        let key = record.storage_key();
        let value = record.encode();
        self.engine.put(&key, &value)
    }

    /// Look up a specific transaction by txn_id.
    /// Note: This requires scanning since the primary key is timestamp-based.
    /// For production, a secondary index on txn_id would be needed.
    pub fn get_by_txn_id(&self, txn_id: TxnId) -> io::Result<Option<TxnAuditRecord>> {
        // For the minimal viable version, we use a direct key lookup
        // by encoding txn_id as a secondary key prefix.
        let secondary_key = Self::txn_id_key(txn_id);
        match self.engine.get(&secondary_key)? {
            Some(raw) => Ok(TxnAuditRecord::decode(&raw)),
            None => Ok(None),
        }
    }

    /// Record with both primary (timestamp) and secondary (txn_id) keys.
    pub fn record_with_index(&self, record: &TxnAuditRecord) -> io::Result<()> {
        let primary_key = record.storage_key();
        let secondary_key = Self::txn_id_key(record.txn_id);
        let value = record.encode();

        self.engine.put(&primary_key, &value)?;
        self.engine.put(&secondary_key, &value)?;
        Ok(())
    }

    /// Flush pending writes.
    pub fn flush(&self) -> io::Result<()> {
        self.engine.flush()
    }

    /// Shutdown the audit log.
    pub fn shutdown(&self) -> io::Result<()> {
        self.engine.shutdown()
    }

    fn txn_id_key(txn_id: TxnId) -> Vec<u8> {
        let mut key = Vec::with_capacity(12);
        key.extend_from_slice(b"txn:");
        key.extend_from_slice(&txn_id.0.to_be_bytes());
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_record(txn_id: u64, outcome: AuditOutcome) -> TxnAuditRecord {
        TxnAuditRecord::new(
            TxnId(txn_id),
            outcome,
            Timestamp(if outcome == AuditOutcome::Committed { 100 } else { 0 }),
            vec![b"pk_1".to_vec(), b"pk_2".to_vec()],
            "test_user".to_string(),
            1,
            500,
        )
    }

    #[test]
    fn test_audit_record_encode_decode() {
        let record = make_record(42, AuditOutcome::Committed);
        let encoded = record.encode();
        let decoded = TxnAuditRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.txn_id, TxnId(42));
        assert_eq!(decoded.outcome, AuditOutcome::Committed);
        assert_eq!(decoded.commit_ts, Timestamp(100));
        assert_eq!(decoded.affected_key_count, 2);
        assert_eq!(decoded.affected_keys.len(), 2);
        assert_eq!(decoded.affected_keys[0], b"pk_1");
        assert_eq!(decoded.affected_keys[1], b"pk_2");
        assert_eq!(decoded.user_id, "test_user");
        assert_eq!(decoded.epoch, 1);
        assert_eq!(decoded.duration_us, 500);
    }

    #[test]
    fn test_audit_record_aborted() {
        let record = make_record(99, AuditOutcome::Aborted);
        let encoded = record.encode();
        let decoded = TxnAuditRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.outcome, AuditOutcome::Aborted);
        assert_eq!(decoded.commit_ts, Timestamp(0));
    }

    #[test]
    fn test_audit_record_storage_key_ordering() {
        let r1 = TxnAuditRecord {
            txn_id: TxnId(1),
            outcome: AuditOutcome::Committed,
            commit_ts: Timestamp(0),
            timestamp_ms: 1000,
            affected_key_count: 0,
            affected_keys: vec![],
            user_id: String::new(),
            epoch: 0,
            duration_us: 0,
        };
        let r2 = TxnAuditRecord {
            timestamp_ms: 2000,
            ..r1.clone()
        };
        // Keys should be ordered chronologically
        assert!(r1.storage_key() < r2.storage_key());
    }

    #[test]
    fn test_audit_log_basic() {
        let dir = TempDir::new().unwrap();
        let log = TxnAuditLog::open(dir.path()).unwrap();

        let record = make_record(42, AuditOutcome::Committed);
        log.record_with_index(&record).unwrap();

        let loaded = log.get_by_txn_id(TxnId(42)).unwrap().unwrap();
        assert_eq!(loaded.txn_id, TxnId(42));
        assert_eq!(loaded.outcome, AuditOutcome::Committed);
        assert_eq!(loaded.affected_keys.len(), 2);
    }

    #[test]
    fn test_audit_log_not_found() {
        let dir = TempDir::new().unwrap();
        let log = TxnAuditLog::open(dir.path()).unwrap();

        assert!(log.get_by_txn_id(TxnId(999)).unwrap().is_none());
    }

    #[test]
    fn test_audit_log_multiple_records() {
        let dir = TempDir::new().unwrap();
        let log = TxnAuditLog::open(dir.path()).unwrap();

        for i in 1..=10 {
            let record = make_record(i, AuditOutcome::Committed);
            log.record_with_index(&record).unwrap();
        }

        for i in 1..=10 {
            let loaded = log.get_by_txn_id(TxnId(i)).unwrap().unwrap();
            assert_eq!(loaded.txn_id, TxnId(i));
        }
    }

    #[test]
    fn test_audit_log_persistence() {
        let dir = TempDir::new().unwrap();

        {
            let log = TxnAuditLog::open(dir.path()).unwrap();
            let record = make_record(77, AuditOutcome::Aborted);
            log.record_with_index(&record).unwrap();
            log.flush().unwrap();
        }

        {
            let log = TxnAuditLog::open(dir.path()).unwrap();
            let loaded = log.get_by_txn_id(TxnId(77)).unwrap().unwrap();
            assert_eq!(loaded.outcome, AuditOutcome::Aborted);
        }
    }

    #[test]
    fn test_audit_outcome_display() {
        assert_eq!(format!("{}", AuditOutcome::Committed), "committed");
        assert_eq!(format!("{}", AuditOutcome::Aborted), "aborted");
        assert_eq!(format!("{}", AuditOutcome::RolledBack), "rolled_back");
    }

    #[test]
    fn test_audit_record_many_keys_capped() {
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("key_{}", i).into_bytes()).collect();
        let record = TxnAuditRecord::new(
            TxnId(1), AuditOutcome::Committed, Timestamp(1),
            keys, "user".to_string(), 0, 0,
        );
        // Should be capped at MAX_AUDIT_KEYS
        assert_eq!(record.affected_keys.len(), MAX_AUDIT_KEYS);
        assert_eq!(record.affected_key_count, 200); // original count preserved
    }

    #[test]
    fn test_audit_decode_short_buffer() {
        assert!(TxnAuditRecord::decode(&[]).is_none());
        assert!(TxnAuditRecord::decode(&[0u8; 20]).is_none());
    }
}
