//! Idempotency key store — persistent deduplication for transaction-level retries.
//!
//! Provides native support for idempotency keys at the database level.
//! When a client submits a transaction with an idempotency key:
//! 1. The store is checked for a prior result with the same key.
//! 2. If found, the cached result is returned without re-executing.
//! 3. If not found, the transaction executes normally and the result is stored.
//!
//! The store is backed by a dedicated LSM instance for crash-safe persistence.
//! Entries have a configurable TTL and are garbage-collected during compaction.

use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use falcon_common::types::TxnId;

use super::compaction::CompactionConfig;
use super::engine::{LsmConfig, LsmEngine};

/// Result of a previously-executed idempotent transaction.
#[derive(Debug, Clone)]
pub struct IdempotencyResult {
    /// The idempotency key provided by the client.
    pub key: String,
    /// The transaction ID that executed this operation.
    pub txn_id: TxnId,
    /// Whether the transaction committed successfully.
    pub committed: bool,
    /// Optional result payload (e.g., affected row count, RETURNING data).
    pub result_payload: Vec<u8>,
    /// Unix timestamp (ms) when this entry was created.
    pub created_at_ms: u64,
    /// TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
}

impl IdempotencyResult {
    /// Encode to bytes for storage.
    pub fn encode(&self) -> Vec<u8> {
        let key_bytes = self.key.as_bytes();
        let key_len = key_bytes.len() as u32;
        let payload_len = self.result_payload.len() as u32;

        let total = 4 + key_bytes.len() + 8 + 1 + 4 + self.result_payload.len() + 8 + 8;
        let mut buf = Vec::with_capacity(total);

        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());
        buf.push(if self.committed { 1 } else { 0 });
        buf.extend_from_slice(&payload_len.to_le_bytes());
        buf.extend_from_slice(&self.result_payload);
        buf.extend_from_slice(&self.created_at_ms.to_le_bytes());
        buf.extend_from_slice(&self.ttl_ms.to_le_bytes());

        buf
    }

    /// Decode from bytes.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < 4 {
            return None;
        }
        let mut pos = 0;

        let key_len = u32::from_le_bytes(raw[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + key_len > raw.len() {
            return None;
        }
        let key = String::from_utf8(raw[pos..pos + key_len].to_vec()).ok()?;
        pos += key_len;

        if pos + 8 + 1 + 4 > raw.len() {
            return None;
        }
        let txn_id = TxnId(u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?));
        pos += 8;
        let committed = raw[pos] == 1;
        pos += 1;
        let payload_len = u32::from_le_bytes(raw[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        if pos + payload_len + 16 > raw.len() {
            return None;
        }
        let result_payload = raw[pos..pos + payload_len].to_vec();
        pos += payload_len;

        let created_at_ms = u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let ttl_ms = u64::from_le_bytes(raw[pos..pos + 8].try_into().ok()?);

        Some(Self {
            key,
            txn_id,
            committed,
            result_payload,
            created_at_ms,
            ttl_ms,
        })
    }

    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        if self.ttl_ms == 0 {
            return false;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now_ms > self.created_at_ms + self.ttl_ms
    }
}

/// Configuration for the idempotency store.
#[derive(Debug, Clone)]
pub struct IdempotencyConfig {
    /// Default TTL for idempotency entries in milliseconds.
    /// After this time, the entry is eligible for GC and the key can be reused.
    /// Default: 24 hours.
    pub default_ttl_ms: u64,
    /// Maximum number of entries to keep (0 = unlimited).
    pub max_entries: u64,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            default_ttl_ms: 24 * 60 * 60 * 1000, // 24 hours
            max_entries: 0,
        }
    }
}

/// Persistent idempotency key store backed by a dedicated LSM instance.
pub struct IdempotencyStore {
    engine: LsmEngine,
    config: IdempotencyConfig,
}

impl IdempotencyStore {
    /// Open or create the idempotency store.
    pub fn open(data_dir: &Path, config: IdempotencyConfig) -> io::Result<Self> {
        let store_dir = data_dir.join("idempotency");
        let lsm_config = LsmConfig {
            memtable_budget_bytes: 4 * 1024 * 1024,
            block_cache_bytes: 8 * 1024 * 1024,
            compaction: CompactionConfig {
                l0_compaction_trigger: 4,
                l0_stall_trigger: 12,
                ..Default::default()
            },
            sync_writes: true,
        };
        let engine = LsmEngine::open(&store_dir, lsm_config)?;
        Ok(Self { engine, config })
    }

    /// Check if an idempotency key has a cached result.
    /// Returns `Some(result)` if found and not expired, `None` otherwise.
    pub fn check(&self, key: &str) -> io::Result<Option<IdempotencyResult>> {
        let store_key = Self::make_key(key);
        match self.engine.get(&store_key)? {
            Some(raw) => match IdempotencyResult::decode(&raw) {
                Some(result) if !result.is_expired() => Ok(Some(result)),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Store the result of an idempotent transaction.
    /// If `ttl_ms` is 0, uses the configured default TTL.
    pub fn store(
        &self,
        key: &str,
        txn_id: TxnId,
        committed: bool,
        result_payload: Vec<u8>,
        ttl_ms: u64,
    ) -> io::Result<()> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let effective_ttl = if ttl_ms == 0 {
            self.config.default_ttl_ms
        } else {
            ttl_ms
        };

        let result = IdempotencyResult {
            key: key.to_string(),
            txn_id,
            committed,
            result_payload,
            created_at_ms: now_ms,
            ttl_ms: effective_ttl,
        };

        let store_key = Self::make_key(key);
        let value = result.encode();
        self.engine.put(&store_key, &value)
    }

    /// Remove an idempotency entry (e.g., after GC).
    pub fn remove(&self, key: &str) -> io::Result<()> {
        let store_key = Self::make_key(key);
        self.engine.delete(&store_key)
    }

    /// Flush pending writes to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.engine.flush()
    }

    /// Shutdown the store.
    pub fn shutdown(&self) -> io::Result<()> {
        self.engine.shutdown()
    }

    /// Configuration reference.
    pub fn config(&self) -> &IdempotencyConfig {
        &self.config
    }

    fn make_key(idempotency_key: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(4 + idempotency_key.len());
        k.extend_from_slice(b"idk:");
        k.extend_from_slice(idempotency_key.as_bytes());
        k
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_idempotency_result_encode_decode() {
        let result = IdempotencyResult {
            key: "req-abc-123".to_string(),
            txn_id: TxnId(42),
            committed: true,
            result_payload: b"OK: 1 row affected".to_vec(),
            created_at_ms: 1700000000000,
            ttl_ms: 86400000,
        };

        let encoded = result.encode();
        let decoded = IdempotencyResult::decode(&encoded).unwrap();

        assert_eq!(decoded.key, "req-abc-123");
        assert_eq!(decoded.txn_id, TxnId(42));
        assert!(decoded.committed);
        assert_eq!(decoded.result_payload, b"OK: 1 row affected");
        assert_eq!(decoded.created_at_ms, 1700000000000);
        assert_eq!(decoded.ttl_ms, 86400000);
    }

    #[test]
    fn test_idempotency_result_aborted() {
        let result = IdempotencyResult {
            key: "req-xyz".to_string(),
            txn_id: TxnId(99),
            committed: false,
            result_payload: b"CONFLICT".to_vec(),
            created_at_ms: 1700000000000,
            ttl_ms: 0,
        };

        let encoded = result.encode();
        let decoded = IdempotencyResult::decode(&encoded).unwrap();

        assert!(!decoded.committed);
        assert_eq!(decoded.ttl_ms, 0);
        assert!(!decoded.is_expired()); // ttl=0 means no expiry
    }

    #[test]
    fn test_idempotency_expired() {
        let result = IdempotencyResult {
            key: "old".to_string(),
            txn_id: TxnId(1),
            committed: true,
            result_payload: vec![],
            created_at_ms: 1000, // very old
            ttl_ms: 1,           // 1ms TTL
        };
        assert!(result.is_expired());
    }

    #[test]
    fn test_idempotency_not_expired() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let result = IdempotencyResult {
            key: "fresh".to_string(),
            txn_id: TxnId(1),
            committed: true,
            result_payload: vec![],
            created_at_ms: now_ms,
            ttl_ms: 60_000, // 60s TTL
        };
        assert!(!result.is_expired());
    }

    #[test]
    fn test_idempotency_store_basic() {
        let dir = TempDir::new().unwrap();
        let store = IdempotencyStore::open(dir.path(), IdempotencyConfig::default()).unwrap();

        // Initially empty
        assert!(store.check("req-1").unwrap().is_none());

        // Store a result
        store
            .store("req-1", TxnId(10), true, b"OK".to_vec(), 0)
            .unwrap();

        // Should find it
        let result = store.check("req-1").unwrap().unwrap();
        assert_eq!(result.txn_id, TxnId(10));
        assert!(result.committed);
        assert_eq!(result.result_payload, b"OK");

        // Different key still not found
        assert!(store.check("req-2").unwrap().is_none());
    }

    #[test]
    fn test_idempotency_store_duplicate_returns_same() {
        let dir = TempDir::new().unwrap();
        let store = IdempotencyStore::open(dir.path(), IdempotencyConfig::default()).unwrap();

        store
            .store("req-dup", TxnId(5), true, b"result-A".to_vec(), 0)
            .unwrap();

        // First check
        let r1 = store.check("req-dup").unwrap().unwrap();
        assert_eq!(r1.result_payload, b"result-A");

        // Second check (simulating retry) — same result
        let r2 = store.check("req-dup").unwrap().unwrap();
        assert_eq!(r2.result_payload, b"result-A");
        assert_eq!(r2.txn_id, r1.txn_id);
    }

    #[test]
    fn test_idempotency_store_remove() {
        let dir = TempDir::new().unwrap();
        let store = IdempotencyStore::open(dir.path(), IdempotencyConfig::default()).unwrap();

        store.store("req-rm", TxnId(7), true, vec![], 0).unwrap();
        assert!(store.check("req-rm").unwrap().is_some());

        store.remove("req-rm").unwrap();
        assert!(store.check("req-rm").unwrap().is_none());
    }

    #[test]
    fn test_idempotency_store_persistence() {
        let dir = TempDir::new().unwrap();

        {
            let store = IdempotencyStore::open(dir.path(), IdempotencyConfig::default()).unwrap();
            store
                .store("persist-key", TxnId(33), true, b"persisted".to_vec(), 0)
                .unwrap();
            store.flush().unwrap();
        }

        {
            let store = IdempotencyStore::open(dir.path(), IdempotencyConfig::default()).unwrap();
            let result = store.check("persist-key").unwrap().unwrap();
            assert_eq!(result.txn_id, TxnId(33));
            assert_eq!(result.result_payload, b"persisted");
        }
    }

    #[test]
    fn test_idempotency_store_expired_entry_not_returned() {
        let dir = TempDir::new().unwrap();
        let store = IdempotencyStore::open(dir.path(), IdempotencyConfig::default()).unwrap();

        // Store with 1ms TTL — will be expired by the time we check
        store
            .store("expired-key", TxnId(1), true, vec![], 1)
            .unwrap();

        // Sleep briefly to ensure expiry
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Should not be found (expired)
        assert!(store.check("expired-key").unwrap().is_none());
    }

    #[test]
    fn test_idempotency_decode_short_buffer() {
        assert!(IdempotencyResult::decode(&[]).is_none());
        assert!(IdempotencyResult::decode(&[0, 0, 0]).is_none());
    }
}
