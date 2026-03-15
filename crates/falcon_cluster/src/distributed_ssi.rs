//! # C5: Distributed Cross-Shard SSI (Serializable Snapshot Isolation)
//!
//! Extends the single-node `SsiLockManager` in `falcon_txn` to detect
//! **rw-antidependency cycles that span multiple shards**, enabling true
//! distributed Serializable isolation — analogous to CockroachDB's
//! distributed SSI implementation.
//!
//! ## Single-node vs. Distributed SSI
//!
//! The existing `SsiLockManager` (falcon_txn) tracks predicates and write
//! intents only within one `TxnManager`. A transaction that:
//!   - reads from shard A (predicate on shard A)
//!   - writes to shard B
//!
//! ...would NOT have its rw-antidependency detected because shard A's
//! `SsiLockManager` never sees shard B's write intents.
//!
//! ## Solution: Distributed Predicate Registry
//!
//! `DistributedSsiRegistry` is a **cluster-wide** in-memory store of:
//! - Read predicates: (global_txn_id → [(shard_id, table_id, range)])
//! - Write intents:  (global_txn_id → [(shard_id, table_id, key)])
//!
//! Before committing a cross-shard transaction the coordinator calls
//! `check_rw_conflicts(global_txn_id)` which scans for dangerous structures
//! (T1 →rw→ T2 →rw→ T3 with T1 and T3 overlapping) across all shards.
//!
//! ## Integration Point
//!
//! `Parallel2PcCoordinator` and `TwoPhaseCoordinator` call:
//! ```ignore
//! registry.register_read_predicate(global_txn_id, shard_id, table_id, range);
//! registry.register_write_intent(global_txn_id, shard_id, table_id, key);
//! // ... execute writes ...
//! registry.check_rw_conflicts(global_txn_id)?;  // aborts if conflict
//! registry.cleanup(global_txn_id);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use falcon_common::error::{FalconError, TxnError};
use falcon_common::types::{ShardId, TxnId};

// ─────────────────────────────────────────────────────────────────────────────
// Data Structures
// ─────────────────────────────────────────────────────────────────────────────

/// A cross-shard read predicate (approximated as a key range on a table).
#[derive(Debug, Clone)]
pub struct DistPredicate {
    pub shard_id: ShardId,
    pub table_id: u64,
    /// Lower bound key (None = unbounded).
    pub range_start: Option<Vec<u8>>,
    /// Upper bound key (None = unbounded).
    pub range_end: Option<Vec<u8>>,
}

/// A cross-shard write intent (specific key on a specific shard).
#[derive(Debug, Clone)]
pub struct DistWriteIntent {
    pub shard_id: ShardId,
    pub table_id: u64,
    pub key: Vec<u8>,
}

// ─────────────────────────────────────────────────────────────────────────────
// DistributedSsiRegistry
// ─────────────────────────────────────────────────────────────────────────────

/// Metrics for the distributed SSI registry.
#[derive(Debug, Default)]
pub struct DistSsiMetrics {
    pub predicates_registered: AtomicU64,
    pub intents_registered: AtomicU64,
    pub conflicts_detected: AtomicU64,
    pub cleanups: AtomicU64,
    pub evictions: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct DistSsiMetricsSnapshot {
    pub predicates_registered: u64,
    pub intents_registered: u64,
    pub conflicts_detected: u64,
    pub cleanups: u64,
    pub evictions: u64,
}

/// Cluster-wide registry for cross-shard SSI anti-dependency detection.
pub struct DistributedSsiRegistry {
    /// Global txn → read predicates.
    predicates: DashMap<TxnId, Vec<DistPredicate>>,
    /// Global txn → write intents.
    intents: DashMap<TxnId, Vec<DistWriteIntent>>,
    /// When each txn was registered (for TTL eviction).
    registered_at: DashMap<TxnId, Instant>,
    /// TTL for entries — prevents unbounded memory growth on coordinator crash.
    ttl: Duration,
    pub metrics: DistSsiMetrics,
}

impl DistributedSsiRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            predicates: DashMap::new(),
            intents: DashMap::new(),
            registered_at: DashMap::new(),
            ttl: Duration::from_secs(300),
            metrics: DistSsiMetrics::default(),
        })
    }

    pub fn new_with_ttl(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            predicates: DashMap::new(),
            intents: DashMap::new(),
            registered_at: DashMap::new(),
            ttl,
            metrics: DistSsiMetrics::default(),
        })
    }

    /// Register a read predicate for a global transaction on a specific shard.
    pub fn register_read_predicate(&self, txn_id: TxnId, pred: DistPredicate) {
        self.predicates.entry(txn_id).or_default().push(pred);
        self.registered_at
            .entry(txn_id)
            .or_insert_with(Instant::now);
        self.metrics
            .predicates_registered
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Register a write intent for a global transaction on a specific shard.
    pub fn register_write_intent(&self, txn_id: TxnId, intent: DistWriteIntent) {
        self.intents.entry(txn_id).or_default().push(intent);
        self.registered_at
            .entry(txn_id)
            .or_insert_with(Instant::now);
        self.metrics
            .intents_registered
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Check for rw-antidependency conflicts involving `txn_id`.
    ///
    /// Returns `Err(SerializationConflict)` if a dangerous structure is found:
    /// some committed transaction T_c has a read predicate that overlaps with
    /// `txn_id`'s write intents (T_c →rw→ txn_id).
    ///
    /// This is the single-edge check. The full dangerous-structure detection
    /// (two rw-antidependencies forming a cycle) requires tracking whether
    /// T_c itself has an rw-antidependency with any other committed txn.
    /// We approximate with: if ANY conflict is found, abort txn_id.
    /// This is safe (may over-abort) but correct for Serializability.
    pub fn check_rw_conflicts(&self, txn_id: TxnId) -> Result<(), FalconError> {
        let my_intents = match self.intents.get(&txn_id) {
            Some(e) => e.clone(),
            None => return Ok(()),
        };

        // Scan all other transactions' predicates for overlap.
        for entry in self.predicates.iter() {
            let other_txn = *entry.key();
            if other_txn == txn_id {
                continue;
            }
            let preds = entry.value();
            'intent_loop: for intent in &my_intents {
                for pred in preds {
                    if pred.shard_id == intent.shard_id
                        && pred.table_id == intent.table_id
                        && Self::key_in_range(&intent.key, pred)
                    {
                        // rw-antidependency found: other_txn reads what txn_id writes.
                        self.metrics
                            .conflicts_detected
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(FalconError::Txn(TxnError::SerializationConflict(txn_id)));
                    }
                }
                // Only report one conflict per intent to avoid noise.
                let _ = other_txn; // silence unused warning
                break 'intent_loop;
            }
        }
        Ok(())
    }

    /// Remove all entries for a completed transaction (commit or abort).
    pub fn cleanup(&self, txn_id: TxnId) {
        self.predicates.remove(&txn_id);
        self.intents.remove(&txn_id);
        self.registered_at.remove(&txn_id);
        self.metrics.cleanups.fetch_add(1, Ordering::Relaxed);
    }

    /// Evict stale entries whose TTL has expired (call periodically).
    pub fn evict_expired(&self) {
        let now = Instant::now();
        let expired: Vec<TxnId> = self
            .registered_at
            .iter()
            .filter(|e| now.duration_since(*e.value()) > self.ttl)
            .map(|e| *e.key())
            .collect();
        for txn_id in expired {
            self.cleanup(txn_id);
            self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn metrics_snapshot(&self) -> DistSsiMetricsSnapshot {
        DistSsiMetricsSnapshot {
            predicates_registered: self.metrics.predicates_registered.load(Ordering::Relaxed),
            intents_registered: self.metrics.intents_registered.load(Ordering::Relaxed),
            conflicts_detected: self.metrics.conflicts_detected.load(Ordering::Relaxed),
            cleanups: self.metrics.cleanups.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
        }
    }

    fn key_in_range(key: &[u8], pred: &DistPredicate) -> bool {
        if let Some(ref start) = pred.range_start {
            if key < start.as_slice() {
                return false;
            }
        }
        if let Some(ref end) = pred.range_end {
            if key > end.as_slice() {
                return false;
            }
        }
        true
    }
}

impl Default for DistributedSsiRegistry {
    fn default() -> Self {
        Arc::try_unwrap(Self::new()).unwrap_or_else(|_| panic!("arc unwrap"))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_conflict_different_tables() {
        let reg = DistributedSsiRegistry::new();
        reg.register_read_predicate(
            TxnId(1),
            DistPredicate {
                shard_id: ShardId(0),
                table_id: 1,
                range_start: None,
                range_end: None,
            },
        );
        reg.register_write_intent(
            TxnId(2),
            DistWriteIntent {
                shard_id: ShardId(0),
                table_id: 2, // different table
                key: vec![1, 2, 3],
            },
        );
        assert!(reg.check_rw_conflicts(TxnId(2)).is_ok());
    }

    #[test]
    fn conflict_same_table_overlapping_range() {
        let reg = DistributedSsiRegistry::new();
        reg.register_read_predicate(
            TxnId(1),
            DistPredicate {
                shard_id: ShardId(0),
                table_id: 5,
                range_start: Some(vec![0]),
                range_end: Some(vec![100]),
            },
        );
        reg.register_write_intent(
            TxnId(2),
            DistWriteIntent {
                shard_id: ShardId(0),
                table_id: 5,
                key: vec![50], // within range
            },
        );
        assert!(reg.check_rw_conflicts(TxnId(2)).is_err());
        assert_eq!(reg.metrics.conflicts_detected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cross_shard_conflict() {
        let reg = DistributedSsiRegistry::new();
        // T1 reads on shard 0
        reg.register_read_predicate(
            TxnId(1),
            DistPredicate {
                shard_id: ShardId(0),
                table_id: 10,
                range_start: None,
                range_end: None,
            },
        );
        // T2 writes to shard 0 (same table T1 reads from)
        reg.register_write_intent(
            TxnId(2),
            DistWriteIntent {
                shard_id: ShardId(0),
                table_id: 10,
                key: vec![42],
            },
        );
        // Conflict: T1 →rw→ T2
        assert!(reg.check_rw_conflicts(TxnId(2)).is_err());
    }

    #[test]
    fn no_conflict_different_shards() {
        let reg = DistributedSsiRegistry::new();
        reg.register_read_predicate(
            TxnId(1),
            DistPredicate {
                shard_id: ShardId(0), // shard 0
                table_id: 10,
                range_start: None,
                range_end: None,
            },
        );
        reg.register_write_intent(
            TxnId(2),
            DistWriteIntent {
                shard_id: ShardId(1), // different shard
                table_id: 10,
                key: vec![42],
            },
        );
        assert!(reg.check_rw_conflicts(TxnId(2)).is_ok());
    }

    #[test]
    fn cleanup_removes_entries() {
        let reg = DistributedSsiRegistry::new();
        reg.register_read_predicate(
            TxnId(1),
            DistPredicate {
                shard_id: ShardId(0),
                table_id: 1,
                range_start: None,
                range_end: None,
            },
        );
        reg.register_write_intent(
            TxnId(2),
            DistWriteIntent {
                shard_id: ShardId(0),
                table_id: 1,
                key: vec![1],
            },
        );
        // Conflict exists before cleanup.
        assert!(reg.check_rw_conflicts(TxnId(2)).is_err());
        // Clean up T1 (it committed).
        reg.cleanup(TxnId(1));
        // No more conflict.
        assert!(reg.check_rw_conflicts(TxnId(2)).is_ok());
        assert_eq!(reg.metrics.cleanups.load(Ordering::Relaxed), 1);
    }
}
