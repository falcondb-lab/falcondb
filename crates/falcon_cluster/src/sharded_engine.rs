//! Same-process multi-shard engine for distributed execution simulation.
//!
//! Wraps N independent (StorageEngine, TxnManager) pairs, one per shard.
//! Each shard has its own MVCC state, write-sets, and WAL (if enabled).
//! This allows testing scatter/gather, two-phase agg, and replication
//! without real network overhead.

use std::path::Path;
use std::sync::Arc;

use falcon_common::datum::OwnedRow;
use falcon_common::error::FalconError;
use falcon_common::hlc::HybridClock;
use falcon_common::schema::TableSchema;
use falcon_common::types::{DataType, ShardId};
use falcon_storage::engine::StorageEngine;
use falcon_storage::wal::SyncMode;
use falcon_txn::manager::TxnManager;

use crate::sharding;

/// Result type for execute_subplan: (columns, rows).
pub type SubPlanOutput = (Vec<(String, DataType)>, Vec<OwnedRow>);

/// A single shard instance: storage + txn manager.
pub struct ShardInstance {
    pub shard_id: ShardId,
    pub storage: Arc<StorageEngine>,
    pub txn_mgr: Arc<TxnManager>,
}

/// Same-process multi-shard engine.
/// Each shard is a fully independent (StorageEngine, TxnManager) pair.
pub struct ShardedEngine {
    shards: Vec<ShardInstance>,
    num_shards: u64,
    /// Cluster-level HLC for cross-node causal ordering.
    pub hlc: Arc<HybridClock>,
    /// Whether TxnManagers use HLC (true for WAL/production, false for in-memory/test).
    hlc_enabled: bool,
}

impl ShardedEngine {
    /// Create N in-memory shards, each with independent storage and txn manager.
    pub fn new(num_shards: u64) -> Self {
        let hlc = Arc::new(HybridClock::new());
        let shards = (0..num_shards)
            .map(|i| {
                let storage = Arc::new(StorageEngine::new_in_memory());
                // In-memory shards use plain monotonic counter (test-friendly)
                let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
                ShardInstance {
                    shard_id: ShardId(i),
                    storage,
                    txn_mgr,
                }
            })
            .collect();
        Self { shards, num_shards, hlc, hlc_enabled: false }
    }

    /// Create N WAL-backed shards under `base_dir/shard_<i>/`.
    pub fn new_with_wal(
        num_shards: u64,
        base_dir: &Path,
        sync_mode: SyncMode,
        wal_mode: &str,
        no_buffering: bool,
    ) -> Result<Self, FalconError> {
        let hlc = Arc::new(HybridClock::new());
        let mut shards = Vec::with_capacity(num_shards as usize);
        for i in 0..num_shards {
            let shard_dir = base_dir.join(format!("shard_{i}"));
            std::fs::create_dir_all(&shard_dir).map_err(|e| {
                FalconError::Internal(format!("failed to create shard dir {:?}: {e}", shard_dir))
            })?;

            let has_wal = std::fs::read_dir(&shard_dir)
                .map(|entries| {
                    entries.flatten().any(|e| {
                        let s = e.file_name();
                        let s = s.to_string_lossy();
                        s.starts_with("falcon_") && s.ends_with(".wal")
                    })
                })
                .unwrap_or(false);

            let storage = if has_wal {
                StorageEngine::recover(&shard_dir).map_err(|e| {
                    FalconError::Internal(format!("shard {i} WAL recovery failed: {e}"))
                })?
            } else {
                StorageEngine::new_with_wal_mode(
                    Some(&shard_dir),
                    sync_mode,
                    wal_mode,
                    no_buffering,
                )
                .map_err(|e| {
                    FalconError::Internal(format!("shard {i} WAL init failed: {e}"))
                })?
            };
            let storage = Arc::new(storage);
            let txn_mgr = Arc::new(TxnManager::new_with_hlc(storage.clone(), hlc.clone()));

            // Advance counters past recovered state
            let max_ts = storage.recovered_max_ts.load(std::sync::atomic::Ordering::Relaxed);
            let max_txn = storage.recovered_max_txn_id.load(std::sync::atomic::Ordering::Relaxed);
            if max_ts > 0 || max_txn > 0 {
                txn_mgr.advance_counters_past(max_ts, max_txn);
            }

            shards.push(ShardInstance {
                shard_id: ShardId(i),
                storage,
                txn_mgr,
            });
        }
        Ok(Self { shards, num_shards, hlc, hlc_enabled: true })
    }

    /// Get a shard instance by ShardId.
    pub fn shard(&self, shard_id: ShardId) -> Option<&ShardInstance> {
        self.shards.iter().find(|s| s.shard_id == shard_id)
    }

    /// Get a shard instance by index (0..num_shards).
    pub fn shard_by_index(&self, idx: usize) -> Option<&ShardInstance> {
        self.shards.get(idx)
    }

    /// Number of shards.
    pub const fn num_shards(&self) -> u64 {
        self.num_shards
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards.iter().map(|s| s.shard_id).collect()
    }

    /// Create a table on ALL shards (required for cross-shard queries).
    pub fn create_table_all(&self, schema: &TableSchema) -> Result<(), FalconError> {
        for shard in &self.shards {
            shard.storage.create_table(schema.clone())?;
        }
        Ok(())
    }

    /// Shard-aware table creation:
    /// - Hash/None sharded tables: created on ALL shards (data distributed by shard key).
    /// - Reference tables: created on ALL shards (data replicated).
    ///
    /// In both cases the schema metadata is identical on every shard.
    pub fn create_table_sharded(&self, schema: &TableSchema) -> Result<(), FalconError> {
        self.create_table_all(schema)
    }

    /// Drop a table from ALL shards.
    pub fn drop_table_all(&self, table_name: &str) -> Result<(), FalconError> {
        for shard in &self.shards {
            shard.storage.drop_table(table_name)?;
        }
        Ok(())
    }

    /// Truncate a table on ALL shards.
    pub fn truncate_table_all(&self, table_name: &str) -> Result<(), FalconError> {
        for shard in &self.shards {
            shard.storage.truncate_table(table_name)?;
        }
        Ok(())
    }

    /// Deterministic shard assignment for a single i64 key (legacy).
    pub fn shard_for_key(&self, key: i64) -> ShardId {
        let hash = xxhash_rust::xxh3::xxh3_64(&key.to_le_bytes());
        ShardId(hash % self.num_shards)
    }

    /// Shard assignment for a row based on the table's shard key.
    /// Returns None for reference tables (caller must replicate to all shards).
    pub fn shard_for_row(&self, row: &OwnedRow, schema: &TableSchema) -> Option<ShardId> {
        sharding::target_shard_for_row(row, schema, self.num_shards)
    }

    /// Get the list of shards that a table's data lives on.
    /// For reference tables and hash-sharded tables, this is all shards.
    pub fn shards_for_table(&self, schema: &TableSchema) -> Vec<ShardId> {
        sharding::all_shards_for_table(schema, self.num_shards)
    }

    /// Get all shard instances.
    pub fn all_shards(&self) -> &[ShardInstance] {
        &self.shards
    }

    /// Return a consistent snapshot timestamp for cross-shard reads.
    /// In production (WAL mode with HLC), delegates to HLC for causal ordering.
    /// In test (in-memory), takes max of shard timestamps.
    pub fn consistent_snapshot_ts(&self) -> falcon_common::types::Timestamp {
        if self.hlc_enabled {
            self.hlc.now()
        } else {
            self.shards
                .iter()
                .map(|s| s.txn_mgr.current_ts())
                .max()
                .unwrap_or(falcon_common::types::Timestamp(1))
        }
    }

    /// Update the HLC on receiving a timestamp from a remote node.
    /// Call this on every cross-node RPC response to maintain causal order.
    pub fn recv_remote_ts(&self, remote_ts: falcon_common::types::Timestamp) -> falcon_common::types::Timestamp {
        self.hlc.recv(remote_ts)
    }

    /// Flush WAL on all shards. Returns the first error encountered, if any.
    pub fn flush_wal_all(&self) -> Result<(), FalconError> {
        for shard in &self.shards {
            if shard.storage.is_wal_enabled() {
                shard.storage.flush_wal().map_err(|e| {
                    FalconError::Internal(format!(
                        "shard {} WAL flush failed: {e}", shard.shard_id.0
                    ))
                })?;
            }
        }
        Ok(())
    }

    /// Shutdown WAL flush threads on all shards.
    pub fn shutdown_wal_all(&self) {
        for shard in &self.shards {
            shard.storage.shutdown_wal_flush();
        }
    }

    /// Execute a subplan function on a single shard.
    ///
    /// This is the first-class `execute_subplan(shard_id, f) -> ResultSet`
    /// abstraction required for distributed execution. In production, this
    /// would be an RPC call; here it's a direct function call on the
    /// shard's (StorageEngine, TxnManager).
    pub fn execute_subplan<F>(&self, shard_id: ShardId, f: F) -> Result<SubPlanOutput, FalconError>
    where
        F: FnOnce(&StorageEngine, &TxnManager) -> Result<SubPlanOutput, FalconError>,
    {
        let shard = self
            .shard(shard_id)
            .ok_or_else(|| FalconError::Internal(format!("Shard {shard_id:?} not found")))?;
        f(&shard.storage, &shard.txn_mgr)
    }
}
