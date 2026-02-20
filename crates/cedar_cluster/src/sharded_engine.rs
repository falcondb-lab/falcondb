//! Same-process multi-shard engine for distributed execution simulation.
//!
//! Wraps N independent (StorageEngine, TxnManager) pairs, one per shard.
//! Each shard has its own MVCC state, write-sets, and WAL (if enabled).
//! This allows testing scatter/gather, two-phase agg, and replication
//! without real network overhead.

use std::sync::Arc;

use cedar_common::datum::OwnedRow;
use cedar_common::error::CedarError;
use cedar_common::schema::TableSchema;
use cedar_common::types::{DataType, ShardId};
use cedar_storage::engine::StorageEngine;
use cedar_txn::manager::TxnManager;

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
}

impl ShardedEngine {
    /// Create N in-memory shards, each with independent storage and txn manager.
    pub fn new(num_shards: u64) -> Self {
        let shards = (0..num_shards)
            .map(|i| {
                let storage = Arc::new(StorageEngine::new_in_memory());
                let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
                ShardInstance {
                    shard_id: ShardId(i),
                    storage,
                    txn_mgr,
                }
            })
            .collect();
        Self { shards, num_shards }
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
    pub fn num_shards(&self) -> u64 {
        self.num_shards
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards.iter().map(|s| s.shard_id).collect()
    }

    /// Create a table on ALL shards (required for cross-shard queries).
    pub fn create_table_all(&self, schema: &TableSchema) -> Result<(), CedarError> {
        for shard in &self.shards {
            shard.storage.create_table(schema.clone())?;
        }
        Ok(())
    }

    /// Shard-aware table creation:
    /// - Hash/None sharded tables: created on ALL shards (data distributed by shard key).
    /// - Reference tables: created on ALL shards (data replicated).
    /// In both cases the schema metadata is identical on every shard.
    pub fn create_table_sharded(&self, schema: &TableSchema) -> Result<(), CedarError> {
        self.create_table_all(schema)
    }

    /// Drop a table from ALL shards.
    pub fn drop_table_all(&self, table_name: &str) -> Result<(), CedarError> {
        for shard in &self.shards {
            shard.storage.drop_table(table_name)?;
        }
        Ok(())
    }

    /// Truncate a table on ALL shards.
    pub fn truncate_table_all(&self, table_name: &str) -> Result<(), CedarError> {
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

    /// Execute a subplan function on a single shard.
    ///
    /// This is the first-class `execute_subplan(shard_id, f) -> ResultSet`
    /// abstraction required for distributed execution. In production, this
    /// would be an RPC call; here it's a direct function call on the
    /// shard's (StorageEngine, TxnManager).
    pub fn execute_subplan<F>(
        &self,
        shard_id: ShardId,
        f: F,
    ) -> Result<SubPlanOutput, CedarError>
    where
        F: FnOnce(
            &StorageEngine,
            &TxnManager,
        ) -> Result<SubPlanOutput, CedarError>,
    {
        let shard = self.shard(shard_id).ok_or_else(|| {
            CedarError::Internal(format!("Shard {:?} not found", shard_id))
        })?;
        f(&shard.storage, &shard.txn_mgr)
    }
}
