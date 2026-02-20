//! ShardReplicaGroup: manages replication for a single shard and promote/failover.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use falcon_common::error::FalconError;
use falcon_common::schema::TableSchema;
use falcon_common::types::{NodeId, ShardId, TxnId};
use falcon_storage::engine::StorageEngine;

use crate::routing::shard_map::ShardMap;

use super::catchup::{apply_wal_record_to_engine, WriteOp};
use super::replica_state::{ReplicaNode, ReplicaRole};
use super::wal_stream::{ReplicationLog, WalChunk};
use super::ReplicationMetrics;

/// Manages replication for a single shard: one primary, N replicas.
pub struct ShardReplicaGroup {
    pub shard_id: ShardId,
    pub primary: Arc<ReplicaNode>,
    pub replicas: Vec<Arc<ReplicaNode>>,
    pub log: Arc<ReplicationLog>,
    pub metrics: ReplicationMetrics,
}

impl ShardReplicaGroup {
    /// Create a new replica group with a primary and one replica.
    /// Both start with identical table schemas (empty data).
    pub fn new(shard_id: ShardId, schemas: &[TableSchema]) -> Result<Self, FalconError> {
        let primary_storage = Arc::new(StorageEngine::new_in_memory());
        let replica_storage = Arc::new(StorageEngine::new_in_memory());

        // Create tables on both nodes.
        for schema in schemas {
            primary_storage.create_table(schema.clone())?;
            replica_storage.create_table(schema.clone())?;
        }

        let primary = Arc::new(ReplicaNode::new_primary(primary_storage));
        let replica = Arc::new(ReplicaNode::new_replica(replica_storage));

        Ok(Self {
            shard_id,
            primary,
            replicas: vec![replica],
            log: Arc::new(ReplicationLog::new()),
            metrics: ReplicationMetrics::new(),
        })
    }

    /// Ship a WAL record from primary to the replication log.
    /// Returns the LSN assigned to this record.
    pub fn ship_wal_record(&self, record: falcon_storage::wal::WalRecord) -> u64 {
        self.log.append(record)
    }

    /// Apply pending WAL records to a specific replica via the replication log.
    /// Returns the number of records applied.
    pub fn catch_up_replica(&self, replica_idx: usize) -> Result<usize, FalconError> {
        let replica = self.replicas.get(replica_idx).ok_or_else(|| {
            FalconError::Internal(format!("Replica index {} out of range", replica_idx))
        })?;

        let from_lsn = replica.current_lsn();
        let pending = self.log.read_from(from_lsn);

        if pending.is_empty() {
            return Ok(0);
        }

        let count = pending.len();
        let mut max_lsn = from_lsn;

        // Apply records using the same logic as WAL recovery.
        let mut write_sets: HashMap<TxnId, Vec<WriteOp>> = HashMap::new();

        for lsn_record in &pending {
            apply_wal_record_to_engine(
                &replica.storage,
                &lsn_record.record,
                &mut write_sets,
            )?;
            if lsn_record.lsn > max_lsn {
                max_lsn = lsn_record.lsn;
            }
        }

        replica.applied_lsn.store(max_lsn, Ordering::SeqCst);

        Ok(count)
    }

    /// Apply a WalChunk (received from transport) to a replica.
    /// Returns the number of records applied.
    pub fn apply_chunk_to_replica(
        &self,
        replica_idx: usize,
        chunk: &WalChunk,
    ) -> Result<usize, FalconError> {
        if !chunk.verify_checksum() {
            return Err(FalconError::Internal("WalChunk checksum mismatch".into()));
        }
        if chunk.is_empty() {
            return Ok(0);
        }

        let replica = self.replicas.get(replica_idx).ok_or_else(|| {
            FalconError::Internal(format!("Replica index {} out of range", replica_idx))
        })?;

        let mut write_sets: HashMap<TxnId, Vec<WriteOp>> = HashMap::new();
        let mut max_lsn = replica.current_lsn();

        for lsn_record in &chunk.records {
            // Skip records already applied (idempotent).
            if lsn_record.lsn <= max_lsn {
                continue;
            }
            apply_wal_record_to_engine(
                &replica.storage,
                &lsn_record.record,
                &mut write_sets,
            )?;
            max_lsn = lsn_record.lsn;
        }

        replica.applied_lsn.store(max_lsn, Ordering::SeqCst);
        Ok(chunk.records.len())
    }

    /// Replication lag: how many LSNs behind the primary each replica is.
    pub fn replication_lag(&self) -> Vec<(usize, u64)> {
        let primary_lsn = self.log.current_lsn();
        self.replicas
            .iter()
            .enumerate()
            .map(|(i, r)| (i, primary_lsn.saturating_sub(r.current_lsn())))
            .collect()
    }

    /// Promote replica at `replica_idx` to primary.
    ///
    /// Protocol:
    /// 1. Fence old primary (mark read-only).
    /// 2. Catch up the replica to the latest LSN.
    /// 3. Swap roles: old primary becomes replica, replica becomes primary.
    /// 4. Unfence new primary (allow writes).
    /// 5. Record metrics (failover time).
    pub fn promote(&mut self, replica_idx: usize) -> Result<(), FalconError> {
        self.metrics.start_failover();
        let start = std::time::Instant::now();
        self.promote_inner(replica_idx)?;
        self.metrics.complete_failover();
        let duration_ms = start.elapsed().as_millis() as u64;
        self.primary.storage.record_failover(duration_ms);
        Ok(())
    }

    /// Promote replica and update the shard routing map atomically.
    ///
    /// After role swap, calls `shard_map.update_leader(shard_id, new_leader_node)`
    /// so that the Router directs future queries to the new primary.
    pub fn promote_with_routing(
        &mut self,
        replica_idx: usize,
        shard_map: &mut ShardMap,
        new_leader_node: NodeId,
    ) -> Result<(), FalconError> {
        self.metrics.start_failover();
        let start = std::time::Instant::now();
        self.promote_inner(replica_idx)?;
        shard_map.update_leader(self.shard_id, new_leader_node);
        self.metrics.complete_failover();
        let duration_ms = start.elapsed().as_millis() as u64;
        self.primary.storage.record_failover(duration_ms);
        Ok(())
    }

    fn promote_inner(&mut self, replica_idx: usize) -> Result<(), FalconError> {
        // Step 1: Fence old primary — no new writes accepted.
        self.primary.fence();

        // Step 2: Catch up the replica to latest LSN.
        self.catch_up_replica(replica_idx)?;

        let replica = self.replicas.get(replica_idx).ok_or_else(|| {
            FalconError::Internal(format!("Replica index {} out of range", replica_idx))
        })?;

        // Step 3: Swap roles.
        {
            let mut old_primary_role = self.primary.role.write();
            *old_primary_role = ReplicaRole::Replica;
        }
        {
            let mut new_primary_role = replica.role.write();
            *new_primary_role = ReplicaRole::Primary;
        }

        // Step 4: Swap in the data structures.
        let new_primary = Arc::clone(replica);
        let old_primary = Arc::clone(&self.primary);

        self.replicas[replica_idx] = old_primary;
        self.primary = new_primary;

        // Step 5: Unfence new primary — allow writes.
        self.primary.unfence();

        tracing::info!(
            "Shard {:?}: promoted replica {} to primary (old primary fenced)",
            self.shard_id,
            replica_idx
        );

        Ok(())
    }
}
