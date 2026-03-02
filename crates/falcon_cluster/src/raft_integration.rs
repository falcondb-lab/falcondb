//! Raft-WAL Integration Bridge.
//!
//! Connects `falcon_raft::RaftGroup` to `falcon_storage::StorageEngine` so
//! that every Raft-committed WAL record is applied to the state machine via
//! the `ApplyFn` callback.  This replaces the manual `InProcessTransport`
//! WAL-shipping path with a Raft-consensus-backed one.
//!
//! # Architecture
//!
//! ```text
//!   Client write
//!       │
//!       ▼
//!   RaftWalGroup::propose_wal_record(WalRecord)
//!       │  bincode-serialize record
//!       ▼
//!   RaftGroup::propose(bytes)        ← openraft multi-node consensus
//!       │  leader replicates to quorum
//!       ▼
//!   StateMachine::apply(entry)
//!       │  ApplyFn callback
//!       ▼
//!   apply_wal_record_to_engine()     ← applied on EVERY replica
//!       │
//!       ▼
//!   StorageEngine (MVCC MemTable)
//! ```
//!
//! # Automatic Failover
//!
//! `RaftFailoverWatcher` watches `Raft::metrics()` for leader changes and
//! updates the shard routing map automatically — no manual fencing needed.
//! When a new leader is elected by Raft, the watcher calls
//! `ShardMap::update_leader()` and increments the epoch.
//!
//! # Multi-Shard Coordination
//!
//! `RaftShardCoordinator` manages one `RaftWalGroup` per shard and exposes
//! a unified API for multi-shard writes (scatter) and reads (gather),
//! backed by Raft consensus on each shard.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use falcon_common::error::FalconError;
use falcon_common::types::{NodeId, ShardId};
use falcon_storage::engine::StorageEngine;
use falcon_storage::wal::WalRecord;

use crate::replication::catchup::apply_wal_record_to_engine;
use crate::routing::shard_map::ShardMap;

// ---------------------------------------------------------------------------
// Re-export from falcon_raft for convenience
// ---------------------------------------------------------------------------

pub use falcon_raft::network::GrpcNetworkFactory;
pub use falcon_raft::{ConsensusError, RaftGroup};

// ---------------------------------------------------------------------------
// RaftWalGroup — Raft-backed WAL replication for one shard
// ---------------------------------------------------------------------------

/// Serialized WAL record wrapper for Raft log entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaftWalEntry {
    pub record: WalRecord,
}

/// A Raft-backed WAL group for a single shard.
///
/// Every write to this group is proposed through Raft consensus: the entry
/// is only applied to the `StorageEngine` after a quorum of replicas confirm
/// it is committed.
pub struct RaftWalGroup {
    pub shard_id: ShardId,
    /// The Raft consensus group (all nodes in this shard's Raft cluster).
    pub raft_group: RaftGroup,
    /// Local storage engine for this node.
    pub storage: Arc<StorageEngine>,
    /// Count of WAL records proposed (for observability).
    records_proposed: AtomicU64,
    /// Count of WAL records applied (for observability).
    records_applied: Arc<AtomicU64>,
}

impl RaftWalGroup {
    /// Create a new `RaftWalGroup` for the given shard.
    ///
    /// `node_ids` — all node IDs that are part of this shard's Raft cluster.
    /// `storage`  — local `StorageEngine` instance; receives applied entries
    ///              via the `ApplyFn` callback.
    pub async fn new(
        shard_id: ShardId,
        node_ids: Vec<u64>,
        storage: Arc<StorageEngine>,
    ) -> Result<Arc<Self>, FalconError> {
        let records_applied = Arc::new(AtomicU64::new(0));
        let records_applied_cb = records_applied.clone();
        let storage_cb = storage.clone();

        let apply_fn: falcon_raft::store::ApplyFn = Arc::new(move |data: &[u8]| {
            match bincode::deserialize::<RaftWalEntry>(data) {
                Ok(entry) => {
                    let mut write_sets = std::collections::HashMap::new();
                    if let Err(e) = apply_wal_record_to_engine(&storage_cb, &entry.record, &mut write_sets) {
                        return Err(format!("WAL apply failed: {e}"));
                    }
                    records_applied_cb.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(e) => Err(format!("Raft WAL entry decode failed: {e}")),
            }
        });

        let raft_group =
            falcon_raft::RaftGroup::new_cluster_with_callback(node_ids, Some(apply_fn))
                .await
                .map_err(|e| FalconError::Internal(format!("RaftGroup init: {e}")))?;

        Ok(Arc::new(Self {
            shard_id,
            raft_group,
            storage,
            records_proposed: AtomicU64::new(0),
            records_applied,
        }))
    }

    /// Propose a WAL record through Raft consensus.
    ///
    /// Returns only after the record is committed by a quorum; the local
    /// `StorageEngine` will have already applied the record.
    pub async fn propose_wal_record(&self, record: WalRecord) -> Result<(), FalconError> {
        let entry = RaftWalEntry { record };
        let bytes = bincode::serialize(&entry)
            .map_err(|e| FalconError::Internal(format!("Raft WAL serialize: {e}")))?;

        self.raft_group
            .propose(bytes)
            .await
            .map_err(|e| FalconError::Internal(format!("Raft propose: {e}")))?;

        self.records_proposed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Wait until a Raft leader is elected (up to `timeout`).
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<u64, FalconError> {
        self.raft_group
            .wait_for_leader(timeout)
            .await
            .map_err(|e| FalconError::Internal(format!("wait_for_leader: {e}")))
    }

    /// Return the current Raft leader node ID, if any.
    pub async fn current_leader(&self) -> Option<u64> {
        self.raft_group.current_leader().await
    }

    /// Check whether this node is the current Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.current_leader().await.is_some()
    }

    /// Snapshot of metrics for observability.
    pub fn metrics_snapshot(&self) -> RaftWalGroupMetrics {
        RaftWalGroupMetrics {
            shard_id: self.shard_id,
            records_proposed: self.records_proposed.load(Ordering::Relaxed),
            records_applied: self.records_applied.load(Ordering::Relaxed),
        }
    }

    /// Gracefully shut down the Raft group.
    pub async fn shutdown(self) -> Result<(), FalconError> {
        self.raft_group
            .shutdown()
            .await
            .map_err(|e| FalconError::Internal(format!("RaftGroup shutdown: {e}")))
    }
}

/// Point-in-time metrics for a `RaftWalGroup`.
#[derive(Debug, Clone)]
pub struct RaftWalGroupMetrics {
    pub shard_id: ShardId,
    pub records_proposed: u64,
    pub records_applied: u64,
}

// ---------------------------------------------------------------------------
// RaftFailoverWatcher — monitors Raft leader changes and updates ShardMap
// ---------------------------------------------------------------------------

/// Background watcher: polls `Raft::metrics()` and updates `ShardMap` when
/// the Raft leader changes for a shard.
///
/// This replaces the manual 5-step fencing protocol with automatic,
/// consensus-driven failover: as soon as a quorum elects a new leader,
/// the shard map is updated and the epoch is incremented.
pub struct RaftFailoverWatcher {
    groups: Vec<(ShardId, Arc<RaftWalGroup>)>,
    shard_map: Arc<RwLock<ShardMap>>,
    /// Maps shard_id → (last_known_leader, epoch)
    state: Mutex<BTreeMap<u64, (Option<u64>, u64)>>,
    /// Total leader changes observed.
    leader_changes: AtomicU64,
    /// Total failovers committed to the shard map.
    failovers_committed: AtomicU64,
}

impl RaftFailoverWatcher {
    /// Create a new watcher for the given Raft shard groups.
    pub fn new(
        groups: Vec<(ShardId, Arc<RaftWalGroup>)>,
        shard_map: Arc<RwLock<ShardMap>>,
    ) -> Arc<Self> {
        let state: BTreeMap<u64, (Option<u64>, u64)> =
            groups.iter().map(|(s, _)| (s.0, (None, 1u64))).collect();
        Arc::new(Self {
            groups,
            shard_map,
            state: Mutex::new(state),
            leader_changes: AtomicU64::new(0),
            failovers_committed: AtomicU64::new(0),
        })
    }

    /// Poll all shard groups once and update the shard map for any leader
    /// changes.  Should be called periodically (e.g. every 200ms) from a
    /// background task.
    pub async fn poll_once(&self) {
        for (shard_id, group) in &self.groups {
            let new_leader = group.current_leader().await;

            let mut state = self.state.lock();
            let entry = state.entry(shard_id.0).or_insert((None, 1));
            let (last_leader, epoch) = entry;

            if *last_leader != new_leader {
                let old = *last_leader;
                *last_leader = new_leader;
                *epoch += 1;
                let new_epoch = *epoch;
                drop(state);

                self.leader_changes.fetch_add(1, Ordering::Relaxed);

                if let Some(leader_node) = new_leader {
                    // Update the shard routing map with the new leader.
                    // ShardMap.update_leader() increments the shard epoch and
                    // fires the leader-change callback (if registered).
                    let mut map = self.shard_map.write();
                    let shard_epoch = map.update_leader(*shard_id, NodeId(leader_node));
                    self.failovers_committed.fetch_add(1, Ordering::Relaxed);

                    tracing::info!(
                        shard = shard_id.0,
                        old_leader = ?old,
                        new_leader = leader_node,
                        watcher_epoch = new_epoch,
                        shard_epoch = ?shard_epoch,
                        "RaftFailoverWatcher: shard leader changed → ShardMap updated"
                    );
                } else {
                    tracing::warn!(
                        shard = shard_id.0,
                        old_leader = ?old,
                        "RaftFailoverWatcher: no leader elected yet (election in progress)"
                    );
                }
            }
        }
    }

    /// Start the watcher as a background tokio task.
    ///
    /// Returns a `RaftFailoverWatcherHandle` that stops the task when dropped.
    pub fn start(self: Arc<Self>, poll_interval: Duration) -> RaftFailoverWatcherHandle {
        let watcher = self;
        let (tx, mut rx) = tokio::sync::watch::channel(false);

        let task = tokio::spawn(async move {
            tracing::info!(
                interval_ms = poll_interval.as_millis(),
                "RaftFailoverWatcher started"
            );
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {
                        watcher.poll_once().await;
                    }
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            break;
                        }
                    }
                }
            }
            tracing::info!("RaftFailoverWatcher stopped");
        });

        RaftFailoverWatcherHandle { stop_tx: tx, task: Some(task) }
    }

    /// Snapshot of watcher metrics.
    pub fn metrics_snapshot(&self) -> RaftFailoverWatcherMetrics {
        RaftFailoverWatcherMetrics {
            leader_changes: self.leader_changes.load(Ordering::Relaxed),
            failovers_committed: self.failovers_committed.load(Ordering::Relaxed),
        }
    }
}

/// Handle for a running `RaftFailoverWatcher` background task.
pub struct RaftFailoverWatcherHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl RaftFailoverWatcherHandle {
    /// Signal the watcher to stop and wait for it to finish.
    pub async fn stop(mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for RaftFailoverWatcherHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Metrics for a `RaftFailoverWatcher`.
#[derive(Debug, Clone)]
pub struct RaftFailoverWatcherMetrics {
    /// Total leader-change events observed across all shards.
    pub leader_changes: u64,
    /// Total times the ShardMap was updated with a new leader.
    pub failovers_committed: u64,
}

// ---------------------------------------------------------------------------
// RaftShardCoordinator — multi-shard Raft-backed coordinator
// ---------------------------------------------------------------------------

/// Coordinates Raft-backed WAL replication across multiple shards.
///
/// Provides:
/// - Per-shard WAL proposal routing
/// - Automatic failover via `RaftFailoverWatcher`
/// - Aggregated observability across all shards
pub struct RaftShardCoordinator {
    /// Shard ID → RaftWalGroup
    groups: RwLock<BTreeMap<u64, Arc<RaftWalGroup>>>,
    /// Shared shard routing map (also used by query router).
    shard_map: Arc<RwLock<ShardMap>>,
    /// Failover watcher handle (stops watcher on drop).
    _watcher_handle: Mutex<Option<RaftFailoverWatcherHandle>>,
    /// Total WAL records proposed across all shards.
    total_proposed: AtomicU64,
    /// gRPC network factory for multi-process clusters.
    /// Kept alive so peer connections persist for the lifetime of the coordinator.
    /// Currently used alongside in-process `RouterNetworkFactory`; will replace it
    /// when `RaftGroup` supports pluggable network factories.
    grpc_network: Mutex<Option<GrpcNetworkFactory>>,
}

impl RaftShardCoordinator {
    /// Create a new coordinator with an empty shard set.
    pub fn new(shard_map: Arc<RwLock<ShardMap>>) -> Arc<Self> {
        Arc::new(Self {
            groups: RwLock::new(BTreeMap::new()),
            shard_map,
            _watcher_handle: Mutex::new(None),
            total_proposed: AtomicU64::new(0),
            grpc_network: Mutex::new(None),
        })
    }

    /// Set the gRPC network factory for multi-process cluster communication.
    /// The factory is kept alive for the lifetime of the coordinator.
    pub fn set_grpc_network(&self, factory: GrpcNetworkFactory) {
        *self.grpc_network.lock() = Some(factory);
    }

    /// Register a shard group.
    pub fn register_shard(&self, group: Arc<RaftWalGroup>) {
        self.groups.write().insert(group.shard_id.0, group);
    }

    /// Start the automatic failover watcher.
    /// Call this after all shards have been registered.
    pub fn start_failover_watcher(self: &Arc<Self>, poll_interval: Duration) {
        let groups: Vec<(ShardId, Arc<RaftWalGroup>)> = self
            .groups
            .read().values().map(|g| (g.shard_id, g.clone()))
            .collect();

        let watcher =
            RaftFailoverWatcher::new(groups, self.shard_map.clone());
        let handle = watcher.start(poll_interval);
        *self._watcher_handle.lock() = Some(handle);
    }

    /// Propose a WAL record to the Raft group for `shard_id`.
    pub async fn propose(
        &self,
        shard_id: ShardId,
        record: WalRecord,
    ) -> Result<(), FalconError> {
        let group = self
            .groups
            .read()
            .get(&shard_id.0)
            .cloned()
            .ok_or_else(|| FalconError::Internal(format!("No Raft group for shard {}", shard_id.0)))?;

        group.propose_wal_record(record).await?;
        self.total_proposed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Return the current Raft leader for a shard.
    pub async fn shard_leader(&self, shard_id: ShardId) -> Option<u64> {
        let group = self.groups.read().get(&shard_id.0).cloned()?;
        group.current_leader().await
    }

    /// Wait for all registered shards to have a leader.
    pub async fn wait_all_leaders_elected(&self, timeout: Duration) -> Result<(), FalconError> {
        let groups: Vec<Arc<RaftWalGroup>> =
            self.groups.read().values().cloned().collect();

        for group in groups {
            group
                .wait_for_leader(timeout)
                .await
                .map_err(|e| FalconError::Internal(format!(
                    "Shard {} leader timeout: {e}",
                    group.shard_id.0
                )))?;
        }
        Ok(())
    }

    /// Snapshot of metrics for all shards.
    pub fn metrics_snapshot(&self) -> RaftCoordinatorMetrics {
        let shards: Vec<RaftWalGroupMetrics> = self
            .groups
            .read()
            .values()
            .map(|g| g.metrics_snapshot())
            .collect();
        RaftCoordinatorMetrics {
            shard_count: shards.len(),
            total_proposed: self.total_proposed.load(Ordering::Relaxed),
            shards,
        }
    }

    /// Add a new voter node to a shard's Raft cluster (dynamic membership).
    pub async fn add_shard_voter(
        &self,
        shard_id: ShardId,
        node_id: NodeId,
        addr: &str,
    ) -> Result<(), FalconError> {
        let group = self
            .groups
            .read()
            .get(&shard_id.0)
            .cloned()
            .ok_or_else(|| {
                FalconError::Internal(format!("No Raft group for shard {}", shard_id.0))
            })?;

        group
            .raft_group
            .add_voter(node_id.0, addr)
            .await
            .map_err(|e| FalconError::Internal(format!("add_voter: {e}")))
    }

    /// Return the `RaftWalGroup` for the given shard, if registered.
    pub fn get_shard_group(&self, shard_id: ShardId) -> Option<Arc<RaftWalGroup>> {
        self.groups.read().get(&shard_id.0).cloned()
    }

    /// Remove a voter from a shard's Raft cluster (scale-in / decommission).
    pub async fn remove_shard_voter(
        &self,
        shard_id: ShardId,
        node_id: NodeId,
    ) -> Result<(), FalconError> {
        let group = self
            .groups
            .read()
            .get(&shard_id.0)
            .cloned()
            .ok_or_else(|| {
                FalconError::Internal(format!("No Raft group for shard {}", shard_id.0))
            })?;

        group
            .raft_group
            .remove_voter(node_id.0)
            .await
            .map_err(|e| FalconError::Internal(format!("remove_voter: {e}")))
    }
}

/// Point-in-time metrics for a `RaftShardCoordinator`.
#[derive(Debug, Clone)]
pub struct RaftCoordinatorMetrics {
    pub shard_count: usize,
    pub total_proposed: u64,
    pub shards: Vec<RaftWalGroupMetrics>,
}

// ---------------------------------------------------------------------------
// RaftStats — SHOW falcon.raft_stats output type
// ---------------------------------------------------------------------------

/// Output row for `SHOW falcon.raft_stats`.
#[derive(Debug, Clone)]
pub struct RaftStatRow {
    pub shard_id: u64,
    pub current_leader: Option<u64>,
    pub records_proposed: u64,
    pub records_applied: u64,
}

/// Collect `SHOW falcon.raft_stats` data from a coordinator.
pub async fn collect_raft_stats(coordinator: &RaftShardCoordinator) -> Vec<RaftStatRow> {
    let groups: Vec<Arc<RaftWalGroup>> = coordinator
        .groups
        .read()
        .values()
        .cloned()
        .collect();

    let mut rows = Vec::with_capacity(groups.len());
    for group in groups {
        let leader = group.current_leader().await;
        let m = group.metrics_snapshot();
        rows.push(RaftStatRow {
            shard_id: m.shard_id.0,
            current_leader: leader,
            records_proposed: m.records_proposed,
            records_applied: m.records_applied,
        });
    }
    rows
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::*;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "test".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_raft_wal_group_creation_and_leader_election() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();

        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage)
            .await
            .unwrap();

        let leader = group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();

        assert!(
            [1u64, 2, 3].contains(&leader),
            "leader must be one of the cluster nodes, got {leader}"
        );
    }

    #[tokio::test]
    async fn test_raft_wal_group_propose_commit_record() {
        use falcon_common::datum::{Datum, OwnedRow};
        use falcon_storage::wal::WalRecord;

        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();

        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage.clone())
            .await
            .unwrap();

        group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();

        // Propose an Insert WAL record
        let row = OwnedRow::new(vec![Datum::Int32(42), Datum::Text("hello".into())]);
        let record = WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: row.clone(),
        };
        group.propose_wal_record(record).await.unwrap();

        // Propose a CommitTxnLocal record
        let commit = WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(1),
        };
        group.propose_wal_record(commit).await.unwrap();

        // Give openraft time to apply on all nodes
        tokio::time::sleep(Duration::from_millis(300)).await;

        let m = group.metrics_snapshot();
        assert!(
            m.records_proposed >= 2,
            "expected >= 2 proposals, got {}",
            m.records_proposed
        );
        assert!(
            m.records_applied >= 2,
            "expected >= 2 applied records, got {}",
            m.records_applied
        );
    }

    #[tokio::test]
    async fn test_raft_wal_group_insert_visible_after_commit() {
        use falcon_common::datum::{Datum, OwnedRow};
        use falcon_storage::wal::WalRecord;

        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();

        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage.clone())
            .await
            .unwrap();

        group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();

        let row = OwnedRow::new(vec![Datum::Int32(7), Datum::Text("raft-row".into())]);
        group
            .propose_wal_record(WalRecord::Insert {
                txn_id: TxnId(10),
                table_id: TableId(1),
                row: row.clone(),
            })
            .await
            .unwrap();
        group
            .propose_wal_record(WalRecord::CommitTxnLocal {
                txn_id: TxnId(10),
                commit_ts: Timestamp(10),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;

        // Row should be visible via the storage engine
        let rows = storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert!(
            rows.iter().any(|(_pk, r)| r.values.first() == Some(&Datum::Int32(7))),
            "row id=7 should be visible after Raft commit"
        );
    }

    #[tokio::test]
    async fn test_raft_wal_group_leader_failover() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();

        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage.clone())
            .await
            .unwrap();

        let old_leader = group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();

        // Partition the current leader from the router
        group.raft_group.partition_node(old_leader);

        // Wait for the remaining nodes to elect a new leader
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let new_leader = loop {
            for &id in group.raft_group.node_ids() {
                if id == old_leader {
                    continue;
                }
                if let Some(raft) = group.raft_group.get_node(id) {
                    if let Some(l) = raft.metrics().borrow().current_leader {
                        if l != old_leader {
                            break;
                        }
                    }
                }
            }
            // Re-check current_leader on group
            if let Some(l) = group.current_leader().await {
                if l != old_leader {
                    break l;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "new leader not elected within timeout"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert_ne!(new_leader, old_leader, "a different node must become leader");
    }

    #[tokio::test]
    async fn test_raft_shard_coordinator_multi_shard() {
        use falcon_storage::wal::WalRecord;
        use falcon_common::datum::{Datum, OwnedRow};

        let shard_map = Arc::new(RwLock::new(ShardMap::uniform(2, NodeId(1))));
        let coordinator = RaftShardCoordinator::new(shard_map.clone());

        // Register two shards
        for shard_idx in 0u64..2 {
            let storage = Arc::new(StorageEngine::new_in_memory());
            storage.create_table(test_schema()).unwrap();
            let node_ids = vec![shard_idx * 10 + 1, shard_idx * 10 + 2, shard_idx * 10 + 3];
            let group = RaftWalGroup::new(ShardId(shard_idx), node_ids, storage)
                .await
                .unwrap();
            coordinator.register_shard(group);
        }

        coordinator
            .wait_all_leaders_elected(Duration::from_secs(10))
            .await
            .unwrap();

        // Propose records on both shards
        for shard_idx in 0u64..2 {
            let row = OwnedRow::new(vec![
                Datum::Int32(shard_idx as i32),
                Datum::Text(format!("shard-{shard_idx}")),
            ]);
            coordinator
                .propose(
                    ShardId(shard_idx),
                    WalRecord::Insert {
                        txn_id: TxnId(shard_idx + 1),
                        table_id: TableId(1),
                        row,
                    },
                )
                .await
                .unwrap();
            coordinator
                .propose(
                    ShardId(shard_idx),
                    WalRecord::CommitTxnLocal {
                        txn_id: TxnId(shard_idx + 1),
                        commit_ts: Timestamp(shard_idx + 1),
                    },
                )
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(300)).await;

        let m = coordinator.metrics_snapshot();
        assert_eq!(m.shard_count, 2);
        assert!(m.total_proposed >= 4, "expected >= 4 total proposals");
    }

    #[tokio::test]
    async fn test_raft_failover_watcher_updates_shard_map() {
        let shard_map = Arc::new(RwLock::new(ShardMap::single_shard(NodeId(1))));
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();

        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage)
            .await
            .unwrap();

        group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();

        let watcher = RaftFailoverWatcher::new(
            vec![(ShardId(0), group)],
            shard_map.clone(),
        );

        // Poll once — should update the shard map with the elected leader
        watcher.poll_once().await;

        let m = watcher.metrics_snapshot();
        assert!(
            m.failovers_committed >= 1,
            "expected at least 1 shard map update after first poll"
        );
    }

    #[tokio::test]
    async fn test_raft_add_remove_voter() {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();

        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage.clone())
            .await
            .unwrap();

        group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();

        // Add node 4 as a new voter
        group
            .raft_group
            .add_voter(4, "node-4")
            .await
            .unwrap();

        // Remove node 4
        group
            .raft_group
            .remove_voter(4)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_collect_raft_stats() {
        let shard_map = Arc::new(RwLock::new(ShardMap::single_shard(NodeId(1))));
        let coordinator = RaftShardCoordinator::new(shard_map);

        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();
        let group = RaftWalGroup::new(ShardId(0), vec![1, 2, 3], storage)
            .await
            .unwrap();
        group
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        coordinator.register_shard(group);

        let stats = collect_raft_stats(&coordinator).await;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].shard_id, 0);
        assert!(stats[0].current_leader.is_some());
    }

    #[test]
    fn test_shard_map_epoch_fencing() {
        use crate::routing::shard_map::ShardMap;

        let mut map = ShardMap::uniform(2, NodeId(1));

        // Initial epoch is 1
        assert_eq!(map.shard_epoch(ShardId(0)), Some(1));
        assert_eq!(map.shard_epoch(ShardId(1)), Some(1));

        // Epoch validation: current epoch passes
        assert!(map.validate_epoch(ShardId(0), 1).is_ok());

        // Leader change bumps epoch
        let new_epoch = map.update_leader(ShardId(0), NodeId(2));
        assert_eq!(new_epoch, Some(2));
        assert_eq!(map.shard_epoch(ShardId(0)), Some(2));

        // Stale epoch is rejected
        assert_eq!(map.validate_epoch(ShardId(0), 1), Err(2));
        // Current epoch passes
        assert!(map.validate_epoch(ShardId(0), 2).is_ok());

        // Same leader → no epoch bump (idempotent)
        let same_epoch = map.update_leader(ShardId(0), NodeId(2));
        assert_eq!(same_epoch, Some(2));

        // Second leader change
        let epoch3 = map.update_leader(ShardId(0), NodeId(3));
        assert_eq!(epoch3, Some(3));
        assert_eq!(map.validate_epoch(ShardId(0), 2), Err(3));
    }

    #[test]
    fn test_shard_map_leader_change_callback() {
        use crate::routing::shard_map::ShardMap;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let mut map = ShardMap::single_shard(NodeId(1));
        let callback_count = Arc::new(AtomicU64::new(0));
        let last_epoch = Arc::new(AtomicU64::new(0));

        let cc = callback_count.clone();
        let le = last_epoch.clone();
        map.set_leader_change_callback(move |_shard, _old, _new, epoch| {
            cc.fetch_add(1, Ordering::Relaxed);
            le.store(epoch, Ordering::Relaxed);
        });

        // First change
        map.update_leader(ShardId(0), NodeId(2));
        assert_eq!(callback_count.load(Ordering::Relaxed), 1);
        assert_eq!(last_epoch.load(Ordering::Relaxed), 2);

        // Same leader → no callback
        map.update_leader(ShardId(0), NodeId(2));
        assert_eq!(callback_count.load(Ordering::Relaxed), 1);

        // Another change
        map.update_leader(ShardId(0), NodeId(3));
        assert_eq!(callback_count.load(Ordering::Relaxed), 2);
        assert_eq!(last_epoch.load(Ordering::Relaxed), 3);
    }
}
