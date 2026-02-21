use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::{Catalog, TableSchema};
use falcon_common::config::{NodeRole, WritePathEnforcement};
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memory::{MemoryBudget, MemoryTracker};
use crate::online_ddl::OnlineDdlManager;
use crate::stats::TableStatistics;

use crate::columnstore::ColumnStoreTable;
use crate::disk_rowstore::DiskRowstoreTable;
use crate::memtable::{MemTable, PrimaryKey};
use crate::wal::{CheckpointData, SyncMode, WalRecord, WalWriter};

#[derive(Debug, Clone)]
pub(crate) struct TxnWriteOp {
    pub(crate) table_id: TableId,
    pub(crate) pk: PrimaryKey,
}

/// Metadata for a named secondary index.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub table_id: TableId,
    pub table_name: String,
    pub column_idx: usize,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TxnReadOp {
    pub(crate) table_id: TableId,
    pub(crate) pk: PrimaryKey,
}

/// Lightweight replication / failover metrics stored in the engine so the
/// protocol handler can expose them via `SHOW falcon.replication_stats`.
/// Updated by the cluster layer after each promote operation.
#[derive(Debug)]
pub struct ReplicationStats {
    promote_count: AtomicU64,
    last_failover_time_ms: AtomicU64,
    /// P1-3: Total leader changes observed (includes initial promotion).
    leader_changes: AtomicU64,
    /// P1-3: Current replication lag in microseconds (primary→replica).
    replication_lag_us: AtomicU64,
    /// P1-3: Maximum observed replication lag in microseconds.
    max_replication_lag_us: AtomicU64,
}

impl Default for ReplicationStats {
    fn default() -> Self {
        Self {
            promote_count: AtomicU64::new(0),
            last_failover_time_ms: AtomicU64::new(0),
            leader_changes: AtomicU64::new(0),
            replication_lag_us: AtomicU64::new(0),
            max_replication_lag_us: AtomicU64::new(0),
        }
    }
}

/// Immutable snapshot of replication metrics.
#[derive(Debug, Clone, Default)]
pub struct ReplicationStatsSnapshot {
    pub promote_count: u64,
    pub last_failover_time_ms: u64,
    /// P1-3: Total leader changes observed.
    pub leader_changes: u64,
    /// P1-3: Current replication lag in microseconds.
    pub replication_lag_us: u64,
    /// P1-3: Maximum observed replication lag in microseconds.
    pub max_replication_lag_us: u64,
}

impl ReplicationStats {
    /// Record a completed failover.
    pub fn record_failover(&self, duration_ms: u64) {
        self.promote_count.fetch_add(1, AtomicOrdering::Relaxed);
        self.last_failover_time_ms.store(duration_ms, AtomicOrdering::Relaxed);
        self.leader_changes.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// P1-3: Update the current replication lag.
    pub fn update_replication_lag(&self, lag_us: u64) {
        self.replication_lag_us.store(lag_us, AtomicOrdering::Relaxed);
        // Update max via CAS loop
        let mut current_max = self.max_replication_lag_us.load(AtomicOrdering::Relaxed);
        while lag_us > current_max {
            match self.max_replication_lag_us.compare_exchange_weak(
                current_max, lag_us, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    pub fn snapshot(&self) -> ReplicationStatsSnapshot {
        ReplicationStatsSnapshot {
            promote_count: self.promote_count.load(AtomicOrdering::Relaxed),
            last_failover_time_ms: self.last_failover_time_ms.load(AtomicOrdering::Relaxed),
            leader_changes: self.leader_changes.load(AtomicOrdering::Relaxed),
            replication_lag_us: self.replication_lag_us.load(AtomicOrdering::Relaxed),
            max_replication_lag_us: self.max_replication_lag_us.load(AtomicOrdering::Relaxed),
        }
    }
}

/// WAL write statistics (lock-free atomics).
#[derive(Debug)]
pub struct WalStats {
    pub(crate) records_written: AtomicU64,
    pub(crate) observer_notifications: AtomicU64,
    pub(crate) flushes: AtomicU64,
    /// P1-2: Cumulative fsync latency in microseconds.
    pub(crate) fsync_total_us: AtomicU64,
    /// P1-2: Max single fsync latency in microseconds.
    pub(crate) fsync_max_us: AtomicU64,
    /// P1-2: Total records batched across all group commits.
    pub(crate) group_commit_records: AtomicU64,
    /// P1-2: Number of group commit batches.
    pub(crate) group_commit_batches: AtomicU64,
    /// P1-2: Current WAL backlog bytes (written but not yet replicated).
    pub(crate) backlog_bytes: AtomicU64,
}

impl Default for WalStats {
    fn default() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            observer_notifications: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            fsync_total_us: AtomicU64::new(0),
            fsync_max_us: AtomicU64::new(0),
            group_commit_records: AtomicU64::new(0),
            group_commit_batches: AtomicU64::new(0),
            backlog_bytes: AtomicU64::new(0),
        }
    }
}

/// Immutable snapshot of WAL statistics.
#[derive(Debug, Clone, Default)]
pub struct WalStatsSnapshot {
    pub records_written: u64,
    pub observer_notifications: u64,
    pub flushes: u64,
    /// P1-2: Cumulative fsync latency in microseconds.
    pub fsync_total_us: u64,
    /// P1-2: Max single fsync latency in microseconds.
    pub fsync_max_us: u64,
    /// P1-2: Average fsync latency in microseconds (0 if no flushes).
    pub fsync_avg_us: u64,
    /// P1-2: Average group commit batch size (records per flush).
    pub group_commit_avg_size: f64,
    /// P1-2: Current WAL backlog bytes.
    pub backlog_bytes: u64,
}

impl WalStats {
    fn snapshot(&self) -> WalStatsSnapshot {
        let flushes = self.flushes.load(AtomicOrdering::Relaxed);
        let fsync_total = self.fsync_total_us.load(AtomicOrdering::Relaxed);
        let gc_batches = self.group_commit_batches.load(AtomicOrdering::Relaxed);
        let gc_records = self.group_commit_records.load(AtomicOrdering::Relaxed);
        WalStatsSnapshot {
            records_written: self.records_written.load(AtomicOrdering::Relaxed),
            observer_notifications: self.observer_notifications.load(AtomicOrdering::Relaxed),
            flushes,
            fsync_total_us: fsync_total,
            fsync_max_us: self.fsync_max_us.load(AtomicOrdering::Relaxed),
            fsync_avg_us: if flushes > 0 { fsync_total / flushes } else { 0 },
            group_commit_avg_size: if gc_batches > 0 { gc_records as f64 / gc_batches as f64 } else { 0.0 },
            backlog_bytes: self.backlog_bytes.load(AtomicOrdering::Relaxed),
        }
    }

    /// Record an fsync operation with its latency.
    #[allow(dead_code)]
    pub(crate) fn record_fsync(&self, latency_us: u64) {
        self.fsync_total_us.fetch_add(latency_us, AtomicOrdering::Relaxed);
        // Update max via CAS loop
        let mut current_max = self.fsync_max_us.load(AtomicOrdering::Relaxed);
        while latency_us > current_max {
            match self.fsync_max_us.compare_exchange_weak(
                current_max, latency_us, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Record a group commit batch.
    #[allow(dead_code)]
    pub(crate) fn record_group_commit(&self, batch_size: u64) {
        self.group_commit_records.fetch_add(batch_size, AtomicOrdering::Relaxed);
        self.group_commit_batches.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

/// Callback type for observing WAL writes (used by replication layer).
pub type WalObserver = Box<dyn Fn(&WalRecord) + Send + Sync>;

/// Tracks per-replica ack timestamps for GC safepoint computation.
/// The GC safepoint must not reclaim versions that replicas have not yet applied.
#[derive(Debug, Default)]
pub struct ReplicaAckTracker {
    /// Per-replica ack timestamp: replica_id -> last acked timestamp.
    /// Uses a DashMap for lock-free concurrent updates from multiple replica streams.
    ack_timestamps: DashMap<usize, AtomicU64>,
}

impl ReplicaAckTracker {
    pub fn new() -> Self {
        Self {
            ack_timestamps: DashMap::new(),
        }
    }

    /// Update the ack timestamp for a replica. Called when the replica confirms
    /// it has applied WAL records up to `ack_ts`.
    pub fn update_ack(&self, replica_id: usize, ack_ts: u64) {
        match self.ack_timestamps.get(&replica_id) {
            Some(entry) => {
                // Only advance forward
                let current = entry.value().load(AtomicOrdering::Relaxed);
                if ack_ts > current {
                    entry.value().store(ack_ts, AtomicOrdering::Relaxed);
                }
            }
            None => {
                self.ack_timestamps.insert(replica_id, AtomicU64::new(ack_ts));
            }
        }
    }

    /// Remove a replica (e.g. when it disconnects or is decommissioned).
    pub fn remove_replica(&self, replica_id: usize) {
        self.ack_timestamps.remove(&replica_id);
    }

    /// Compute the minimum ack timestamp across all tracked replicas.
    /// Returns `Timestamp::MAX` if no replicas are registered (no constraint).
    pub fn min_replica_safe_ts(&self) -> Timestamp {
        if self.ack_timestamps.is_empty() {
            return Timestamp(u64::MAX);
        }
        let min_ts = self
            .ack_timestamps
            .iter()
            .map(|entry| entry.value().load(AtomicOrdering::Relaxed))
            .min()
            .unwrap_or(u64::MAX);
        Timestamp(min_ts)
    }

    /// Number of tracked replicas.
    pub fn replica_count(&self) -> usize {
        self.ack_timestamps.len()
    }

    /// Snapshot of all replica ack timestamps for observability.
    pub fn snapshot(&self) -> Vec<(usize, u64)> {
        self.ack_timestamps
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(AtomicOrdering::Relaxed)))
            .collect()
    }
}

/// The storage engine. Owns all tables (rowstore, columnstore, disk), the catalog, and the WAL.
pub struct StorageEngine {
    /// Runtime node role — controls which storage paths are permitted.
    /// Primary nodes are forbidden from using columnstore/disk paths on the write side.
    pub(crate) node_role: NodeRole,
    /// Rowstore tables (in-memory, default).
    pub(crate) tables: DashMap<TableId, Arc<MemTable>>,
    /// Columnstore tables (columnar, analytics-optimised).
    pub(crate) columnstore_tables: DashMap<TableId, Arc<ColumnStoreTable>>,
    /// Disk-based rowstore tables (B-tree on disk).
    pub(crate) disk_tables: DashMap<TableId, Arc<DiskRowstoreTable>>,
    /// Data directory for disk-based tables (None = in-memory only).
    pub(crate) data_dir: Option<PathBuf>,
    /// Schema catalog (protected by RwLock for DDL).
    pub(crate) catalog: RwLock<Catalog>,
    /// WAL writer (None if WAL disabled).
    pub(crate) wal: Option<Arc<WalWriter>>,
    /// Per-transaction write-set for key-scoped commit/abort.
    pub(crate) txn_write_sets: DashMap<TxnId, Vec<TxnWriteOp>>,
    /// Per-transaction read-set for OCC validation at commit time.
    pub(crate) txn_read_sets: DashMap<TxnId, Vec<TxnReadOp>>,
    /// Cumulative GC statistics (lock-free atomics).
    pub(crate) gc_stats: crate::gc::GcStats,
    /// GC configuration.
    pub(crate) gc_config: crate::gc::GcConfig,
    /// Replication / failover metrics (lock-free atomics).
    pub(crate) replication_stats: ReplicationStats,
    /// WAL write statistics (lock-free atomics).
    pub(crate) wal_stats: WalStats,
    /// Optional WAL observer for replication (called after every WAL append).
    pub(crate) wal_observer: Option<WalObserver>,
    /// Per-table statistics collected by ANALYZE.
    pub(crate) table_stats: DashMap<TableId, TableStatistics>,
    /// Named sequences: name -> current value (atomic).
    pub(crate) sequences: DashMap<String, AtomicI64>,
    /// Named index registry: index_name (lowercase) -> IndexMeta.
    pub(crate) index_registry: DashMap<String, IndexMeta>,
    /// Shard-local memory tracker for backpressure.
    pub(crate) memory_tracker: MemoryTracker,
    /// Online DDL operation tracker.
    pub(crate) online_ddl: OnlineDdlManager,
    /// Internal TxnId counter for auto-commit helpers (insert_row, delete_row).
    /// Uses a high range (starting at 1<<60) to avoid collisions with user txns.
    pub(crate) internal_txn_counter: AtomicU64,
    /// Tracks per-replica ack timestamps for GC safepoint computation.
    pub(crate) replica_ack_tracker: ReplicaAckTracker,
    /// Counter: number of times a write-txn path touched columnstore (should be 0 on Primary).
    pub(crate) write_path_columnstore_violations: AtomicU64,
    /// Counter: number of times a write-txn path touched disk-rowstore (should be 0 on Primary).
    pub(crate) write_path_disk_violations: AtomicU64,
    /// Enforcement level for write-path purity violations on Primary nodes.
    pub(crate) write_path_enforcement: WritePathEnforcement,
}

impl StorageEngine {
    /// Create a new storage engine. If `wal_dir` is Some, WAL is enabled.
    pub fn new(wal_dir: Option<&Path>) -> Result<Self, StorageError> {
        let wal = if let Some(dir) = wal_dir {
            Some(Arc::new(WalWriter::open(dir, SyncMode::FDataSync)?))
        } else {
            None
        };

        Ok(Self {
            node_role: NodeRole::Standalone,
            tables: DashMap::new(),
            columnstore_tables: DashMap::new(),
            disk_tables: DashMap::new(),
            data_dir: wal_dir.map(|d| d.to_path_buf()),
            catalog: RwLock::new(Catalog::new()),
            wal,
            txn_write_sets: DashMap::new(),
            txn_read_sets: DashMap::new(),
            gc_stats: crate::gc::GcStats::new(),
            gc_config: crate::gc::GcConfig::default(),
            replication_stats: ReplicationStats::default(),
            wal_stats: WalStats::default(),
            wal_observer: None,
            table_stats: DashMap::new(),
            sequences: DashMap::new(),
            index_registry: DashMap::new(),
            memory_tracker: MemoryTracker::unlimited(),
            online_ddl: OnlineDdlManager::new(),
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            write_path_columnstore_violations: AtomicU64::new(0),
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
        })
    }

    /// Set the node role. Must be called before any DML.
    pub fn set_node_role(&mut self, role: NodeRole) {
        self.node_role = role;
    }

    /// Current node role.
    pub fn node_role(&self) -> NodeRole {
        self.node_role
    }

    /// Number of OLTP write-path columnstore violations (should be 0 on Primary).
    pub fn write_path_columnstore_violation_count(&self) -> u64 {
        self.write_path_columnstore_violations.load(AtomicOrdering::Relaxed)
    }

    /// Number of OLTP write-path disk-rowstore violations (should be 0 on Primary).
    pub fn write_path_disk_violation_count(&self) -> u64 {
        self.write_path_disk_violations.load(AtomicOrdering::Relaxed)
    }

    /// Set the write-path enforcement level.
    /// Call this after `set_node_role` to configure production enforcement.
    pub fn set_write_path_enforcement(&mut self, enforcement: WritePathEnforcement) {
        self.write_path_enforcement = enforcement;
    }

    /// Current write-path enforcement level.
    pub fn write_path_enforcement(&self) -> WritePathEnforcement {
        self.write_path_enforcement
    }

    /// Test helper: create a WAL-backed engine with custom WAL options.
    /// Useful for forcing segment rotation in checkpoint/WAL tests.
    #[cfg(test)]
    pub(crate) fn new_with_wal_options(
        wal_dir: &Path,
        sync_mode: SyncMode,
        max_segment_size: u64,
        group_commit_size: usize,
    ) -> Result<Self, StorageError> {
        let wal = Some(Arc::new(WalWriter::open_with_options(
            wal_dir,
            sync_mode,
            max_segment_size,
            group_commit_size,
        )?));

        Ok(Self {
            node_role: NodeRole::Standalone,
            tables: DashMap::new(),
            columnstore_tables: DashMap::new(),
            disk_tables: DashMap::new(),
            data_dir: Some(wal_dir.to_path_buf()),
            catalog: RwLock::new(Catalog::new()),
            wal,
            txn_write_sets: DashMap::new(),
            txn_read_sets: DashMap::new(),
            gc_stats: crate::gc::GcStats::new(),
            gc_config: crate::gc::GcConfig::default(),
            replication_stats: ReplicationStats::default(),
            wal_stats: WalStats::default(),
            wal_observer: None,
            table_stats: DashMap::new(),
            sequences: DashMap::new(),
            index_registry: DashMap::new(),
            memory_tracker: MemoryTracker::unlimited(),
            online_ddl: OnlineDdlManager::new(),
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            write_path_columnstore_violations: AtomicU64::new(0),
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
        })
    }

    /// Create a new in-memory-only storage engine (no WAL).
    pub fn new_in_memory() -> Self {
        Self {
            node_role: NodeRole::Standalone,
            tables: DashMap::new(),
            columnstore_tables: DashMap::new(),
            disk_tables: DashMap::new(),
            data_dir: None,
            catalog: RwLock::new(Catalog::new()),
            wal: None,
            txn_write_sets: DashMap::new(),
            txn_read_sets: DashMap::new(),
            gc_stats: crate::gc::GcStats::new(),
            gc_config: crate::gc::GcConfig::default(),
            replication_stats: ReplicationStats::default(),
            wal_stats: WalStats::default(),
            wal_observer: None,
            table_stats: DashMap::new(),
            sequences: DashMap::new(),
            index_registry: DashMap::new(),
            memory_tracker: MemoryTracker::unlimited(),
            online_ddl: OnlineDdlManager::new(),
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            write_path_columnstore_violations: AtomicU64::new(0),
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
        }
    }

    /// Create a new in-memory-only storage engine with a memory budget.
    pub fn new_in_memory_with_budget(budget: MemoryBudget) -> Self {
        Self {
            node_role: NodeRole::Standalone,
            tables: DashMap::new(),
            columnstore_tables: DashMap::new(),
            disk_tables: DashMap::new(),
            data_dir: None,
            catalog: RwLock::new(Catalog::new()),
            wal: None,
            txn_write_sets: DashMap::new(),
            txn_read_sets: DashMap::new(),
            gc_stats: crate::gc::GcStats::new(),
            gc_config: crate::gc::GcConfig::default(),
            replication_stats: ReplicationStats::default(),
            wal_stats: WalStats::default(),
            wal_observer: None,
            table_stats: DashMap::new(),
            sequences: DashMap::new(),
            index_registry: DashMap::new(),
            memory_tracker: MemoryTracker::new(budget),
            online_ddl: OnlineDdlManager::new(),
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            write_path_columnstore_violations: AtomicU64::new(0),
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
        }
    }

    /// Set a WAL observer callback for replication. Called after every WAL append.
    /// The observer receives a reference to the WAL record that was just written.
    pub fn set_wal_observer(&mut self, observer: WalObserver) {
        self.wal_observer = Some(observer);
    }

    /// Append a record to the WAL (if enabled) and notify the observer (if set).
    /// This is the single entry point for all WAL writes, ensuring replication
    /// always sees every record.
    pub(crate) fn append_wal(&self, record: &WalRecord) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            wal.append(record)?;
        }
        self.wal_stats.records_written.fetch_add(1, AtomicOrdering::Relaxed);
        if let Some(ref observer) = self.wal_observer {
            observer(record);
            self.wal_stats.observer_notifications.fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok(())
    }

    /// Append + flush the WAL (for DDL and commit records that need durability).
    pub(crate) fn append_and_flush_wal(&self, record: &WalRecord) -> Result<(), StorageError> {
        self.append_wal(record)?;
        if let Some(ref wal) = self.wal {
            wal.flush()?;
        }
        self.wal_stats.flushes.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Record a completed failover event (called by cluster layer).
    pub fn record_failover(&self, duration_ms: u64) {
        self.replication_stats.record_failover(duration_ms);
    }

    /// Get a snapshot of replication / failover metrics.
    pub fn replication_stats_snapshot(&self) -> ReplicationStatsSnapshot {
        self.replication_stats.snapshot()
    }

    /// Access the online DDL manager for status queries.
    pub fn online_ddl(&self) -> &OnlineDdlManager {
        &self.online_ddl
    }

    /// Get a snapshot of WAL write statistics.
    pub fn wal_stats_snapshot(&self) -> WalStatsSnapshot {
        self.wal_stats.snapshot()
    }

    /// Check whether WAL is enabled for this engine.
    pub fn is_wal_enabled(&self) -> bool {
        self.wal.is_some()
    }

    /// Access the replica ack tracker for updating/querying replica timestamps.
    pub fn replica_ack_tracker(&self) -> &ReplicaAckTracker {
        &self.replica_ack_tracker
    }

    /// Set GC configuration.
    pub fn set_gc_config(&mut self, config: crate::gc::GcConfig) {
        self.gc_config = config;
    }

    /// Get a snapshot of cumulative GC statistics.
    pub fn gc_stats_snapshot(&self) -> crate::gc::GcStatsSnapshot {
        self.gc_stats.snapshot()
    }

    /// Get a reference to the internal GC stats (for use by GcRunner).
    pub fn gc_stats_snapshot_ref(&self) -> &crate::gc::GcStats {
        &self.gc_stats
    }

    /// Run a GC sweep using the engine's own config and stats.
    /// `watermark` should be computed from `compute_safepoint()`.
    /// Returns the sweep result.
    pub fn gc_sweep(&self, watermark: Timestamp) -> crate::gc::GcSweepResult {
        self.run_gc_with_config(watermark, &self.gc_config, &self.gc_stats)
    }

    pub(crate) fn record_write(&self, txn_id: TxnId, table_id: TableId, pk: PrimaryKey) {
        let mut entry = self.txn_write_sets.entry(txn_id).or_default();
        // Deduplicate: skip if this (table_id, pk) is already recorded
        if !entry.iter().any(|op| op.table_id == table_id && op.pk == pk) {
            entry.push(TxnWriteOp { table_id, pk });
        }
    }

    pub(crate) fn record_read(&self, txn_id: TxnId, table_id: TableId, pk: PrimaryKey) {
        self.txn_read_sets
            .entry(txn_id)
            .or_default()
            .push(TxnReadOp { table_id, pk });
    }

    pub(crate) fn take_read_set(&self, txn_id: TxnId) -> Vec<TxnReadOp> {
        self.txn_read_sets
            .remove(&txn_id)
            .map(|(_, reads)| reads)
            .unwrap_or_default()
    }

    pub(crate) fn take_write_set(&self, txn_id: TxnId) -> Vec<TxnWriteOp> {
        self.txn_write_sets
            .remove(&txn_id)
            .map(|(_, writes)| writes)
            .unwrap_or_default()
    }

    /// Return the current write-set length for a transaction.
    /// Used by the session layer to capture a savepoint snapshot.
    pub fn write_set_snapshot(&self, txn_id: TxnId) -> usize {
        self.txn_write_sets
            .get(&txn_id)
            .map(|ws| ws.len())
            .unwrap_or(0)
    }

    /// Rollback all writes performed after `snapshot_len` for the given transaction.
    /// Aborts the MVCC versions for those keys and truncates the write-set.
    pub fn rollback_write_set_after(&self, txn_id: TxnId, snapshot_len: usize) {
        let mut entry = match self.txn_write_sets.get_mut(&txn_id) {
            Some(e) => e,
            None => return,
        };
        if entry.len() <= snapshot_len {
            return;
        }
        // Collect the ops to abort (everything after snapshot_len)
        let ops_to_abort: Vec<TxnWriteOp> = entry.drain(snapshot_len..).collect();
        drop(entry); // release DashMap lock before touching tables

        // Abort the MVCC versions for those keys
        self.apply_abort_to_write_set(txn_id, &ops_to_abort);
    }

    /// Rollback the read-set to a snapshot point (truncate entries after snapshot_len).
    pub fn rollback_read_set_after(&self, txn_id: TxnId, snapshot_len: usize) {
        if let Some(mut entry) = self.txn_read_sets.get_mut(&txn_id) {
            entry.truncate(snapshot_len);
        }
    }

    /// Return the current read-set length for a transaction.
    pub fn read_set_snapshot(&self, txn_id: TxnId) -> usize {
        self.txn_read_sets
            .get(&txn_id)
            .map(|rs| rs.len())
            .unwrap_or(0)
    }

    pub(crate) fn apply_commit_to_write_set(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        write_set: &[TxnWriteOp],
    ) -> Result<(), StorageError> {
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table
                .entry(op.table_id)
                .or_default()
                .push(op.pk.clone());
        }

        for (table_id, keys) in &keys_by_table {
            if let Some(table) = self.tables.get(table_id) {
                table.value().commit_keys(txn_id, commit_ts, keys)?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_abort_to_write_set(&self, txn_id: TxnId, write_set: &[TxnWriteOp]) {
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table
                .entry(op.table_id)
                .or_default()
                .push(op.pk.clone());
        }

        for (table_id, keys) in keys_by_table {
            if let Some(table) = self.tables.get(&table_id) {
                table.value().abort_keys(txn_id, &keys);
            }
        }
    }

    // ── Catalog access ───────────────────────────────────────────────

    pub fn get_catalog(&self) -> Catalog {
        self.catalog.read().clone()
    }

    /// Alias for `get_catalog()` — used by the shard rebalancer.
    pub fn catalog_snapshot(&self) -> Catalog {
        self.get_catalog()
    }

    /// Get a reference to the MemTable by TableId.
    pub fn get_table(&self, id: TableId) -> Option<Arc<MemTable>> {
        self.tables.get(&id).map(|t| t.value().clone())
    }

    pub fn get_table_schema(&self, name: &str) -> Option<TableSchema> {
        self.catalog.read().find_table(name).cloned()
    }

    pub fn get_table_schema_by_id(&self, id: TableId) -> Option<TableSchema> {
        self.catalog.read().find_table_by_id(id).cloned()
    }

    /// Return the set of indexed columns for a table: Vec<(column_idx, unique)>.
    pub fn get_indexed_columns(&self, table_id: TableId) -> Vec<(usize, bool)> {
        if let Some(table) = self.tables.get(&table_id) {
            let indexes = table.secondary_indexes.read();
            indexes.iter().map(|idx| (idx.column_idx, idx.unique)).collect()
        } else {
            vec![]
        }
    }

    // ── Table statistics (ANALYZE) ─────────────────────────────────

    /// Collect statistics for a table by scanning all committed rows.
    /// Uses a snapshot read at the latest timestamp visible to a fresh reader.
    pub fn analyze_table(&self, table_name: &str) -> Result<TableStatistics, StorageError> {
        let schema = self.catalog.read().find_table(table_name).cloned()
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table = self.tables.get(&schema.id)
            .ok_or(StorageError::TableNotFound(schema.id))?;

        // Snapshot scan: use a sentinel txn/ts to read all committed data
        let read_txn = TxnId(u64::MAX);
        let read_ts = Timestamp(u64::MAX);
        let rows: Vec<Vec<falcon_common::datum::Datum>> = table
            .scan(read_txn, read_ts)
            .into_iter()
            .map(|(_pk, row)| row.values)
            .collect();

        let columns: Vec<(usize, String)> = schema.columns.iter().enumerate()
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let column_stats = crate::stats::collect_column_stats(&columns, &rows);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let stats = TableStatistics {
            table_id: schema.id,
            table_name: schema.name.clone(),
            row_count: rows.len() as u64,
            column_stats,
            last_analyzed_ms: now_ms,
        };

        self.table_stats.insert(schema.id, stats.clone());
        Ok(stats)
    }

    /// Get cached statistics for a table (collected by last ANALYZE).
    pub fn get_table_stats(&self, table_id: TableId) -> Option<TableStatistics> {
        self.table_stats.get(&table_id).map(|v| v.value().clone())
    }

    /// Get cached statistics for all tables.
    pub fn get_all_table_stats(&self) -> Vec<TableStatistics> {
        self.table_stats.iter().map(|e| e.value().clone()).collect()
    }

    /// Allocate a synthetic internal TxnId that won't collide with user txns.
    pub(crate) fn next_internal_txn_id(&self) -> TxnId {
        TxnId(self.internal_txn_counter.fetch_add(1, AtomicOrdering::Relaxed))
    }

    // ── Txn lifecycle ────────────────────────────────────────────────

    /// Unified commit entry point. Dispatches to local or global commit
    /// based on `txn_type`. Upper layers MUST use this instead of calling
    /// `commit_txn_local` / `commit_txn_global` directly.
    pub fn commit_txn(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        txn_type: TxnType,
    ) -> Result<(), StorageError> {
        match txn_type {
            TxnType::Local => self.commit_txn_local(txn_id, commit_ts),
            TxnType::Global => self.commit_txn_global(txn_id, commit_ts),
        }
    }

    /// Unified abort entry point. Dispatches to local or global abort
    /// based on `txn_type`. Upper layers MUST use this instead of calling
    /// `abort_txn_local` / `abort_txn_global` directly.
    pub fn abort_txn(&self, txn_id: TxnId, txn_type: TxnType) -> Result<(), StorageError> {
        match txn_type {
            TxnType::Local => self.abort_txn_local(txn_id),
            TxnType::Global => self.abort_txn_global(txn_id),
        }
    }

    pub fn prepare_txn(&self, txn_id: TxnId) -> Result<(), StorageError> {
        self.append_and_flush_wal(&WalRecord::PrepareTxn { txn_id })?;
        Ok(())
    }

    /// Validate the read-set for OCC under Snapshot Isolation.
    /// Returns Ok(()) if no read key was modified by a concurrent committed
    /// transaction after `start_ts`. Returns Err if a conflict is detected.
    pub fn validate_read_set(&self, txn_id: TxnId, start_ts: Timestamp) -> Result<(), StorageError> {
        if let Some(read_set) = self.txn_read_sets.get(&txn_id) {
            for op in read_set.value() {
                if let Some(table) = self.tables.get(&op.table_id) {
                    if table.has_committed_write_after(&op.pk, txn_id, start_ts) {
                        return Err(StorageError::SerializationFailure);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn commit_txn_local(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);

        // Memory accounting: on commit, write-buffer bytes become committed MVCC bytes.
        // We estimate based on write-set size. The actual bytes were tracked at insert/update time.
        let ws_bytes = self.estimate_write_set_bytes(txn_id, &write_set);
        self.memory_tracker.dealloc_write_buffer(ws_bytes);
        self.memory_tracker.alloc_mvcc(ws_bytes);

        // Commit-time unique constraint re-validation + MVCC commit + index update.
        // If validation fails, abort the write-set and propagate the error.
        if let Err(e) = self.apply_commit_to_write_set(txn_id, commit_ts, &write_set) {
            // Undo the accounting transition
            self.memory_tracker.dealloc_mvcc(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &write_set);
            return Err(e);
        }

        self.append_and_flush_wal(&WalRecord::CommitTxnLocal { txn_id, commit_ts })?;

        Ok(())
    }

    pub(crate) fn commit_txn_global(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);

        // Memory accounting: write-buffer → mvcc transition
        let ws_bytes = self.estimate_write_set_bytes(txn_id, &write_set);
        self.memory_tracker.dealloc_write_buffer(ws_bytes);
        self.memory_tracker.alloc_mvcc(ws_bytes);

        // Commit-time unique constraint re-validation + MVCC commit + index update.
        // If validation fails, abort the write-set and propagate the error.
        if let Err(e) = self.apply_commit_to_write_set(txn_id, commit_ts, &write_set) {
            self.memory_tracker.dealloc_mvcc(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &write_set);
            return Err(e);
        }

        self.append_and_flush_wal(&WalRecord::CommitTxnGlobal { txn_id, commit_ts })?;

        Ok(())
    }

    pub(crate) fn abort_txn_local(&self, txn_id: TxnId) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);

        // Memory accounting: aborted writes free the write-buffer allocation
        let ws_bytes = self.estimate_write_set_bytes(txn_id, &write_set);
        self.memory_tracker.dealloc_write_buffer(ws_bytes);

        self.apply_abort_to_write_set(txn_id, &write_set);

        self.append_wal(&WalRecord::AbortTxnLocal { txn_id })?;

        Ok(())
    }

    pub(crate) fn abort_txn_global(&self, txn_id: TxnId) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);

        // Memory accounting: aborted writes free the write-buffer allocation
        let ws_bytes = self.estimate_write_set_bytes(txn_id, &write_set);
        self.memory_tracker.dealloc_write_buffer(ws_bytes);

        self.apply_abort_to_write_set(txn_id, &write_set);

        self.append_wal(&WalRecord::AbortTxnGlobal { txn_id })?;

        Ok(())
    }

    /// Estimate total bytes for a write-set by looking up the version data.
    fn estimate_write_set_bytes(&self, _txn_id: TxnId, write_set: &[TxnWriteOp]) -> u64 {
        let mut total = 0u64;
        for op in write_set {
            if let Some(table) = self.tables.get(&op.table_id) {
                if let Some(chain) = table.data.get(&op.pk) {
                    // Use the head version's estimated size
                    let head = chain.head.read();
                    if let Some(ref ver) = *head {
                        total += crate::mvcc::VersionChain::estimate_version_bytes(ver);
                    }
                }
            }
        }
        total
    }

    /// Flush the WAL (for group commit).
    pub fn flush_wal(&self) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            wal.flush()?;
        }
        Ok(())
    }

    // ── Garbage Collection ──────────────────────────────────────────

    /// Run GC on all tables: truncate version chains older than `watermark`.
    /// Safe to call with `watermark` = min(start_ts of all active txns).
    /// Returns the number of version chains processed.
    pub fn run_gc(&self, watermark: Timestamp) -> usize {
        let config = crate::gc::GcConfig::default();
        let stats = crate::gc::GcStats::new();
        let result = self.run_gc_with_config(watermark, &config, &stats);
        result.chains_inspected as usize
    }

    /// Run a full GC sweep across all tables with configuration and stats.
    /// This is the primary GC entry point for production use.
    ///
    /// - Does NOT hold any table-wide or engine-wide lock.
    /// - Iterates tables then keys, acquiring per-chain locks individually.
    /// - Respects `config.batch_size` (per table) and `config.min_chain_length`.
    /// - Returns aggregate `GcSweepResult` across all tables.
    pub fn run_gc_with_config(
        &self,
        watermark: Timestamp,
        config: &crate::gc::GcConfig,
        stats: &crate::gc::GcStats,
    ) -> crate::gc::GcSweepResult {
        let start = std::time::Instant::now();
        let mut aggregate = crate::gc::GcSweepResult {
            safepoint_ts: watermark,
            ..Default::default()
        };

        for table_ref in self.tables.iter() {
            let memtable = table_ref.value();
            let table_result = crate::gc::sweep_memtable(memtable, watermark, config, stats);
            aggregate.chains_inspected += table_result.chains_inspected;
            aggregate.chains_pruned += table_result.chains_pruned;
            aggregate.reclaimed_versions += table_result.reclaimed_versions;
            aggregate.reclaimed_bytes += table_result.reclaimed_bytes;
            aggregate.keys_skipped += table_result.keys_skipped;
        }

        // Memory accounting: reclaimed bytes reduce MVCC memory
        if aggregate.reclaimed_bytes > 0 {
            self.memory_tracker.dealloc_mvcc(aggregate.reclaimed_bytes);
        }

        aggregate.sweep_duration_us = start.elapsed().as_micros() as u64;
        aggregate
    }

    /// Get the total number of version chain entries across all tables.
    /// Useful for observability (memory pressure estimation).
    pub fn total_chain_count(&self) -> usize {
        self.tables.iter().map(|t| t.value().data.len()).sum()
    }

    // ── Memory Backpressure ─────────────────────────────────────────

    /// Get a reference to the shard-local memory tracker.
    pub fn memory_tracker(&self) -> &MemoryTracker {
        &self.memory_tracker
    }

    /// Get the current pressure state.
    pub fn pressure_state(&self) -> crate::memory::PressureState {
        self.memory_tracker.pressure_state()
    }

    /// Get a snapshot of memory usage for observability.
    pub fn memory_snapshot(&self) -> crate::memory::MemorySnapshot {
        self.memory_tracker.snapshot()
    }

    /// Set the memory budget (e.g. from config at startup).
    pub fn set_memory_budget(&mut self, budget: MemoryBudget) {
        self.memory_tracker = MemoryTracker::new(budget);
    }

    // ── Checkpoint ───────────────────────────────────────────────────

    /// Create a checkpoint: snapshot all committed data to a checkpoint file.
    /// After checkpoint, old WAL segments before the current one can be purged.
    /// Returns (wal_segment_id, row_count) on success.
    pub fn checkpoint(&self) -> Result<(u64, usize), StorageError> {
        let wal = self.wal.as_ref().ok_or_else(|| {
            StorageError::Serialization("Cannot checkpoint without WAL".into())
        })?;

        // Flush any pending WAL writes first
        wal.flush()?;

        let segment_id = wal.current_segment_id();
        let lsn = wal.current_lsn();

        // Snapshot the catalog
        let catalog = self.catalog.read().clone();

        // Snapshot all committed rows from all tables.
        // We use a high read_ts and a dummy txn to read only committed data.
        let read_ts = Timestamp(u64::MAX - 1);
        let dummy_txn = TxnId(0);
        let mut table_data = Vec::new();
        let mut total_rows = 0;
        for table_ref in self.tables.iter() {
            let table_id = *table_ref.key();
            let memtable = table_ref.value();
            let rows: Vec<(Vec<u8>, OwnedRow)> = memtable.scan(dummy_txn, read_ts);
            total_rows += rows.len();
            table_data.push((table_id, rows));
        }

        let ckpt = CheckpointData {
            catalog,
            table_data,
            wal_segment_id: segment_id,
            wal_lsn: lsn,
        };

        // Get WAL dir from the writer
        let wal_dir = wal.wal_dir();
        ckpt.write_to_dir(&wal_dir)?;

        // Write checkpoint marker to WAL
        wal.append(&WalRecord::Checkpoint {
            timestamp: Timestamp(lsn),
        })?;
        wal.flush()?;

        // Purge WAL segments older than this checkpoint base segment.
        // Recovery replays from `segment_id` onward.
        let purged_segments = wal.purge_segments_before(segment_id)?;

        tracing::info!(
            "Checkpoint complete: segment={}, lsn={}, rows={}, purged_segments={}",
            segment_id, lsn, total_rows, purged_segments
        );
        Ok((segment_id, total_rows))
    }

    /// Create an in-memory checkpoint snapshot (no disk write).
    /// Used by the gRPC GetCheckpoint RPC to stream checkpoint data to replicas.
    pub fn snapshot_checkpoint_data(&self) -> CheckpointData {
        let catalog = self.catalog.read().clone();

        let read_ts = Timestamp(u64::MAX - 1);
        let dummy_txn = TxnId(0);
        let mut table_data = Vec::new();
        for table_ref in self.tables.iter() {
            let table_id = *table_ref.key();
            let memtable = table_ref.value();
            let rows: Vec<(Vec<u8>, OwnedRow)> = memtable.scan(dummy_txn, read_ts);
            table_data.push((table_id, rows));
        }

        let (segment_id, lsn) = match self.wal.as_ref() {
            Some(wal) => (wal.current_segment_id(), wal.current_lsn()),
            None => (0, 0),
        };

        CheckpointData {
            catalog,
            table_data,
            wal_segment_id: segment_id,
            wal_lsn: lsn,
        }
    }

    /// Apply a `CheckpointData` snapshot to this engine, replacing all in-memory state.
    ///
    /// Used by replicas during bootstrap: after downloading a checkpoint from the
    /// primary via `GrpcTransport::download_checkpoint`, call this to initialize
    /// the replica's storage, then subscribe to WAL from `ckpt.wal_lsn`.
    ///
    /// This method is safe to call on a freshly-created in-memory engine.
    /// It clears all existing tables and catalog before applying the checkpoint.
    pub fn apply_checkpoint_data(&self, ckpt: &CheckpointData) -> Result<(), StorageError> {
        // 1. Replace catalog
        {
            let mut catalog = self.catalog.write();
            *catalog = ckpt.catalog.clone();
        }

        // 2. Clear existing tables
        self.tables.clear();

        // 3. Restore table data from checkpoint
        for (table_id, rows) in &ckpt.table_data {
            let schema = self.catalog.read().find_table_by_id(*table_id).cloned();
            if let Some(schema) = schema {
                let memtable = Arc::new(MemTable::new(schema));
                let sentinel_txn = TxnId(0);
                let sentinel_ts = Timestamp(1);
                for (pk, row) in rows {
                    let chain = Arc::new(crate::mvcc::VersionChain::new());
                    chain.prepend(sentinel_txn, Some(row.clone()));
                    chain.commit(sentinel_txn, sentinel_ts);
                    memtable.data.insert(pk.clone(), chain);
                }
                // Rebuild secondary indexes from restored data
                memtable.rebuild_secondary_indexes();
                self.tables.insert(*table_id, memtable);
            }
        }

        tracing::info!(
            "Checkpoint applied: {} tables, wal_lsn={}, wal_segment={}",
            ckpt.table_data.len(),
            ckpt.wal_lsn,
            ckpt.wal_segment_id,
        );
        Ok(())
    }

    // ── Recovery ─────────────────────────────────────────────────────

    pub fn recover(wal_dir: &Path) -> Result<Self, StorageError> {
        use crate::wal::WalReader;

        let engine = Self::new(Some(wal_dir))?;

        // Try to load checkpoint first
        let checkpoint = CheckpointData::read_from_dir(wal_dir)?;
        let records = if let Some(ref ckpt) = checkpoint {
            // Restore catalog and data from checkpoint
            {
                let mut catalog = engine.catalog.write();
                *catalog = ckpt.catalog.clone();
            }
            for (table_id, rows) in &ckpt.table_data {
                // Find schema for this table
                let schema = engine.catalog.read().find_table_by_id(*table_id).cloned();
                if let Some(schema) = schema {
                    let memtable = Arc::new(MemTable::new(schema));
                    // Insert rows as already-committed data (use a sentinel txn + commit)
                    let sentinel_txn = TxnId(0);
                    let sentinel_ts = Timestamp(1); // committed at ts=1
                    for (pk, row) in rows {
                        let chain = Arc::new(crate::mvcc::VersionChain::new());
                        chain.prepend(sentinel_txn, Some(row.clone()));
                        chain.commit(sentinel_txn, sentinel_ts);
                        memtable.data.insert(pk.clone(), chain);
                    }
                    engine.tables.insert(*table_id, memtable);
                }
            }
            tracing::info!(
                "Checkpoint loaded: {} tables, segment={}, lsn={}",
                ckpt.table_data.len(), ckpt.wal_segment_id, ckpt.wal_lsn
            );
            // Read only WAL records from the checkpoint segment onward
            let reader = WalReader::new(wal_dir);
            reader.read_from_segment(ckpt.wal_segment_id)?
        } else {
            let reader = WalReader::new(wal_dir);
            reader.read_all()?
        };

        let mut recovered_write_sets: HashMap<TxnId, Vec<TxnWriteOp>> = HashMap::new();

        // When recovering from a checkpoint, skip WAL records until we see the
        // Checkpoint marker, then replay everything after it.
        let has_checkpoint = checkpoint.is_some();
        let mut past_checkpoint_marker = !has_checkpoint; // true if no checkpoint

        // Replay records
        for record in &records {
            // If we have a checkpoint, skip records until we see the Checkpoint marker
            if !past_checkpoint_marker {
                if matches!(record, WalRecord::Checkpoint { .. }) {
                    past_checkpoint_marker = true;
                }
                continue;
            }

            match record {
                WalRecord::BeginTxn { .. } => {
                    // Begin markers are metadata-only for replay.
                }
                WalRecord::CreateTable { schema_json } => {
                    let schema: TableSchema = serde_json::from_str(schema_json)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    // Skip if table already exists (from checkpoint)
                    if engine.tables.contains_key(&schema.id) {
                        continue;
                    }
                    let table = Arc::new(MemTable::new(schema.clone()));
                    engine.tables.insert(schema.id, table);
                    engine.catalog.write().add_table(schema);
                }
                WalRecord::DropTable { table_name } => {
                    let mut catalog = engine.catalog.write();
                    if let Some(schema) = catalog.find_table(table_name) {
                        engine.tables.remove(&schema.id);
                    }
                    catalog.drop_table(table_name);
                }
                WalRecord::Insert {
                    txn_id,
                    table_id,
                    row,
                } => {
                    if let Some(table) = engine.tables.get(table_id) {
                        if let Ok(pk) = table.insert(row.clone(), *txn_id) {
                            recovered_write_sets
                                .entry(*txn_id)
                                .or_default()
                                .push(TxnWriteOp {
                                    table_id: *table_id,
                                    pk,
                                });
                        }
                    }
                }
                WalRecord::Update {
                    txn_id,
                    table_id,
                    pk,
                    new_row,
                } => {
                    if let Some(table) = engine.tables.get(table_id) {
                        if table.update(pk, new_row.clone(), *txn_id).is_ok() {
                            recovered_write_sets
                                .entry(*txn_id)
                                .or_default()
                                .push(TxnWriteOp {
                                    table_id: *table_id,
                                    pk: pk.clone(),
                                });
                        }
                    }
                }
                WalRecord::Delete {
                    txn_id,
                    table_id,
                    pk,
                } => {
                    if let Some(table) = engine.tables.get(table_id) {
                        if table.delete(pk, *txn_id).is_ok() {
                            recovered_write_sets
                                .entry(*txn_id)
                                .or_default()
                                .push(TxnWriteOp {
                                    table_id: *table_id,
                                    pk: pk.clone(),
                                });
                        }
                    }
                }
                WalRecord::CommitTxn { txn_id, commit_ts }
                | WalRecord::CommitTxnLocal { txn_id, commit_ts }
                | WalRecord::CommitTxnGlobal { txn_id, commit_ts } => {
                    let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                    // During recovery, unique validation is skipped — the txn was
                    // already committed before crash. Indexes are rebuilt afterwards.
                    let _ = engine.apply_commit_to_write_set(*txn_id, *commit_ts, &write_set);
                }
                WalRecord::AbortTxn { txn_id }
                | WalRecord::AbortTxnLocal { txn_id }
                | WalRecord::AbortTxnGlobal { txn_id } => {
                    let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                    engine.apply_abort_to_write_set(*txn_id, &write_set);
                }
                WalRecord::CreateView { name, query_sql } => {
                    let mut catalog = engine.catalog.write();
                    if catalog.find_view(name).is_none() {
                        catalog.add_view(falcon_common::schema::ViewDef {
                            name: name.clone(),
                            query_sql: query_sql.clone(),
                        });
                    }
                }
                WalRecord::DropView { name } => {
                    engine.catalog.write().drop_view(name);
                }
                WalRecord::AlterTable { table_name, operation_json } => {
                    // Parse the operation JSON and replay the schema change
                    if let Ok(op) = serde_json::from_str::<serde_json::Value>(operation_json) {
                        let op_type = op.get("op").and_then(|v| v.as_str()).unwrap_or("");
                        match op_type {
                            "add_column" => {
                                if let Some(col_val) = op.get("column") {
                                    if let Ok(col) = serde_json::from_value::<falcon_common::schema::ColumnDef>(col_val.clone()) {
                                        let mut catalog = engine.catalog.write();
                                        if let Some(schema) = catalog.find_table_mut(table_name) {
                                            let new_id = falcon_common::types::ColumnId(schema.columns.len() as u32);
                                            let mut new_col = col;
                                            new_col.id = new_id;
                                            schema.columns.push(new_col);
                                        }
                                    }
                                }
                            }
                            "drop_column" => {
                                if let Some(col_name) = op.get("column_name").and_then(|v| v.as_str()) {
                                    let mut catalog = engine.catalog.write();
                                    if let Some(schema) = catalog.find_table_mut(table_name) {
                                        let lower = col_name.to_lowercase();
                                        if let Some(idx) = schema.columns.iter().position(|c| c.name.to_lowercase() == lower) {
                                            schema.columns.remove(idx);
                                            schema.primary_key_columns = schema.primary_key_columns.iter()
                                                .filter_map(|&pk| {
                                                    if pk == idx { None }
                                                    else if pk > idx { Some(pk - 1) }
                                                    else { Some(pk) }
                                                })
                                                .collect();
                                        }
                                    }
                                }
                            }
                            "rename_column" => {
                                let old_name = op.get("old_name").and_then(|v| v.as_str()).unwrap_or("");
                                let new_name = op.get("new_name").and_then(|v| v.as_str()).unwrap_or("");
                                if !old_name.is_empty() && !new_name.is_empty() {
                                    let mut catalog = engine.catalog.write();
                                    if let Some(schema) = catalog.find_table_mut(table_name) {
                                        let lower = old_name.to_lowercase();
                                        if let Some(col) = schema.columns.iter_mut().find(|c| c.name.to_lowercase() == lower) {
                                            col.name = new_name.to_string();
                                        }
                                    }
                                }
                            }
                            "rename_table" => {
                                if let Some(new_name) = op.get("new_name").and_then(|v| v.as_str()) {
                                    let mut catalog = engine.catalog.write();
                                    catalog.rename_table(table_name, new_name);
                                }
                            }
                            _ => {
                                tracing::warn!("Unknown ALTER TABLE operation during recovery: {}", op_type);
                            }
                        }
                    }
                }
                WalRecord::CreateSequence { name, start } => {
                    if !engine.sequences.contains_key(name.as_str()) {
                        engine.sequences.insert(name.clone(), AtomicI64::new(start - 1));
                    }
                }
                WalRecord::DropSequence { name } => {
                    engine.sequences.remove(name.as_str());
                }
                WalRecord::SetSequenceValue { name, value } => {
                    if let Some(entry) = engine.sequences.get(name.as_str()) {
                        entry.value().store(*value, AtomicOrdering::SeqCst);
                    }
                }
                WalRecord::TruncateTable { table_name } => {
                    let catalog = engine.catalog.read();
                    if let Some(table_schema) = catalog.find_table(table_name) {
                        let table_id = table_schema.id;
                        let schema = table_schema.clone();
                        drop(catalog);
                        engine.tables.insert(table_id, Arc::new(MemTable::new(schema)));
                    }
                }
                WalRecord::CreateIndex { index_name, table_name, column_idx, unique } => {
                    if !engine.index_registry.contains_key(index_name.as_str()) {
                        let _ = engine.create_index_impl(table_name, *column_idx, *unique);
                        if let Some(table_schema) = engine.catalog.read().find_table(table_name) {
                            engine.index_registry.insert(
                                index_name.to_lowercase(),
                                IndexMeta {
                                    table_id: table_schema.id,
                                    table_name: table_name.clone(),
                                    column_idx: *column_idx,
                                    unique: *unique,
                                },
                            );
                        }
                    }
                }
                WalRecord::DropIndex { index_name, table_name: _, column_idx } => {
                    if let Some((_, meta)) = engine.index_registry.remove(index_name.as_str()) {
                        if let Some(table_ref) = engine.tables.get(&meta.table_id) {
                            let mut indexes = table_ref.secondary_indexes.write();
                            indexes.retain(|idx| idx.column_idx != *column_idx);
                        }
                    }
                }
                WalRecord::PrepareTxn { .. } => {
                    // Prepared but not committed records remain invisible after recovery.
                }
                WalRecord::Checkpoint { .. } => {
                    // Additional checkpoint markers are no-ops during replay.
                }
                WalRecord::CoordinatorPrepare { .. } => {
                    // P0-3: Coordinator prepare record — used for crash recovery resolution.
                    // During replay, these are informational only.
                }
                WalRecord::CoordinatorCommit { .. } => {
                    // P0-3: Coordinator commit decision — participant shards should commit.
                }
                WalRecord::CoordinatorAbort { .. } => {
                    // P0-3: Coordinator abort decision — participant shards should abort.
                }
            }
        }

        // Abort all uncommitted transactions (those with writes but no Commit/Abort record).
        // Under crash semantics, these are treated as aborted.
        let uncommitted_count = recovered_write_sets.len();
        for (txn_id, write_set) in recovered_write_sets.drain() {
            engine.apply_abort_to_write_set(txn_id, &write_set);
        }
        if uncommitted_count > 0 {
            tracing::info!(
                "WAL recovery: aborted {} uncommitted transaction(s)",
                uncommitted_count
            );
        }

        // Rebuild secondary indexes from recovered data.
        for entry in engine.tables.iter() {
            entry.value().rebuild_secondary_indexes();
        }

        tracing::info!("WAL recovery complete: {} records replayed", records.len());
        Ok(engine)
    }

}

/// Map a DataType to the cast target string used by eval_cast_datum.
pub(crate) fn datatype_to_cast_target(dt: &falcon_common::types::DataType) -> String {
    use falcon_common::types::DataType;
    match dt {
        DataType::Boolean => "boolean".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float64 => "float8".into(),
        DataType::Text => "text".into(),
        DataType::Timestamp => "timestamp".into(),
        DataType::Date => "date".into(),
        DataType::Jsonb => "jsonb".into(),
        DataType::Array(_) => "text".into(),
        DataType::Decimal(_, _) => "numeric".into(),
    }
}

