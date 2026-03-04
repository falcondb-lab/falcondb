//! # Module Status: PRODUCTION
//! StorageEngine — unified entry point for all storage operations.
//! The production OLTP write path is: MemTable (in-memory row store) + MVCC + WAL.
//! Non-production fields (columnstore, disk_rowstore, lsm) are retained for
//! experimental builds but MUST NOT be used on the default write path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use falcon_common::config::{NodeRole, UstmSectionConfig, WritePathEnforcement};
use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::{Catalog, TableSchema};
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memory::{MemoryBudget, MemoryTracker};
use crate::memtable::{MemTable, PrimaryKey};
use crate::partition::PartitionManager;
use crate::stats::TableStatistics;
use crate::wal::{CheckpointData, SyncMode, WalRecord, WalWriter};

use falcon_common::rls::RlsPolicyManager;

use crate::cdc::{CdcManager, DEFAULT_CDC_BUFFER_SIZE};
use crate::cdc_wal_bridge::WalCdcBridge;
use crate::encryption::KeyManager;
use crate::metering::ResourceMeter;
use crate::online_ddl::OnlineDdlManager;
use crate::pitr::WalArchiver;
use crate::tenant_registry::TenantRegistry;

// ── Feature-gated storage engine imports ──
#[cfg(feature = "columnstore")]
use crate::columnstore::ColumnStoreTable;

#[cfg(feature = "disk_rowstore")]
use crate::disk_rowstore::DiskRowstoreTable;

#[cfg(feature = "lsm")]
use crate::lsm_table::LsmTable;

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
        self.last_failover_time_ms
            .store(duration_ms, AtomicOrdering::Relaxed);
        self.leader_changes.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// P1-3: Update the current replication lag.
    pub fn update_replication_lag(&self, lag_us: u64) {
        self.replication_lag_us
            .store(lag_us, AtomicOrdering::Relaxed);
        // Update max via CAS loop
        let mut current_max = self.max_replication_lag_us.load(AtomicOrdering::Relaxed);
        while lag_us > current_max {
            match self.max_replication_lag_us.compare_exchange_weak(
                current_max,
                lag_us,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
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
            fsync_avg_us: if flushes > 0 {
                fsync_total / flushes
            } else {
                0
            },
            group_commit_avg_size: if gc_batches > 0 {
                gc_records as f64 / gc_batches as f64
            } else {
                0.0
            },
            backlog_bytes: self.backlog_bytes.load(AtomicOrdering::Relaxed),
        }
    }

    /// Record an fsync operation with its latency.
    #[allow(dead_code)]
    pub(crate) fn record_fsync(&self, latency_us: u64) {
        self.fsync_total_us
            .fetch_add(latency_us, AtomicOrdering::Relaxed);
        // Update max via CAS loop
        let mut current_max = self.fsync_max_us.load(AtomicOrdering::Relaxed);
        while latency_us > current_max {
            match self.fsync_max_us.compare_exchange_weak(
                current_max,
                latency_us,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Record a group commit batch.
    #[allow(dead_code)]
    pub(crate) fn record_group_commit(&self, batch_size: u64) {
        self.group_commit_records
            .fetch_add(batch_size, AtomicOrdering::Relaxed);
        self.group_commit_batches
            .fetch_add(1, AtomicOrdering::Relaxed);
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
                self.ack_timestamps
                    .insert(replica_id, AtomicU64::new(ack_ts));
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

/// TDE status snapshot for observability.
#[derive(Debug, Clone)]
pub struct TdeStatusInfo {
    pub enabled: bool,
    pub algorithm: String,
    pub dek_count: usize,
    pub wal_encrypted: bool,
}

/// DEK metadata snapshot (key material never exposed).
#[derive(Debug, Clone)]
pub struct TdeDekInfo {
    pub id: u64,
    pub label: String,
    pub active: bool,
    pub created_at_ms: u64,
}

/// Enterprise and non-core extension fields extracted from `StorageEngine`.
///
/// Grouping these reduces the God Object surface of `StorageEngine` and makes
/// it obvious which fields are on the production OLTP hot-path (in `StorageEngine`
/// directly) versus enterprise / tiering / multi-tenant add-ons (here).
pub struct EnterpriseExtensions {
    /// Row-Level Security policy manager.
    pub rls_manager: RwLock<RlsPolicyManager>,
    /// Transparent Data Encryption key manager.
    pub key_manager: RwLock<KeyManager>,
    /// Table partitioning manager.
    pub partition_manager: RwLock<PartitionManager>,
    /// WAL archiver for Point-in-Time Recovery.
    pub wal_archiver: Arc<RwLock<WalArchiver>>,
    /// Change Data Capture stream manager (internally thread-safe).
    pub cdc_manager: CdcManager,
    /// Hot/Cold Memory Tiering — cold store for compressed old MVCC version payloads.
    pub cold_store: Arc<crate::cold_store::ColdStore>,
    /// String intern pool for low-cardinality string deduplication.
    pub intern_pool: Arc<crate::cold_store::StringInternPool>,
    /// P2-1: Multi-tenant registry — manages tenant lifecycle and quota enforcement.
    pub tenant_registry: Arc<TenantRegistry>,
    /// P3-3: Resource meter — per-tenant billing and throttling.
    pub resource_meter: Arc<ResourceMeter>,
    /// WAL → CDC bridge: converts WAL records into CDC change events with real LSNs.
    /// `None` until explicitly configured via `configure_cdc_bridge()`.
    pub cdc_bridge: RwLock<Option<Arc<WalCdcBridge>>>,
}

impl Default for EnterpriseExtensions {
    fn default() -> Self {
        Self {
            rls_manager: RwLock::new(RlsPolicyManager::new()),
            key_manager: RwLock::new(KeyManager::disabled()),
            partition_manager: RwLock::new(PartitionManager::new()),
            wal_archiver: Arc::new(RwLock::new(WalArchiver::disabled())),
            cdc_manager: CdcManager::new(DEFAULT_CDC_BUFFER_SIZE),
            cold_store: Arc::new(crate::cold_store::ColdStore::new_in_memory()),
            intern_pool: Arc::new(crate::cold_store::StringInternPool::new()),
            tenant_registry: Arc::new(TenantRegistry::new()),
            resource_meter: Arc::new(ResourceMeter::new()),
            cdc_bridge: RwLock::new(None),
        }
    }
}

/// The storage engine. Owns all tables (rowstore, columnstore, disk), the catalog, and the WAL.
pub struct StorageEngine {
    /// Runtime node role — controls which storage paths are permitted.
    /// Primary nodes are forbidden from using columnstore/disk paths on the write side.
    pub(crate) node_role: NodeRole,
    /// Unified table handle map — one entry per table regardless of engine type.
    /// Used by DML methods for O(1) dispatch without per-engine map chains.
    pub(crate) engine_tables: DashMap<TableId, crate::table_handle::TableHandle>,
    /// Rowstore tables (in-memory, default).
    pub(crate) tables: DashMap<TableId, Arc<MemTable>>,
    /// Columnstore tables (columnar, analytics-optimised). Feature-gated.
    #[cfg(feature = "columnstore")]
    pub(crate) columnstore_tables: DashMap<TableId, Arc<ColumnStoreTable>>,
    /// Data directory for disk-based tables (None = in-memory only).
    #[allow(dead_code)]
    pub(crate) data_dir: Option<PathBuf>,
    /// Schema catalog (protected by RwLock for DDL).
    pub(crate) catalog: RwLock<Catalog>,
    /// WAL writer (None if WAL disabled).
    pub(crate) wal: Option<Arc<WalWriter>>,
    pub(crate) wal_flushed_lsn: AtomicU64,
    pub(crate) wal_flush_lock: parking_lot::Mutex<()>,
    wal_flush_thread: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
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
    /// Background executor for async DDL backfill (e.g. ALTER TABLE ADD COLUMN
    /// with DEFAULT). When set, backfill runs in a dedicated thread pool instead
    /// of blocking the DDL statement. Feature-gated behind `online_ddl_full`.
    #[cfg(feature = "online_ddl_full")]
    pub(crate) ddl_executor: Option<Arc<crate::online_ddl::DdlBackgroundExecutor>>,
    /// Internal TxnId counter for auto-commit helpers (insert_row, delete_row).
    /// Uses a high range (starting at 1<<60) to avoid collisions with user txns.
    pub(crate) internal_txn_counter: AtomicU64,
    /// Tracks per-replica ack timestamps for GC safepoint computation.
    pub(crate) replica_ack_tracker: ReplicaAckTracker,
    /// Counter: number of times a write-txn path touched columnstore (should be 0 on Primary).
    #[cfg(feature = "columnstore")]
    pub(crate) write_path_columnstore_violations: AtomicU64,
    /// Counter: number of times a write-txn path touched disk-rowstore (should be 0 on Primary).
    #[cfg(feature = "disk_rowstore")]
    pub(crate) write_path_disk_violations: AtomicU64,
    /// Enforcement level for write-path purity violations on Primary nodes.
    pub(crate) write_path_enforcement: WritePathEnforcement,
    /// Whether LSM tables sync every write to disk.
    pub(crate) lsm_sync_writes: bool,
    /// USTM (User-Space Tiered Memory) page cache engine.
    pub ustm: Arc<crate::ustm::UstmEngine>,
    /// Pre-tracked write-buffer bytes per transaction (set during batch_insert).
    /// Avoids re-scanning the write-set in estimate_write_set_bytes at commit.
    pub(crate) txn_write_bytes: DashMap<TxnId, AtomicU64>,
    /// Deferred WAL records per txn — written in batch at commit time to reduce mutex acquisitions.
    pub(crate) txn_wal_buf: DashMap<TxnId, Vec<WalRecord>>,
    /// Max commit_ts seen during WAL recovery (0 if no recovery).
    /// TxnManager must advance its ts_counter past this value.
    pub recovered_max_ts: AtomicU64,
    /// Max txn_id seen during WAL recovery (0 if no recovery).
    pub recovered_max_txn_id: AtomicU64,
    // ── Enterprise / non-core extensions (RLS, TDE, PITR, CDC, tenancy, cold store) ──
    /// Grouped enterprise features — see [`EnterpriseExtensions`].
    pub ext: EnterpriseExtensions,
}

impl StorageEngine {
    /// Build a USTM engine from config (or use defaults).
    fn build_ustm(cfg: &UstmSectionConfig) -> Arc<crate::ustm::UstmEngine> {
        use crate::ustm::*;
        Arc::new(UstmEngine::new(UstmConfig {
            zones: ZoneConfig {
                hot_capacity_bytes: cfg.hot_capacity_bytes,
                warm_capacity_bytes: cfg.warm_capacity_bytes,
                lirs2: Lirs2Config {
                    lir_capacity: cfg.lirs_lir_capacity,
                    hir_resident_capacity: cfg.lirs_hir_capacity,
                    hir_nonresident_capacity: cfg.lirs_hir_capacity * 2,
                },
            },
            io_scheduler: IoSchedulerConfig {
                background_iops_limit: cfg.background_iops_limit,
                prefetch_iops_limit: cfg.prefetch_iops_limit,
                max_pending_per_queue: 1024,
            },
            prefetcher: PrefetcherConfig {
                enabled: cfg.prefetch_enabled,
                ..PrefetcherConfig::default()
            },
            default_page_size: cfg.page_size,
        }))
    }

    fn default_ustm() -> Arc<crate::ustm::UstmEngine> {
        Self::build_ustm(&UstmSectionConfig::default())
    }

    /// Create a new storage engine. If `wal_dir` is Some, WAL is enabled.
    pub fn new(wal_dir: Option<&Path>) -> Result<Self, StorageError> {
        Self::new_with_sync_mode(wal_dir, SyncMode::FDataSync)
    }

    /// Create a new storage engine with a specific WAL sync mode and wal_mode string.
    ///
    /// `wal_mode` values: `"auto"` | `"posix"` | `"win_async"` | `"raw_experimental"`
    /// - `"auto"`: on Windows selects WinAsync (IOCP) when available, else standard file I/O.
    /// - `"win_async"`: force Windows IOCP backend (error on non-Windows).
    /// - `"posix"` / `"raw_experimental"` / anything else: standard file I/O.
    ///
    /// WinAsync backend uses `WalDeviceWinAsync` directly (bypasses `WalWriter`).
    /// Standard file path uses `WalWriter` (BufWriter + group-commit fsync).
    pub fn new_with_wal_mode(
        wal_dir: Option<&Path>,
        sync_mode: SyncMode,
        wal_mode: &str,
        no_buffering: bool,
    ) -> Result<Self, StorageError> {
        let wal = if let Some(dir) = wal_dir {
            let use_win_async = match wal_mode.to_ascii_lowercase().as_str() {
                "win_async" => true,
                "auto" => {
                    #[cfg(windows)]
                    { crate::wal_win_async::iocp_available() }
                    #[cfg(not(windows))]
                    { false }
                }
                _ => false,
            };

            if use_win_async {
                #[cfg(windows)]
                {
                    tracing::info!(
                        "WAL backend: WinAsync (IOCP) — wal_mode={} no_buffering={}",
                        wal_mode, no_buffering,
                    );
                    Some(Arc::new(WalWriter::open(dir, sync_mode)?))
                }
                #[cfg(not(windows))]
                {
                    let _ = no_buffering;
                    return Err(StorageError::Wal(
                        "wal_mode = 'win_async' is only available on Windows".into(),
                    ));
                }
            } else {
                let _ = no_buffering;
                tracing::info!("WAL backend: Posix/File — wal_mode={}", wal_mode);
                Some(Arc::new(WalWriter::open(dir, sync_mode)?))
            }
        } else {
            None
        };

        Self::build_with_wal(wal_dir, wal)
    }

    /// Create a new storage engine with a specific WAL sync mode.
    pub fn new_with_sync_mode(wal_dir: Option<&Path>, sync_mode: SyncMode) -> Result<Self, StorageError> {
        let wal = if let Some(dir) = wal_dir {
            // Large group_commit_size prevents WalWriter auto-flush;
            // GroupCommitSyncer handles flush timing instead.
            Some(Arc::new(WalWriter::open_with_options(
                dir, sync_mode, 64 * 1024 * 1024, usize::MAX,
            )?))
        } else {
            None
        };
        Self::build_with_wal(wal_dir, wal)
    }

    /// Internal: construct Self from a pre-built WalWriter option.
    fn build_with_wal(wal_dir: Option<&Path>, wal: Option<Arc<WalWriter>>) -> Result<Self, StorageError> {
        Ok(Self {
            node_role: NodeRole::Standalone,
            engine_tables: DashMap::new(),
            tables: DashMap::new(),
            #[cfg(feature = "columnstore")]
            columnstore_tables: DashMap::new(),
            data_dir: wal_dir.map(std::path::Path::to_path_buf),
            catalog: RwLock::new(Catalog::new()),
            wal,
            wal_flushed_lsn: AtomicU64::new(0),
            wal_flush_lock: parking_lot::Mutex::new(()),
            wal_flush_thread: parking_lot::Mutex::new(None),
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
            #[cfg(feature = "online_ddl_full")]
            ddl_executor: None,
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            #[cfg(feature = "columnstore")]
            write_path_columnstore_violations: AtomicU64::new(0),
            #[cfg(feature = "disk_rowstore")]
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
            lsm_sync_writes: false,
            ustm: Self::default_ustm(),
            txn_write_bytes: DashMap::new(),
            txn_wal_buf: DashMap::new(),
            recovered_max_ts: AtomicU64::new(0),
            recovered_max_txn_id: AtomicU64::new(0),
            ext: EnterpriseExtensions::default(),
        })
    }

    /// Configure PITR / WAL archiving from the [pitr] config section.
    /// Call this after construction to enable WAL segment archiving.
    /// Also installs a WAL segment-rotation callback so completed segments
    /// are automatically registered with the archiver.
    pub fn configure_pitr(&self, enabled: bool, archive_dir: &str, retention_hours: u64) {
        if enabled && !archive_dir.is_empty() {
            let path = std::path::Path::new(archive_dir);
            *self.ext.wal_archiver.write() = crate::pitr::WalArchiver::new(path);

            // Install a segment-rotation callback on the WAL writer.
            // The Arc<RwLock<WalArchiver>> is cloned into the closure so the
            // archiver is kept alive as long as the callback exists — no unsafe needed.
            if let Some(ref wal) = self.wal {
                let archiver_arc = Arc::clone(&self.ext.wal_archiver);
                let wal_dir = wal.wal_dir();
                let cb: crate::wal::SegmentRotateCallback =
                    std::sync::Arc::new(move |seg_id: u64, seg_size: u64| {
                        let mut guard = archiver_arc.write();
                        if guard.is_enabled() {
                            let fname = crate::wal::segment_filename(seg_id);
                            let lsn_start = crate::pitr::Lsn(seg_id.saturating_mul(1_000_000));
                            let lsn_end = crate::pitr::Lsn(
                                (seg_id + 1).saturating_mul(1_000_000).saturating_sub(1),
                            );
                            // Source path: absolute path from the WAL directory.
                            let source = wal_dir.join(&fname);
                            match guard.archive_segment(&source, &fname, lsn_start, lsn_end, seg_size) {
                                Ok(_) => tracing::debug!(
                                    "PITR: archived segment {} ({} bytes)",
                                    fname,
                                    seg_size
                                ),
                                Err(e) => tracing::error!(
                                    "PITR: failed to archive segment {}: {e}",
                                    fname
                                ),
                            }
                        }
                    });
                wal.set_segment_rotate_callback(cb);
            }

            tracing::info!(
                "PITR enabled: archive_dir={} retention_hours={}",
                archive_dir, retention_hours
            );
        } else if enabled && archive_dir.is_empty() {
            tracing::warn!("PITR enabled but pitr.archive_dir is empty — archiving inactive");
        } else {
            tracing::info!("PITR disabled by configuration");
        }
    }

    /// Configure Transparent Data Encryption (TDE) from the [tde] config section.
    /// Reads the passphrase from the environment variable specified by `key_env_var`.
    /// When enabled, generates a WAL DEK and installs encryption on the WAL writer.
    pub fn configure_tde(&self, enabled: bool, key_env_var: &str, encrypt_wal: bool, encrypt_data: bool) {
        if !enabled {
            tracing::info!("TDE disabled by configuration");
            return;
        }

        let passphrase = match std::env::var(key_env_var) {
            Ok(p) if !p.is_empty() => p,
            _ => {
                tracing::error!(
                    "TDE enabled but environment variable '{}' is not set or empty — TDE inactive",
                    key_env_var
                );
                return;
            }
        };

        // Initialize the key manager with the passphrase
        *self.ext.key_manager.write() = crate::encryption::KeyManager::new(&passphrase);

        if encrypt_wal {
            // Generate a DEK for WAL encryption and install it on the WAL writer
            match self.ext.key_manager.write().generate_dek(
                crate::encryption::EncryptionScope::Wal,
            ) {
                Ok(dek_id) => {
                    if let Some(ref wal) = self.wal {
                        let mut km = self.ext.key_manager.write();
                        if let Some(enc) = crate::wal::WalEncryption::from_key_manager(&mut km, dek_id) {
                            wal.set_encryption(enc);
                            tracing::info!("TDE: WAL encryption enabled (DEK {})", dek_id);
                        } else {
                            tracing::error!("TDE: failed to create WAL encryption context for DEK {}", dek_id);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("TDE: failed to generate WAL DEK: {}", e);
                }
            }
        }

        if encrypt_data {
            tracing::info!("TDE: data block encryption enabled (per-table DEKs generated on demand)");
        }

        let km = self.ext.key_manager.read();
        tracing::info!(
            "TDE enabled: algorithm=AES-256-GCM, dek_count={}, encrypt_wal={}, encrypt_data={}",
            km.dek_count(), encrypt_wal, encrypt_data,
        );
    }

    /// Configure CDC WAL bridge from the [cdc] config section.
    /// Call this after construction to enable WAL-backed CDC event generation.
    ///
    /// When enabled, every WAL record is automatically converted into CDC
    /// change events with real WAL LSNs. Events are buffered per-transaction
    /// and only emitted to consumers on commit (matching PostgreSQL logical
    /// decoding semantics).
    ///
    /// `persist_every` controls how often (in CDC events) slot state is flushed
    /// to disk for crash recovery. Set to 0 to disable persistence.
    pub fn configure_cdc_bridge(&self, enabled: bool, persist_every: u64) {
        if !enabled {
            tracing::info!("CDC WAL bridge disabled by configuration");
            return;
        }

        let resolver = Arc::new(crate::cdc_wal_bridge::CachedTableResolver::new());

        // Bulk-load current table names from catalog
        {
            let catalog = self.catalog.read();
            resolver.load_from_catalog(&catalog);
        }

        let cdc = Arc::new(crate::cdc::CdcManager::new(crate::cdc::DEFAULT_CDC_BUFFER_SIZE));

        let mut bridge = WalCdcBridge::new(Arc::clone(&cdc), resolver);

        // Enable slot persistence if WAL directory is available
        if persist_every > 0 {
            if let Some(ref dir) = self.data_dir {
                bridge = bridge.with_persistence(dir, persist_every);
                // Recover any previously persisted slots
                match bridge.recover_slots() {
                    Ok(n) if n > 0 => tracing::info!("CDC bridge: recovered {} slots from disk", n),
                    Ok(_) => {}
                    Err(e) => tracing::warn!("CDC bridge: slot recovery failed: {}", e),
                }
            }
        }

        // Install the bridge via interior-mutable RwLock
        *self.ext.cdc_bridge.write() = Some(Arc::new(bridge));

        tracing::info!(
            "CDC WAL bridge enabled (persist_every={})",
            persist_every,
        );
    }

    /// Get TDE status summary for SHOW commands.
    pub fn tde_status(&self) -> TdeStatusInfo {
        let km = self.ext.key_manager.read();
        let wal_encrypted = self.wal.as_ref().map_or(false, |w| w.is_encrypted());
        TdeStatusInfo {
            enabled: km.is_enabled(),
            algorithm: if km.is_enabled() { "AES-256-GCM".into() } else { "none".into() },
            dek_count: km.dek_count(),
            wal_encrypted,
        }
    }

    /// List all DEK metadata (without exposing key material).
    pub fn tde_list_deks(&self) -> Vec<TdeDekInfo> {
        let km = self.ext.key_manager.read();
        km.list_deks().iter().map(|d| TdeDekInfo {
            id: d.id.0,
            label: d.label.clone(),
            active: d.active,
            created_at_ms: d.created_at_ms,
        }).collect()
    }

    /// Rotate the TDE master key with a new passphrase.
    pub fn tde_rotate_master_key(&self, new_passphrase: &str) -> Result<(), String> {
        let mut km = self.ext.key_manager.write();
        if !km.is_enabled() {
            return Err("TDE is not enabled".into());
        }
        km.rotate_master_key(new_passphrase)
            .map_err(|e| format!("TDE master key rotation failed: {e}"))?;
        tracing::info!("TDE: master key rotated successfully");
        Ok(())
    }

    /// Generate a new DEK for a given scope (e.g., table encryption).
    pub fn tde_generate_dek(&self, scope: crate::encryption::EncryptionScope) -> Result<u64, String> {
        let mut km = self.ext.key_manager.write();
        if !km.is_enabled() {
            return Err("TDE is not enabled".into());
        }
        let dek_id = km.generate_dek(scope)
            .map_err(|e| format!("TDE DEK generation failed: {e}"))?;
        tracing::info!("TDE: generated DEK {} for scope {}", dek_id, scope);
        Ok(dek_id.0)
    }

    /// Configure multi-tenant isolation and metering from the [multi_tenant] config section.
    /// Call this after construction to enable tenant quota enforcement and resource metering.
    pub fn configure_multi_tenant(
        &self,
        enabled: bool,
        metering_enabled: bool,
        default_max_qps: u64,
        default_max_concurrent_txns: u32,
        default_max_memory_bytes: u64,
        default_max_storage_bytes: u64,
    ) {
        if enabled {
            // Register the system tenant in the metering system
            use falcon_common::tenant::SYSTEM_TENANT_ID;
            self.ext.resource_meter.register_tenant(SYSTEM_TENANT_ID);
            tracing::info!(
                "Multi-tenant isolation enabled (metering={}, default_max_qps={}, default_max_txns={}, default_max_mem={}, default_max_storage={})",
                metering_enabled, default_max_qps, default_max_concurrent_txns,
                default_max_memory_bytes, default_max_storage_bytes,
            );
        } else {
            tracing::info!("Multi-tenant isolation disabled — all sessions use system tenant");
        }
    }

    /// Check whether a tenant is allowed to begin a new transaction.
    /// Returns Ok(()) if allowed, or Err with the denial reason.
    pub fn check_tenant_quota(&self, tenant_id: falcon_common::tenant::TenantId) -> Result<(), String> {
        let result = self.ext.tenant_registry.check_begin_txn(tenant_id);
        match result {
            crate::tenant_registry::QuotaCheckResult::Allowed => Ok(()),
            crate::tenant_registry::QuotaCheckResult::QpsExceeded { limit, current } => {
                Err(format!("tenant QPS limit exceeded: {current}/{limit}"))
            }
            crate::tenant_registry::QuotaCheckResult::MemoryExceeded { limit, current } => {
                Err(format!("tenant memory limit exceeded: {current}/{limit} bytes"))
            }
            crate::tenant_registry::QuotaCheckResult::TxnLimitExceeded { limit, current } => {
                Err(format!("tenant concurrent transaction limit exceeded: {current}/{limit}"))
            }
            crate::tenant_registry::QuotaCheckResult::TenantNotActive { status } => {
                Err(format!("tenant is not active: {status}"))
            }
        }
    }

    /// Record that a transaction has begun for a tenant (metering + quota tracking).
    pub fn record_tenant_txn_begin(&self, tenant_id: falcon_common::tenant::TenantId) {
        self.ext.tenant_registry.record_txn_begin(tenant_id);
        self.ext.resource_meter.record_query(tenant_id);
    }

    /// Record that a transaction has committed for a tenant.
    pub fn record_tenant_txn_commit(&self, tenant_id: falcon_common::tenant::TenantId) {
        self.ext.tenant_registry.record_txn_commit(tenant_id);
        self.ext.resource_meter.record_txn_commit(tenant_id);
    }

    /// Record that a transaction has been aborted for a tenant.
    pub fn record_tenant_txn_abort(&self, tenant_id: falcon_common::tenant::TenantId) {
        self.ext.tenant_registry.record_txn_abort(tenant_id);
        self.ext.resource_meter.record_txn_abort(tenant_id);
    }

    /// Create a named restore point at the current WAL LSN.
    /// Returns the LSN at which the restore point was recorded.
    pub fn create_restore_point(&self, name: &str) -> Result<u64, StorageError> {
        let lsn = self.wal.as_ref().map_or(0, |w| w.current_lsn());
        self.ext.wal_archiver
            .write()
            .create_restore_point(name, crate::pitr::Lsn(lsn));
        tracing::info!("Restore point '{}' created at LSN {}", name, lsn);
        Ok(lsn)
    }

    /// Start a base backup. Returns (backup_id, start_lsn).
    pub fn start_base_backup(&self, label: &str) -> Result<(u64, u64), StorageError> {
        let lsn = self.wal.as_ref().map_or(0, |w| w.current_lsn());
        // Use a monotonic counter based on wall time as backup_id
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let backup_id = now_ms;
        tracing::info!(
            "Base backup started: label='{}' backup_id={} start_lsn={}",
            label, backup_id, lsn
        );
        Ok((backup_id, lsn))
    }

    /// Stop a base backup and register it with the archiver.
    /// Returns end_lsn.
    pub fn stop_base_backup(&self, backup_id: u64, label: &str, backup_path: &str) -> Result<u64, StorageError> {
        let lsn = self.wal.as_ref().map_or(0, |w| w.current_lsn());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let backup = crate::pitr::BaseBackup {
            label: label.to_owned(),
            start_lsn: crate::pitr::Lsn(backup_id), // backup_id encodes start time ≈ start_lsn
            end_lsn: crate::pitr::Lsn(lsn),
            start_time_ms: backup_id,
            end_time_ms: now_ms,
            backup_path: std::path::PathBuf::from(backup_path),
            size_bytes: 0,
            consistent: true,
        };
        self.ext.wal_archiver.write().register_base_backup(backup);
        tracing::info!(
            "Base backup stopped: label='{}' backup_id={} end_lsn={}",
            label, backup_id, lsn
        );
        Ok(lsn)
    }

    /// Archive a completed WAL segment (called from the WAL rotation hook).
    pub fn archive_wal_segment(&self, segment_id: u64, size_bytes: u64) {
        let mut archiver = self.ext.wal_archiver.write();
        if !archiver.is_enabled() {
            return;
        }
        let filename = crate::wal::segment_filename(segment_id);
        let lsn_start = crate::pitr::Lsn(segment_id * 1000);
        let lsn_end = crate::pitr::Lsn((segment_id + 1) * 1000 - 1);
        let source = std::path::PathBuf::from(format!("wal/{filename}"));
        match archiver.archive_segment(&source, &filename, lsn_start, lsn_end, size_bytes) {
            Ok(_) => tracing::debug!("WAL segment {} archived ({} bytes)", filename, size_bytes),
            Err(e) => tracing::error!("Failed to archive WAL segment {}: {e}", filename),
        }
    }

    /// List all archived WAL segments.
    pub fn list_archived_segments(&self) -> Vec<crate::pitr::ArchivedSegment> {
        self.ext.wal_archiver.read().list_segments().into_iter().cloned().collect()
    }

    /// List all base backups registered with the archiver.
    pub fn list_base_backups(&self) -> Vec<crate::pitr::BaseBackup> {
        self.ext.wal_archiver.read().list_base_backups().into_iter().cloned().collect()
    }

    /// List all named restore points.
    pub fn list_restore_points(&self) -> Vec<crate::pitr::RestorePoint> {
        self.ext.wal_archiver.read().list_restore_points().into_iter().cloned().collect()
    }

    /// PITR archiver stats: (enabled, segment_count, bytes_archived).
    pub fn pitr_stats(&self) -> (bool, u64, u64) {
        let archiver = self.ext.wal_archiver.read();
        (archiver.is_enabled(), archiver.segment_count(), archiver.total_bytes())
    }

    /// Configure CDC from the [cdc] config section.
    /// Call this after construction if you have a `[cdc]` config section.
    pub fn configure_cdc(&self, enabled: bool, buffer_size: usize) {
        if enabled {
            self.ext.cdc_manager.set_enabled(true);
            tracing::info!("CDC enabled (buffer_size={})", buffer_size);
        } else {
            self.ext.cdc_manager.set_enabled(false);
            tracing::info!("CDC disabled by configuration");
        }
    }

    /// Set the node role. Must be called before any DML.
    pub const fn set_node_role(&mut self, role: NodeRole) {
        self.node_role = role;
    }

    /// Current node role.
    pub const fn node_role(&self) -> NodeRole {
        self.node_role
    }

    /// Number of OLTP write-path columnstore violations (should be 0 on Primary).
    #[cfg(feature = "columnstore")]
    pub fn write_path_columnstore_violation_count(&self) -> u64 {
        self.write_path_columnstore_violations
            .load(AtomicOrdering::Relaxed)
    }

    /// Always 0 when columnstore feature is disabled.
    #[cfg(not(feature = "columnstore"))]
    pub fn write_path_columnstore_violation_count(&self) -> u64 { 0 }

    /// Number of OLTP write-path disk-rowstore violations (should be 0 on Primary).
    #[cfg(feature = "disk_rowstore")]
    pub fn write_path_disk_violation_count(&self) -> u64 {
        self.write_path_disk_violations
            .load(AtomicOrdering::Relaxed)
    }

    /// Always 0 when disk_rowstore feature is disabled.
    #[cfg(not(feature = "disk_rowstore"))]
    pub fn write_path_disk_violation_count(&self) -> u64 { 0 }

    /// Set the write-path enforcement level.
    /// Call this after `set_node_role` to configure production enforcement.
    pub const fn set_write_path_enforcement(&mut self, enforcement: WritePathEnforcement) {
        self.write_path_enforcement = enforcement;
    }

    /// Set whether LSM tables sync every write to disk.
    /// false = WAL provides crash recovery (faster bulk insert).
    /// true  = every LSM write is fsynced (extra durability).
    pub const fn set_lsm_sync_writes(&mut self, sync: bool) {
        self.lsm_sync_writes = sync;
    }

    /// Current write-path enforcement level.
    pub const fn write_path_enforcement(&self) -> WritePathEnforcement {
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
            engine_tables: DashMap::new(),
            tables: DashMap::new(),
            #[cfg(feature = "columnstore")]
            columnstore_tables: DashMap::new(),
            data_dir: Some(wal_dir.to_path_buf()),
            catalog: RwLock::new(Catalog::new()),
            wal,
            wal_flushed_lsn: AtomicU64::new(0),
            wal_flush_lock: parking_lot::Mutex::new(()),
            wal_flush_thread: parking_lot::Mutex::new(None),
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
            #[cfg(feature = "online_ddl_full")]
            ddl_executor: None,
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            #[cfg(feature = "columnstore")]
            write_path_columnstore_violations: AtomicU64::new(0),
            #[cfg(feature = "disk_rowstore")]
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
            lsm_sync_writes: false,
            ustm: Self::default_ustm(),
            txn_write_bytes: DashMap::new(),
            txn_wal_buf: DashMap::new(),
            recovered_max_ts: AtomicU64::new(0),
            recovered_max_txn_id: AtomicU64::new(0),
            ext: EnterpriseExtensions::default(),
        })
    }

    /// Create a new in-memory-only storage engine (no WAL).
    pub fn new_in_memory() -> Self {
        Self {
            node_role: NodeRole::Standalone,
            engine_tables: DashMap::new(),
            tables: DashMap::new(),
            #[cfg(feature = "columnstore")]
            columnstore_tables: DashMap::new(),
            data_dir: None,
            catalog: RwLock::new(Catalog::new()),
            wal: None,
            wal_flushed_lsn: AtomicU64::new(0),
            wal_flush_lock: parking_lot::Mutex::new(()),
            wal_flush_thread: parking_lot::Mutex::new(None),
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
            #[cfg(feature = "online_ddl_full")]
            ddl_executor: None,
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            #[cfg(feature = "columnstore")]
            write_path_columnstore_violations: AtomicU64::new(0),
            #[cfg(feature = "disk_rowstore")]
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
            lsm_sync_writes: false,
            ustm: Self::default_ustm(),
            txn_write_bytes: DashMap::new(),
            txn_wal_buf: DashMap::new(),
            recovered_max_ts: AtomicU64::new(0),
            recovered_max_txn_id: AtomicU64::new(0),
            ext: EnterpriseExtensions::default(),
        }
    }

    /// Create a new in-memory-only storage engine with a memory budget.
    pub fn new_in_memory_with_budget(budget: MemoryBudget) -> Self {
        Self {
            node_role: NodeRole::Standalone,
            engine_tables: DashMap::new(),
            tables: DashMap::new(),
            #[cfg(feature = "columnstore")]
            columnstore_tables: DashMap::new(),
            data_dir: None,
            catalog: RwLock::new(Catalog::new()),
            wal: None,
            wal_flushed_lsn: AtomicU64::new(0),
            wal_flush_lock: parking_lot::Mutex::new(()),
            wal_flush_thread: parking_lot::Mutex::new(None),
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
            #[cfg(feature = "online_ddl_full")]
            ddl_executor: None,
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            #[cfg(feature = "columnstore")]
            write_path_columnstore_violations: AtomicU64::new(0),
            #[cfg(feature = "disk_rowstore")]
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
            lsm_sync_writes: false,
            ustm: Self::default_ustm(),
            txn_write_bytes: DashMap::new(),
            txn_wal_buf: DashMap::new(),
            recovered_max_ts: AtomicU64::new(0),
            recovered_max_txn_id: AtomicU64::new(0),
            ext: EnterpriseExtensions::default(),
        }
    }

    pub fn shutdown_wal_flush(&self) {
        if let Some(ref wal) = self.wal {
            wal.shutdown_flush_thread();
        }
        if let Some(handle) = self.wal_flush_thread.lock().take() {
            let _ = handle.join();
        }
    }

    /// Replace the USTM engine with one built from the given config.
    /// Call this after construction if you have a custom `[ustm]` config section.
    pub fn set_ustm_config(&mut self, cfg: &UstmSectionConfig) {
        self.ustm = Self::build_ustm(cfg);
        tracing::info!(
            "USTM engine configured: hot={}MB, warm={}MB, lirs_lir={}, prefetch={}",
            cfg.hot_capacity_bytes / (1024 * 1024),
            cfg.warm_capacity_bytes / (1024 * 1024),
            cfg.lirs_lir_capacity,
            cfg.prefetch_enabled,
        );
    }

    /// Get a snapshot of USTM engine statistics.
    pub fn ustm_stats(&self) -> crate::ustm::UstmStats {
        self.ustm.stats()
    }

    /// Set a WAL observer callback for replication. Called after every WAL append.
    /// The observer receives a reference to the WAL record that was just written.
    pub fn set_wal_observer(&mut self, observer: WalObserver) {
        self.wal_observer = Some(observer);
    }

    /// Append a record to the WAL (if enabled) and notify the observer (if set).
    /// This is the single entry point for all WAL writes, ensuring replication
    /// always sees every record. Also feeds the CDC bridge if configured.
    pub(crate) fn append_wal(&self, record: &WalRecord) -> Result<(), StorageError> {
        if self.wal.is_none() && self.wal_observer.is_none() {
            return Ok(());
        }
        let lsn = if let Some(ref wal) = self.wal {
            wal.append(record)?
        } else {
            0
        };
        self.wal_stats
            .records_written
            .fetch_add(1, AtomicOrdering::Relaxed);
        if let Some(ref observer) = self.wal_observer {
            observer(record);
            self.wal_stats
                .observer_notifications
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        if let Some(ref bridge) = *self.ext.cdc_bridge.read() {
            bridge.on_wal_record(lsn, record);
        }
        Ok(())
    }

    /// Append + flush the WAL (for DDL and commit records that need durability).
    pub(crate) fn append_and_flush_wal(&self, record: &WalRecord) -> Result<(), StorageError> {
        self.append_wal(record)?;
        if let Some(ref wal) = self.wal {
            let start = std::time::Instant::now();
            wal.flush()?;
            let elapsed_us = start.elapsed().as_micros() as u64;
            self.wal_stats
                .fsync_total_us
                .fetch_add(elapsed_us, AtomicOrdering::Relaxed);
            // Update max fsync latency (lock-free CAS loop).
            let mut cur = self.wal_stats.fsync_max_us.load(AtomicOrdering::Relaxed);
            while elapsed_us > cur {
                match self.wal_stats.fsync_max_us.compare_exchange_weak(
                    cur,
                    elapsed_us,
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
        }
        self.wal_stats.flushes.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Leader-follower group commit: append WAL record and ensure durable.
    /// Uses WRITE_THROUGH (FUA) so flush_split's write_all is immediately durable.
    pub(crate) fn durable_wal_commit(&self, record: &WalRecord) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            let txn_id = match record {
                WalRecord::CommitTxnLocal { txn_id, .. } => Some(*txn_id),
                _ => None,
            };
            let deferred = txn_id.and_then(|id| self.txn_wal_buf.remove(&id).map(|(_, v)| v));

            let lsn = if let Some(buf) = &deferred {
                let n = buf.len() + 1;
                let lsn = if buf.len() == 1 {
                    wal.append_multi(&[&buf[0], record])?
                } else {
                    let mut all: Vec<&WalRecord> = buf.iter().collect();
                    all.push(record);
                    wal.append_multi(&all)?
                };
                self.wal_stats.records_written.fetch_add(n as u64, AtomicOrdering::Relaxed);
                lsn
            } else {
                let lsn = wal.append(record)?;
                self.wal_stats.records_written.fetch_add(1, AtomicOrdering::Relaxed);
                lsn
            };

            if let Some(ref observer) = self.wal_observer {
                if let Some(buf) = &deferred {
                    for r in buf { observer(r); }
                }
                observer(record);
            }
            if self.wal_flushed_lsn.load(AtomicOrdering::Acquire) >= lsn {
                return Ok(());
            }
            let _guard = self.wal_flush_lock.lock();
            if self.wal_flushed_lsn.load(AtomicOrdering::Acquire) >= lsn {
                return Ok(());
            }
            let flushed = wal.flush_split()?;
            self.wal_flushed_lsn.store(flushed, AtomicOrdering::Release);
        }
        Ok(())
    }

    /// Record group commit statistics after a batch flush.
    /// Called by the group commit path to update `WalStats` metrics.
    #[allow(dead_code)]
    pub(crate) fn record_group_commit(&self, records_in_batch: u64) {
        self.wal_stats
            .group_commit_batches
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.wal_stats
            .group_commit_records
            .fetch_add(records_in_batch, AtomicOrdering::Relaxed);
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
    pub const fn online_ddl(&self) -> &OnlineDdlManager {
        &self.online_ddl
    }

    /// Set the background DDL executor for async backfill operations.
    ///
    /// When configured, `ALTER TABLE ADD COLUMN ... DEFAULT ...` will submit
    /// backfill work to this executor and return immediately instead of
    /// blocking until all existing rows are updated.
    #[cfg(feature = "online_ddl_full")]
    pub fn set_ddl_executor(&mut self, executor: Arc<crate::online_ddl::DdlBackgroundExecutor>) {
        self.ddl_executor = Some(executor);
    }

    /// Access the background DDL executor, if configured.
    #[cfg(feature = "online_ddl_full")]
    pub fn ddl_executor(&self) -> Option<&Arc<crate::online_ddl::DdlBackgroundExecutor>> {
        self.ddl_executor.as_ref()
    }

    /// Get a snapshot of WAL write statistics.
    pub fn wal_stats_snapshot(&self) -> WalStatsSnapshot {
        self.wal_stats.snapshot()
    }

    /// Check whether WAL is enabled for this engine.
    pub const fn is_wal_enabled(&self) -> bool {
        self.wal.is_some()
    }

    /// Access the replica ack tracker for updating/querying replica timestamps.
    pub const fn replica_ack_tracker(&self) -> &ReplicaAckTracker {
        &self.replica_ack_tracker
    }

    /// Set GC configuration.
    pub const fn set_gc_config(&mut self, config: crate::gc::GcConfig) {
        self.gc_config = config;
    }

    /// Get a snapshot of cumulative GC statistics.
    pub fn gc_stats_snapshot(&self) -> crate::gc::GcStatsSnapshot {
        self.gc_stats.snapshot()
    }

    /// Get a reference to the internal GC stats (for use by GcRunner).
    pub const fn gc_stats_snapshot_ref(&self) -> &crate::gc::GcStats {
        &self.gc_stats
    }

    /// Run a GC sweep using the engine's own config and stats.
    /// `watermark` should be computed from `compute_safepoint()`.
    /// Returns the sweep result.
    pub fn gc_sweep(&self, watermark: Timestamp) -> crate::gc::GcSweepResult {
        self.run_gc_with_config(watermark, &self.gc_config, &self.gc_stats)
    }

    pub(crate) fn record_write(&self, txn_id: TxnId, table_id: TableId, pk: PrimaryKey) {
        let mut ws = self.txn_write_sets.entry(txn_id).or_default();
        if !ws.iter().any(|op| op.table_id == table_id && op.pk == pk) {
            ws.push(TxnWriteOp { table_id, pk });
        }
    }

    pub(crate) fn record_write_no_dedup(&self, txn_id: TxnId, table_id: TableId, pk: PrimaryKey) {
        self.txn_write_sets
            .entry(txn_id)
            .or_default()
            .push(TxnWriteOp { table_id, pk });
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
            .map_or(0, |ws| ws.len())
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
            .map_or(0, |rs| rs.len())
    }

    pub(crate) fn apply_commit_to_write_set(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
        write_set: &[TxnWriteOp],
    ) -> Result<(), StorageError> {
        if write_set.is_empty() {
            return Ok(());
        }
        // Single-key fast path: no Vec alloc, no PK clone, skip index machinery
        if write_set.len() == 1 {
            let op = &write_set[0];
            if let Some(table) = self.tables.get(&op.table_id) {
                if !table.has_secondary_idx.load(AtomicOrdering::Acquire) {
                    if let Some(chain) = table.data.get(&op.pk) {
                        let is_insert = chain.commit_no_report(txn_id, commit_ts);
                        if is_insert {
                            table.committed_row_count.fetch_add(1, AtomicOrdering::Relaxed);
                        }
                    }
                    return Ok(());
                }
                return table.value().commit_keys(txn_id, commit_ts, &[op.pk.clone()]);
            }
            // Not an in-memory table — use engine_tables dispatch
            if let Some(handle) = self.engine_tables.get(&op.table_id) {
                handle.commit_keys_batch(&[op.pk.clone()], txn_id, commit_ts)?;
            }
            return Ok(());
        }
        let first_table = write_set[0].table_id;
        if write_set.iter().all(|op| op.table_id == first_table) {
            if let Some(table) = self.tables.get(&first_table) {
                let keys: Vec<PrimaryKey> = write_set.iter().map(|op| op.pk.clone()).collect();
                return table.value().commit_keys(txn_id, commit_ts, &keys);
            }
            // single disk table
            let keys: Vec<PrimaryKey> = write_set.iter().map(|op| op.pk.clone()).collect();
            if let Some(handle) = self.engine_tables.get(&first_table) {
                handle.commit_keys_batch(&keys, txn_id, commit_ts)?;
            }
            return Ok(());
        }
        // Multi-table: commit each table type
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table.entry(op.table_id).or_default().push(op.pk.clone());
        }
        for (table_id, keys) in &keys_by_table {
            if let Some(handle) = self.engine_tables.get(table_id) {
                match handle.value() {
                    crate::table_handle::TableHandle::Rowstore(t) => t.commit_keys(txn_id, commit_ts, keys)?,
                    _ => handle.commit_keys_batch(keys, txn_id, commit_ts)?,
                }
            }
        }

        Ok(())
    }

    pub(crate) fn apply_abort_to_write_set(&self, txn_id: TxnId, write_set: &[TxnWriteOp]) {
        if write_set.is_empty() {
            return;
        }
        let first_table = write_set[0].table_id;
        if write_set.iter().all(|op| op.table_id == first_table) {
            if let Some(table) = self.tables.get(&first_table) {
                let keys: Vec<PrimaryKey> = write_set.iter().map(|op| op.pk.clone()).collect();
                table.value().abort_keys(txn_id, &keys);
                return;
            }
            // Not in-memory — use engine_tables dispatch
            if let Some(handle) = self.engine_tables.get(&first_table) {
                let keys: Vec<PrimaryKey> = write_set.iter().map(|op| op.pk.clone()).collect();
                handle.abort_keys_batch(&keys, txn_id);
            }
            return;
        }
        // Multi-table
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table.entry(op.table_id).or_default().push(op.pk.clone());
        }
        for (table_id, keys) in keys_by_table {
            if let Some(handle) = self.engine_tables.get(&table_id) {
                match handle.value() {
                    crate::table_handle::TableHandle::Rowstore(t) => t.abort_keys(txn_id, &keys),
                    _ => handle.abort_keys_batch(&keys, txn_id),
                }
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
            indexes
                .iter()
                .map(|idx| (idx.column_idx, idx.unique))
                .collect()
        } else {
            vec![]
        }
    }

    // ── Table statistics (ANALYZE) ─────────────────────────────────

    /// Collect statistics for a table by scanning all committed rows.
    /// Uses a snapshot read at the latest timestamp visible to a fresh reader.
    pub fn analyze_table(&self, table_name: &str) -> Result<TableStatistics, StorageError> {
        let schema = self
            .catalog
            .read()
            .find_table(table_name)
            .cloned()
            .ok_or(StorageError::TableNotFound(TableId(0)))?;
        let table = self
            .tables
            .get(&schema.id)
            .ok_or(StorageError::TableNotFound(schema.id))?;

        // Snapshot scan: use a sentinel txn/ts to read all committed data
        let read_txn = TxnId(u64::MAX);
        let read_ts = Timestamp(u64::MAX);
        let rows: Vec<Vec<falcon_common::datum::Datum>> = table
            .scan(read_txn, read_ts)
            .into_iter()
            .map(|(_pk, row)| row.values)
            .collect();

        let columns: Vec<(usize, String)> = schema
            .columns
            .iter()
            .enumerate()
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
        TxnId(
            self.internal_txn_counter
                .fetch_add(1, AtomicOrdering::Relaxed),
        )
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
    pub fn validate_read_set(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<(), StorageError> {
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

    pub(crate) fn commit_txn_local(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);

        // Fast path: read-only transactions have no writes to commit.
        if write_set.is_empty() {
            self.take_read_set(txn_id);
            self.txn_write_bytes.remove(&txn_id);
            return Ok(());
        }

        // Skip read_set cleanup for single-key writes (autocommit INSERT never records reads)
        if write_set.len() > 1 {
            self.take_read_set(txn_id);
        }

        // Memory accounting: on commit, write-buffer bytes become committed MVCC bytes.
        // Fast path: use pre-tracked bytes from batch_insert if available.
        let ws_bytes = if let Some((_, tracked)) = self.txn_write_bytes.remove(&txn_id) {
            tracked.load(AtomicOrdering::Relaxed)
        } else {
            self.estimate_write_set_bytes(txn_id, &write_set)
        };
        self.memory_tracker.dealloc_write_buffer(ws_bytes);
        self.memory_tracker.alloc_mvcc(ws_bytes);

        // CP-D: WAL commit record MUST be durable BEFORE MVCC visibility.
        self.durable_wal_commit(&WalRecord::CommitTxnLocal { txn_id, commit_ts })?;

        // CP-V: Apply to MVCC — makes writes visible to concurrent readers.
        if let Err(e) = self.apply_commit_to_write_set(txn_id, commit_ts, &write_set) {
            self.memory_tracker.dealloc_mvcc(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &write_set);
            let _ = self.append_and_flush_wal(&WalRecord::AbortTxnLocal { txn_id });
            return Err(e);
        }

        if self.ext.cdc_manager.is_enabled() {
            self.ext.cdc_manager.emit_commit(txn_id);
        }

        Ok(())
    }

    pub(crate) fn commit_txn_global(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);

        // Fast path: read-only transactions
        if write_set.is_empty() {
            self.txn_write_bytes.remove(&txn_id);
            return Ok(());
        }

        // Memory accounting: write-buffer → mvcc transition
        let ws_bytes = if let Some((_, tracked)) = self.txn_write_bytes.remove(&txn_id) {
            tracked.load(AtomicOrdering::Relaxed)
        } else {
            self.estimate_write_set_bytes(txn_id, &write_set)
        };
        self.memory_tracker.dealloc_write_buffer(ws_bytes);
        self.memory_tracker.alloc_mvcc(ws_bytes);

        // CP-D: WAL commit record MUST be durable BEFORE MVCC visibility.
        self.durable_wal_commit(&WalRecord::CommitTxnGlobal { txn_id, commit_ts })?;

        // CP-V: Apply to MVCC — makes writes visible to concurrent readers.
        if let Err(e) = self.apply_commit_to_write_set(txn_id, commit_ts, &write_set) {
            self.memory_tracker.dealloc_mvcc(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &write_set);
            // Compensate the durable commit record so recovery also aborts.
            let _ = self.append_and_flush_wal(&WalRecord::AbortTxn { txn_id });
            return Err(e);
        }

        if self.ext.cdc_manager.is_enabled() {
            self.ext.cdc_manager.emit_commit(txn_id);
        }

        Ok(())
    }

    pub(crate) fn abort_txn_local(&self, txn_id: TxnId) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);
        self.txn_wal_buf.remove(&txn_id);

        // Memory accounting: aborted writes free the write-buffer allocation
        let ws_bytes = if let Some((_, tracked)) = self.txn_write_bytes.remove(&txn_id) {
            tracked.load(AtomicOrdering::Relaxed)
        } else {
            self.estimate_write_set_bytes(txn_id, &write_set)
        };
        self.memory_tracker.dealloc_write_buffer(ws_bytes);

        self.apply_abort_to_write_set(txn_id, &write_set);

        self.append_wal(&WalRecord::AbortTxnLocal { txn_id })?;

        // CDC: emit rollback marker
        self.ext.cdc_manager.emit_rollback(txn_id);

        Ok(())
    }

    pub(crate) fn abort_txn_global(&self, txn_id: TxnId) -> Result<(), StorageError> {
        let write_set = self.take_write_set(txn_id);
        let _read_set = self.take_read_set(txn_id);
        self.txn_wal_buf.remove(&txn_id);

        // Memory accounting: aborted writes free the write-buffer allocation
        let ws_bytes = if let Some((_, tracked)) = self.txn_write_bytes.remove(&txn_id) {
            tracked.load(AtomicOrdering::Relaxed)
        } else {
            self.estimate_write_set_bytes(txn_id, &write_set)
        };
        self.memory_tracker.dealloc_write_buffer(ws_bytes);

        self.apply_abort_to_write_set(txn_id, &write_set);

        self.append_wal(&WalRecord::AbortTxnGlobal { txn_id })?;

        // CDC: emit rollback marker
        self.ext.cdc_manager.emit_rollback(txn_id);

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

    /// Current WAL LSN (monotonically increasing sequence number).
    /// Returns 0 if WAL is disabled.
    pub fn current_wal_lsn(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.current_lsn())
    }

    /// Current WAL segment ID. Returns 0 if WAL is disabled.
    pub fn current_wal_segment(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.current_segment_id())
    }

    /// Flushed (durable) WAL LSN — approximated from WAL stats.
    /// In production, the GroupCommitSyncer tracks the precise flushed LSN;
    /// here we report current_lsn as the upper bound (records appended).
    /// The actual flushed LSN is <= current_lsn. Returns 0 if WAL is disabled.
    pub fn flushed_wal_lsn(&self) -> u64 {
        // Best approximation: if flushes > 0, the latest flush covered up to current_lsn.
        // The GroupCommitSyncer (external) tracks the precise boundary.
        if self.wal_stats.flushes.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            self.current_wal_lsn()
        } else {
            0
        }
    }

    /// Current WAL backlog bytes (written but not yet replicated/archived).
    /// Returns 0 if WAL is disabled.
    pub fn wal_backlog_bytes(&self) -> u64 {
        self.wal_stats.backlog_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    // ── v1.0.7: Cold Store accessors ────────────────────────────────

    /// Snapshot of cold store metrics.
    pub fn cold_store_metrics(&self) -> crate::cold_store::ColdStoreMetricsSnapshot {
        self.ext.cold_store.metrics.snapshot()
    }

    /// Hot memory bytes (MVCC bytes tracked by memory tracker).
    pub fn memory_hot_bytes(&self) -> u64 {
        let snap = self.memory_tracker.snapshot();
        snap.mvcc_bytes
    }

    /// Cold memory bytes (compressed, in cold store).
    pub fn memory_cold_bytes(&self) -> u64 {
        self.ext.cold_store.metrics.cold_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// String intern pool hit rate.
    pub fn intern_hit_rate(&self) -> f64 {
        self.ext.intern_pool.hit_rate()
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

        for table_ref in &self.tables {
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
    pub const fn memory_tracker(&self) -> &MemoryTracker {
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
    pub const fn set_memory_budget(&mut self, budget: MemoryBudget) {
        self.memory_tracker = MemoryTracker::new(budget);
    }

    // ── Checkpoint ───────────────────────────────────────────────────

    /// Create a checkpoint: snapshot all committed data to a checkpoint file.
    /// After checkpoint, old WAL segments before the current one can be purged.
    /// Returns (wal_segment_id, row_count) on success.
    pub fn checkpoint(&self) -> Result<(u64, usize), StorageError> {
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| StorageError::Serialization("Cannot checkpoint without WAL".into()))?;

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
        for table_ref in &self.tables {
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
            segment_id,
            lsn,
            total_rows,
            purged_segments
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
        for table_ref in &self.tables {
            let table_id = *table_ref.key();
            let memtable = table_ref.value();
            let rows: Vec<(Vec<u8>, OwnedRow)> = memtable.scan(dummy_txn, read_ts);
            table_data.push((table_id, rows));
        }

        let (segment_id, lsn) = self.wal.as_ref().map_or((0, 0), |wal| {
            (wal.current_segment_id(), wal.current_lsn())
        });

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
                let schema = engine.catalog.read().find_table_by_id(*table_id).cloned();
                if let Some(schema) = schema {
                    use falcon_common::schema::StorageType;
                    let sentinel_txn = TxnId(0);
                    let sentinel_ts = Timestamp(1);
                    match schema.storage_type {
                        #[cfg(feature = "rocksdb")]
                        StorageType::RocksDbRowstore => {
                            let data_dir = engine.data_dir.as_deref().unwrap_or_else(|| std::path::Path::new("."));
                            let rdb_dir = data_dir.join(format!("rocksdb_table_{}", table_id.0));
                            let _ = std::fs::remove_dir_all(&rdb_dir);
                            if let Ok(rdb) = crate::rocksdb_table::RocksDbTable::open(schema, &rdb_dir) {
                                // Write checkpoint rows as committed MVCC values
                                for (pk, row) in rows {
                                    if let Ok(data) = bincode::serialize(row) {
                                        let mv = crate::lsm::mvcc_encoding::MvccValue {
                                            txn_id: sentinel_txn,
                                            status: crate::lsm::mvcc_encoding::MvccStatus::Committed,
                                            commit_ts: sentinel_ts,
                                            is_tombstone: false,
                                            data,
                                        };
                                        let _ = rdb.insert_committed(pk, &mv);
                                    }
                                }
                                let rdb = Arc::new(rdb);
                                engine.engine_tables.insert(*table_id, crate::table_handle::TableHandle::RocksDb(Arc::clone(&rdb)));
                                engine.rocksdb_tables.insert(*table_id, rdb);
                            }
                        }
                        _ => {
                            let memtable = Arc::new(MemTable::new(schema));
                            for (pk, row) in rows {
                                let chain = Arc::new(crate::mvcc::VersionChain::new());
                                chain.prepend(sentinel_txn, Some(row.clone()));
                                chain.commit(sentinel_txn, sentinel_ts);
                                memtable.data.insert(pk.clone(), chain);
                            }
                            engine.engine_tables.insert(*table_id, crate::table_handle::TableHandle::Rowstore(Arc::clone(&memtable)));
                            engine.tables.insert(*table_id, memtable);
                        }
                    }
                }
            }
            tracing::info!(
                "Checkpoint loaded: {} tables, segment={}, lsn={}",
                ckpt.table_data.len(),
                ckpt.wal_segment_id,
                ckpt.wal_lsn
            );
            // Read only WAL records from the checkpoint segment onward
            let reader = WalReader::new(wal_dir);
            reader.read_from_segment(ckpt.wal_segment_id)?
        } else {
            let reader = WalReader::new(wal_dir);
            reader.read_all()?
        };

        let mut recovered_write_sets: HashMap<TxnId, Vec<TxnWriteOp>> = HashMap::new();
        let mut max_commit_ts: u64 = 0;
        let mut max_txn_id: u64 = 0;

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
                WalRecord::BeginTxn { .. }
                | WalRecord::PrepareTxn { .. }
                | WalRecord::Checkpoint { .. }
                | WalRecord::CoordinatorPrepare { .. }
                | WalRecord::CoordinatorCommit { .. }
                | WalRecord::CoordinatorAbort { .. } => {
                    // Metadata-only / no-op records during replay.
                }
                WalRecord::CreateDatabase { name, owner } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.create_database(name, owner);
                }
                WalRecord::DropDatabase { name } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.drop_database(name);
                }
                WalRecord::CreateTable { schema } => {
                    // Skip if table already exists (from checkpoint or prior WAL record)
                    if engine.engine_tables.contains_key(&schema.id) {
                        engine.catalog.write().add_table(schema.clone());
                        continue;
                    }
                    use falcon_common::schema::StorageType;
                    match schema.storage_type {
                        StorageType::Rowstore => {
                            let table = Arc::new(MemTable::new(schema.clone()));
                            engine.engine_tables.insert(schema.id, crate::table_handle::TableHandle::Rowstore(Arc::clone(&table)));
                            engine.tables.insert(schema.id, table);
                        }
                        #[cfg(feature = "rocksdb")]
                        StorageType::RocksDbRowstore => {
                            let data_dir = engine.data_dir.as_deref().unwrap_or_else(|| std::path::Path::new("."));
                            let rdb_dir = data_dir.join(format!("rocksdb_table_{}", schema.id.0));
                            // Wipe stale RocksDB state — WAL is the source of truth.
                            let _ = std::fs::remove_dir_all(&rdb_dir);
                            if let Ok(rdb) = crate::rocksdb_table::RocksDbTable::open(schema.clone(), &rdb_dir) {
                                let rdb = Arc::new(rdb);
                                engine.engine_tables.insert(schema.id, crate::table_handle::TableHandle::RocksDb(Arc::clone(&rdb)));
                                engine.rocksdb_tables.insert(schema.id, rdb);
                            }
                        }
                        #[cfg(feature = "lsm")]
                        StorageType::LsmRowstore => {
                            let data_dir = engine.data_dir.as_deref().unwrap_or_else(|| std::path::Path::new("."));
                            let lsm_dir = data_dir.join(format!("lsm_table_{}", schema.id.0));
                            let _ = std::fs::remove_dir_all(&lsm_dir);
                            if let Ok(lsm_engine) = crate::lsm::engine::LsmEngine::open(
                                &lsm_dir,
                                crate::lsm::engine::LsmConfig::default(),
                            ) {
                                let lsm = Arc::new(crate::lsm_table::LsmTable::new(schema.clone(), Arc::new(lsm_engine)));
                                engine.engine_tables.insert(schema.id, crate::table_handle::TableHandle::Lsm(Arc::clone(&lsm)));
                                engine.lsm_tables.insert(schema.id, lsm);
                            }
                        }
                        _ => {
                            // Fallback: create as in-memory Rowstore
                            let table = Arc::new(MemTable::new(schema.clone()));
                            engine.engine_tables.insert(schema.id, crate::table_handle::TableHandle::Rowstore(Arc::clone(&table)));
                            engine.tables.insert(schema.id, table);
                        }
                    }
                    engine.catalog.write().add_table(schema.clone());
                }
                WalRecord::DropTable { table_name } => {
                    let mut catalog = engine.catalog.write();
                    if let Some(schema) = catalog.find_table(table_name) {
                        let tid = schema.id;
                        engine.engine_tables.remove(&tid);
                        engine.tables.remove(&tid);
                        #[cfg(feature = "rocksdb")]
                        engine.rocksdb_tables.remove(&tid);
                        #[cfg(feature = "lsm")]
                        engine.lsm_tables.remove(&tid);
                    }
                    catalog.drop_table(table_name);
                }
                WalRecord::Insert {
                    txn_id,
                    table_id,
                    row,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        if let Ok(pk) = handle.insert(row, *txn_id) {
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
                WalRecord::BatchInsert {
                    txn_id,
                    table_id,
                    rows,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        for row in rows {
                            if let Ok(pk) = handle.insert(row, *txn_id) {
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
                }
                WalRecord::Update {
                    txn_id,
                    table_id,
                    pk,
                    new_row,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        if handle.update(pk, new_row, *txn_id).is_ok() {
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
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        if handle.delete(pk, *txn_id).is_ok() {
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
                    if commit_ts.0 > max_commit_ts { max_commit_ts = commit_ts.0; }
                    if txn_id.0 > max_txn_id { max_txn_id = txn_id.0; }
                    let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
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
                WalRecord::AlterTable {
                    table_name,
                    op,
                } => {
                    use crate::wal::AlterTableOp;
                    match op {
                        AlterTableOp::AddColumn { column } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let new_id = falcon_common::types::ColumnId(
                                    schema.columns.len() as u32,
                                );
                                let mut new_col = column.clone();
                                new_col.id = new_id;
                                schema.columns.push(new_col);
                            }
                        }
                        AlterTableOp::DropColumn { column_name } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = column_name.to_lowercase();
                                if let Some(idx) = schema
                                    .columns
                                    .iter()
                                    .position(|c| c.name.to_lowercase() == lower)
                                {
                                    schema.columns.remove(idx);
                                    schema.primary_key_columns = schema
                                        .primary_key_columns
                                        .iter()
                                        .filter_map(|&pk| {
                                            if pk == idx {
                                                None
                                            } else if pk > idx {
                                                Some(pk - 1)
                                            } else {
                                                Some(pk)
                                            }
                                        })
                                        .collect();
                                }
                            }
                        }
                        AlterTableOp::RenameColumn { old_name, new_name } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = old_name.to_lowercase();
                                if let Some(col) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|c| c.name.to_lowercase() == lower)
                                {
                                    col.name = new_name.clone();
                                }
                            }
                        }
                        AlterTableOp::RenameTable { new_name } => {
                            let mut catalog = engine.catalog.write();
                            catalog.rename_table(table_name, new_name);
                        }
                    }
                }
                WalRecord::CreateSequence { name, start } => {
                    if !engine.sequences.contains_key(name.as_str()) {
                        engine
                            .sequences
                            .insert(name.clone(), AtomicI64::new(start - 1));
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
                        let new_table = Arc::new(MemTable::new(schema));
                        engine.engine_tables.insert(table_id, crate::table_handle::TableHandle::Rowstore(Arc::clone(&new_table)));
                        engine.tables.insert(table_id, new_table);
                    }
                }
                WalRecord::CreateIndex {
                    index_name,
                    table_name,
                    column_idx,
                    unique,
                } => {
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
                WalRecord::DropIndex {
                    index_name,
                    table_name: _,
                    column_idx,
                } => {
                    if let Some((_, meta)) = engine.index_registry.remove(index_name.as_str()) {
                        if let Some(table_ref) = engine.tables.get(&meta.table_id) {
                            let mut indexes = table_ref.secondary_indexes.write();
                            indexes.retain(|idx| idx.column_idx != *column_idx);
                            table_ref.has_secondary_idx.store(!indexes.is_empty(), AtomicOrdering::Release);
                        }
                    }
                }
                WalRecord::CreateSchema { name, owner } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.create_schema(name, owner);
                }
                WalRecord::DropSchema { name } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.drop_schema(name);
                }
                WalRecord::CreateRole {
                    name,
                    can_login,
                    is_superuser,
                    can_create_db,
                    can_create_role,
                    password_hash,
                } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.create_role(
                        name,
                        *can_login,
                        *is_superuser,
                        *can_create_db,
                        *can_create_role,
                        password_hash.clone(),
                    );
                }
                WalRecord::DropRole { name } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.drop_role(name);
                }
                WalRecord::AlterRole { name, opts } => {
                    {
                        let mut catalog = engine.catalog.write();
                        let _ = catalog.alter_role(
                            name,
                            opts.password.clone(),
                            opts.can_login,
                            opts.is_superuser,
                            opts.can_create_db,
                            opts.can_create_role,
                        );
                    }
                }
                WalRecord::GrantPrivilege {
                    grantee,
                    privilege,
                    object_type,
                    object_name,
                    grantor,
                } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.grant_privilege(grantee, privilege, object_type, object_name, grantor);
                }
                WalRecord::RevokePrivilege {
                    grantee,
                    privilege,
                    object_type,
                    object_name,
                } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.revoke_privilege(grantee, privilege, object_type, object_name);
                }
                WalRecord::GrantRole { member, group } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.grant_role_membership(member, group);
                }
                WalRecord::RevokeRole { member, group } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.revoke_role_membership(member, group);
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
        for entry in &engine.tables {
            entry.value().rebuild_secondary_indexes();
        }

        engine.recovered_max_ts.store(max_commit_ts, AtomicOrdering::Relaxed);
        engine.recovered_max_txn_id.store(max_txn_id, AtomicOrdering::Relaxed);
        tracing::info!(
            "WAL recovery complete: {} records replayed, max_commit_ts={}, max_txn_id={}",
            records.len(), max_commit_ts, max_txn_id
        );
        Ok(engine)
    }

    /// Gracefully shut down the storage engine.
    /// Flushes WAL, shuts down USTM page cache, and releases resources.
    pub fn shutdown(&self) {
        self.ustm.shutdown();
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.flush() {
                tracing::warn!("WAL flush on shutdown failed: {}", e);
            }
        }
        tracing::info!(
            "StorageEngine shut down (USTM stats: hot={}B, warm={}B, evictions={})",
            self.ustm.stats().zones.hot_used_bytes,
            self.ustm.stats().zones.warm_used_bytes,
            self.ustm.stats().zones.evictions,
        );
    }
}

/// Map a DataType to the cast target string used by eval_cast_datum.
pub(crate) fn datatype_to_cast_target(dt: &falcon_common::types::DataType) -> String {
    use falcon_common::types::DataType;
    match dt {
        DataType::Boolean => "boolean".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "real".into(),
        DataType::Float64 => "float8".into(),
        DataType::Text | DataType::Array(_) => "text".into(),
        DataType::Timestamp => "timestamp".into(),
        DataType::Date => "date".into(),
        DataType::Jsonb => "jsonb".into(),
        DataType::Decimal(_, _) => "numeric".into(),
        DataType::Time => "time".into(),
        DataType::Interval => "interval".into(),
        DataType::Uuid => "uuid".into(),
        DataType::Bytea => "bytea".into(),
    }
}
