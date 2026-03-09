//! # Module Status: PRODUCTION
//! StorageEngine — unified entry point for all storage operations.
//! The production OLTP write path is: MemTable (in-memory row store) + MVCC + WAL.
//! Non-production fields (columnstore, disk_rowstore, lsm) are retained for
//! experimental builds but MUST NOT be used on the default write path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use falcon_common::config::{NodeRole, UstmSectionConfig, WritePathEnforcement};
use falcon_common::datum::OwnedRow;
use falcon_common::error::StorageError;
use falcon_common::schema::{Catalog, TableSchema};
use falcon_common::types::{TableId, Timestamp, TxnId, TxnType};

use crate::memory::{MemoryBudget, MemoryTracker};

thread_local! {
    static SKIP_READ_TRACKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn set_skip_read_tracking(skip: bool) {
    SKIP_READ_TRACKING.with(|c| c.set(skip));
}
use crate::memtable::{MemTable, PrimaryKey};
use crate::partition::PartitionManager;
use crate::stats::TableStatistics;
use crate::wal::{CheckpointData, SyncMode, WalRecord, WalWriter};

use falcon_common::rls::RlsPolicyManager;

use crate::backup::BackupManager;
use crate::cdc::{CdcManager, DEFAULT_CDC_BUFFER_SIZE};
use crate::cdc_schema_registry::CdcSchemaRegistry;
use crate::cdc_wal_bridge::WalCdcBridge;
use crate::encryption::KeyManager;
use crate::job_scheduler::JobScheduler;
use crate::metering::ResourceMeter;
use crate::online_ddl::OnlineDdlManager;
use crate::pitr::WalArchiver;
use crate::resource_isolation::ResourceIsolator;
use crate::tenant_registry::TenantRegistry;

// ── Feature-gated storage engine imports ──
#[cfg(feature = "disk_rowstore")]
use crate::disk_rowstore::DiskRowstoreTable;

#[derive(Debug, Clone)]
pub(crate) struct TxnWriteOp {
    pub(crate) table_id: TableId,
    pub(crate) pk: PrimaryKey,
}

/// Combined per-transaction local state. Merges write-set, write-bytes tracking,
/// and deferred WAL buffer into a single DashMap entry to cut 4 DashMap ops per txn.
#[derive(Default)]
pub(crate) struct TxnLocalState {
    pub write_set: Vec<TxnWriteOp>,
    pub write_bytes: u64,
    pub wal_buf: Vec<WalRecord>,
    pub wal_preserialized: Vec<u8>,
    pub wal_pre_count: usize,
    pub cached_table: Option<Arc<MemTable>>,
}

thread_local! {
    pub(crate) static TXN_LOCAL_CACHE: std::cell::RefCell<Option<(u64, TxnId, TxnLocalState)>> = const { std::cell::RefCell::new(None) };
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
        match self.ack_timestamps.entry(replica_id) {
            dashmap::mapref::entry::Entry::Occupied(e) => {
                e.get().fetch_max(ack_ts, AtomicOrdering::Relaxed);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(AtomicU64::new(ack_ts));
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

/// CDC status snapshot for observability.
#[derive(Debug, Clone)]
pub struct CdcStatusInfo {
    pub enabled: bool,
    pub slot_count: usize,
    pub buffer_len: usize,
    pub events_emitted: u64,
    pub events_evicted: u64,
    pub bridge_lsn: u64,
    pub inflight_txns: usize,
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
    pub cdc_manager: Arc<CdcManager>,
    /// Hot/Cold Memory Tiering — cold store for compressed old MVCC version payloads.
    pub cold_store: Arc<crate::cold_store::ColdStore>,
    /// String intern pool for low-cardinality string deduplication.
    pub intern_pool: Arc<crate::cold_store::StringInternPool>,
    /// P2-1: Multi-tenant registry — manages tenant lifecycle and quota enforcement.
    pub tenant_registry: Arc<TenantRegistry>,
    /// P3-3: Resource meter — per-tenant billing and throttling.
    pub resource_meter: Arc<ResourceMeter>,
    /// CDC schema registry — tracks per-table schema version history for consumers.
    pub cdc_schema_registry: Arc<CdcSchemaRegistry>,
    /// WAL → CDC bridge: converts WAL records into CDC change events with real LSNs.
    /// `None` until explicitly configured via `configure_cdc_bridge()`.
    pub cdc_bridge: RwLock<Option<Arc<WalCdcBridge>>>,
    /// Resource isolation — throttles background I/O and CPU when foreground is pressured.
    pub resource_isolator: Arc<ResourceIsolator>,
    /// TDE block encryption for SST data files (set by configure_tde when encrypt_data=true).
    pub block_crypto: RwLock<Option<Arc<dyn crate::lsm::sst::BlockCrypto>>>,
    /// Backup lifecycle manager — tracks full/incremental backup metadata.
    pub backup_manager: Arc<BackupManager>,
    /// Job scheduler — cron-style recurring and one-shot maintenance jobs.
    pub job_scheduler: Arc<JobScheduler>,
}

impl Default for EnterpriseExtensions {
    fn default() -> Self {
        Self {
            rls_manager: RwLock::new(RlsPolicyManager::new()),
            key_manager: RwLock::new(KeyManager::disabled()),
            partition_manager: RwLock::new(PartitionManager::new()),
            wal_archiver: Arc::new(RwLock::new(WalArchiver::disabled())),
            cdc_manager: Arc::new(CdcManager::new(DEFAULT_CDC_BUFFER_SIZE)),
            cdc_schema_registry: Arc::new(CdcSchemaRegistry::new()),
            cold_store: Arc::new(crate::cold_store::ColdStore::new_in_memory()),
            intern_pool: Arc::new(crate::cold_store::StringInternPool::new()),
            tenant_registry: Arc::new(TenantRegistry::new()),
            resource_meter: Arc::new(ResourceMeter::new()),
            cdc_bridge: RwLock::new(None),
            resource_isolator: Arc::new(ResourceIsolator::disabled()),
            block_crypto: RwLock::new(None),
            backup_manager: Arc::new(BackupManager::new()),
            job_scheduler: JobScheduler::new(),
        }
    }
}

static ENGINE_ID_GEN: AtomicU64 = AtomicU64::new(1);

/// The storage engine. Owns all tables (rowstore, columnstore, disk), the catalog, and the WAL.
pub struct StorageEngine {
    pub(crate) engine_id: u64,
    pub(crate) node_role: NodeRole,
    /// Unified table handle map — one entry per table regardless of engine type.
    /// Used by DML methods for O(1) dispatch without per-engine map chains.
    pub(crate) engine_tables: DashMap<TableId, crate::table_handle::TableHandle>,
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
    /// Optional group commit syncer — when set, durable_wal_commit delegates
    /// flush to the background syncer thread instead of inline leader-follower.
    pub(crate) group_commit: Option<Arc<crate::group_commit::GroupCommitSyncer>>,
    /// Per-transaction local state (write-set + write-bytes + WAL buffer).
    pub(crate) txn_local: DashMap<TxnId, TxnLocalState>,
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
    pub(crate) online_ddl: Arc<OnlineDdlManager>,
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
    /// Max commit_ts seen during WAL recovery (0 if no recovery).
    /// TxnManager must advance its ts_counter past this value.
    pub recovered_max_ts: AtomicU64,
    /// Max txn_id seen during WAL recovery (0 if no recovery).
    pub recovered_max_txn_id: AtomicU64,
    /// TxnIds that had PrepareTxn WAL record but no Commit/Abort — in-doubt after crash.
    pub recovered_prepared_txns: parking_lot::RwLock<Vec<TxnId>>,
    /// Coordinator mapping for PrepareTxn2pc records: (coord_txn_id, shard_id) → local TxnId.
    /// Used by WalReplicationService to rebuild its prepared_txns DashMap after crash.
    pub recovered_prepared_2pc: parking_lot::RwLock<Vec<(u64, u64, TxnId)>>,
    pub recovered_prepared_gids: parking_lot::RwLock<HashMap<String, TxnId>>,
    /// Per-table write counter since last ANALYZE. Used for auto-analyze threshold.
    pub(crate) rows_modified: DashMap<TableId, AtomicU64>,
    auto_analyze_running: AtomicBool,
    /// Weak self-reference so background tasks can upgrade to Arc<Self> without
    /// requiring callers to pass Arc through the entire call chain.
    /// Set by calling `register_arc` after Arc::new(engine).
    self_weak: parking_lot::RwLock<std::sync::Weak<Self>>,
    // ── Enterprise / non-core extensions (RLS, TDE, PITR, CDC, tenancy, cold store) ──
    /// Grouped enterprise features — see [`EnterpriseExtensions`].
    pub ext: EnterpriseExtensions,
    /// Monotonic DDL epoch — bumped on every CREATE/DROP/ALTER/TRUNCATE.
    /// Used by the thread-local TABLE_CACHE in engine_dml to invalidate stale entries.
    pub(crate) ddl_epoch: AtomicU64,
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
                    {
                        crate::wal_win_async::iocp_available()
                    }
                    #[cfg(not(windows))]
                    {
                        false
                    }
                }
                _ => false,
            };

            if use_win_async {
                #[cfg(windows)]
                {
                    tracing::info!(
                        "WAL backend: WinAsync (IOCP) — wal_mode={} no_buffering={}",
                        wal_mode,
                        no_buffering,
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
    pub fn new_with_sync_mode(
        wal_dir: Option<&Path>,
        sync_mode: SyncMode,
    ) -> Result<Self, StorageError> {
        let wal = if let Some(dir) = wal_dir {
            // Large group_commit_size prevents WalWriter auto-flush;
            // GroupCommitSyncer handles flush timing instead.
            Some(Arc::new(WalWriter::open_with_options(
                dir,
                sync_mode,
                64 * 1024 * 1024,
                usize::MAX,
            )?))
        } else {
            None
        };
        Self::build_with_wal(wal_dir, wal)
    }

    /// Single source of truth for all default field values.
    /// Callers override `wal`, `data_dir`, `memory_tracker` as needed.
    fn base() -> Self {
        Self {
            engine_id: ENGINE_ID_GEN.fetch_add(1, AtomicOrdering::Relaxed),
            node_role: NodeRole::Standalone,
            engine_tables: DashMap::new(),
            data_dir: None,
            catalog: RwLock::new(Catalog::new()),
            wal: None,
            wal_flushed_lsn: AtomicU64::new(0),
            wal_flush_lock: parking_lot::Mutex::new(()),
            wal_flush_thread: parking_lot::Mutex::new(None),
            group_commit: None,
            txn_local: DashMap::new(),
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
            online_ddl: Arc::new(OnlineDdlManager::new()),
            #[cfg(feature = "online_ddl_full")]
            ddl_executor: Some(crate::online_ddl::DdlBackgroundExecutor::new(2)),
            internal_txn_counter: AtomicU64::new(1u64 << 60),
            replica_ack_tracker: ReplicaAckTracker::new(),
            #[cfg(feature = "columnstore")]
            write_path_columnstore_violations: AtomicU64::new(0),
            #[cfg(feature = "disk_rowstore")]
            write_path_disk_violations: AtomicU64::new(0),
            write_path_enforcement: WritePathEnforcement::Warn,
            lsm_sync_writes: false,
            ustm: Self::default_ustm(),
            recovered_max_ts: AtomicU64::new(0),
            recovered_max_txn_id: AtomicU64::new(0),
            recovered_prepared_txns: parking_lot::RwLock::new(Vec::new()),
            recovered_prepared_2pc: parking_lot::RwLock::new(Vec::new()),
            recovered_prepared_gids: parking_lot::RwLock::new(HashMap::new()),
            rows_modified: DashMap::new(),
            auto_analyze_running: AtomicBool::new(false),
            self_weak: parking_lot::RwLock::new(std::sync::Weak::new()),
            ext: EnterpriseExtensions::default(),
            ddl_epoch: AtomicU64::new(0),
        }
    }

    /// Internal: construct Self from a pre-built WalWriter option.
    fn build_with_wal(
        wal_dir: Option<&Path>,
        wal: Option<Arc<WalWriter>>,
    ) -> Result<Self, StorageError> {
        let mut engine = Self::base();
        engine.data_dir = wal_dir.map(std::path::Path::to_path_buf);
        engine.wal = wal;
        Ok(engine)
    }

    /// Register a weak back-reference to the Arc wrapping this engine.
    /// Call immediately after `Arc::new(engine)` so background tasks can
    /// upgrade to `Arc<Self>` without threading Arc through call chains.
    pub fn register_arc(self: &Arc<Self>) {
        *self.self_weak.write() = Arc::downgrade(self);
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
                            match guard
                                .archive_segment(&source, &fname, lsn_start, lsn_end, seg_size)
                            {
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
                archive_dir,
                retention_hours
            );
        } else if enabled && archive_dir.is_empty() {
            tracing::warn!("PITR enabled but pitr.archive_dir is empty — archiving inactive");
        } else {
            tracing::info!("PITR disabled by configuration");
        }
    }

    // ── Backup & Restore ─────────────────────────────────────────────────

    /// Start a full backup. Serialises all visible rows of all tables to `dest_dir`
    /// as a simple binary dump and records metadata in the BackupManager.
    /// Returns the backup_id that can be polled via `backup_status()`.
    pub fn backup_full(&self, dest_dir: &str, label: &str) -> Result<u64, StorageError> {
        use crate::logical_backup::dump_engine;

        let current_lsn = self.wal.as_ref().map_or(0, |w| w.current_lsn());
        let snapshot_ts = Timestamp(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        let mut meta = self
            .ext
            .backup_manager
            .start_full_backup(snapshot_ts, current_lsn);

        if !label.is_empty() {
            meta.label = label.to_owned();
        }
        let backup_id = meta.backup_id;

        let dest = std::path::Path::new(dest_dir);
        match dump_engine(self, snapshot_ts, dest) {
            Ok((bytes, table_count)) => {
                let end_lsn = self.wal.as_ref().map_or(current_lsn, |w| w.current_lsn());
                self.ext
                    .backup_manager
                    .complete_backup(backup_id, end_lsn, bytes, table_count, 0);
                tracing::info!(backup_id, dest_dir, bytes, "full backup completed");
                Ok(backup_id)
            }
            Err(e) => {
                self.ext
                    .backup_manager
                    .fail_backup(backup_id, &e.to_string());
                Err(StorageError::Io(e))
            }
        }
    }

    /// Start an incremental backup from the last full backup's end LSN.
    /// Falls back to a full backup if no prior full backup exists.
    pub fn backup_incremental(&self, dest_dir: &str, label: &str) -> Result<u64, StorageError> {
        use crate::logical_backup::dump_engine as dump_engine_fn;
        let dump_engine = dump_engine_fn;

        let base_lsn = self
            .ext
            .backup_manager
            .latest_full_backup()
            .map_or(0, |b| b.end_lsn);

        let current_lsn = self.wal.as_ref().map_or(0, |w| w.current_lsn());
        let snapshot_ts = Timestamp(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        let mut meta = self
            .ext
            .backup_manager
            .start_incremental_backup(snapshot_ts, base_lsn);

        if !label.is_empty() {
            meta.label = label.to_owned();
        }
        let backup_id = meta.backup_id;

        let dest = std::path::Path::new(dest_dir);
        match dump_engine(self, snapshot_ts, dest) {
            Ok((bytes, table_count)) => {
                self.ext.backup_manager.complete_backup(
                    backup_id,
                    current_lsn,
                    bytes,
                    table_count,
                    0,
                );
                tracing::info!(backup_id, dest_dir, bytes, "incremental backup completed");
                Ok(backup_id)
            }
            Err(e) => {
                self.ext
                    .backup_manager
                    .fail_backup(backup_id, &e.to_string());
                Err(StorageError::Io(e))
            }
        }
    }

    /// Restore the engine from a logical dump directory produced by `backup_full`/`backup_incremental`.
    /// Clears all existing data first, then replays the dump.
    pub fn restore_from_backup(&self, src_dir: &str) -> Result<u64, StorageError> {
        use crate::logical_backup::restore_engine;
        let src = std::path::Path::new(src_dir);
        restore_engine(self, src).map_err(StorageError::Io)
    }

    /// Get metadata for a backup by ID.
    pub fn backup_status(&self, backup_id: u64) -> Option<crate::backup::BackupMetadata> {
        self.ext.backup_manager.get_backup(backup_id)
    }

    /// Get all backup history (most recent first, up to `limit`).
    pub fn backup_history(&self, limit: usize) -> Vec<crate::backup::BackupMetadata> {
        self.ext.backup_manager.history(limit)
    }

    // ── Job Scheduler ────────────────────────────────────────────────────

    /// Register a one-shot backup job that fires immediately.
    pub fn schedule_backup(
        self: &Arc<Self>,
        backup_type: crate::backup::BackupType,
        dest_dir: String,
        label: String,
    ) -> crate::job_scheduler::JobId {
        use crate::job_scheduler::JobInterval;
        let engine = Arc::clone(self);
        let job_name = format!("{}_backup_{}", backup_type, label);
        self.ext.job_scheduler.register(
            &job_name,
            JobInterval::Once,
            Box::new(move || match backup_type {
                crate::backup::BackupType::Full => engine
                    .backup_full(&dest_dir, &label)
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                crate::backup::BackupType::Incremental => engine
                    .backup_incremental(&dest_dir, &label)
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
            }),
        )
    }

    /// Schedule a recurring backup job.
    pub fn schedule_recurring_backup(
        self: &Arc<Self>,
        backup_type: crate::backup::BackupType,
        dest_dir: String,
        label: String,
        interval_secs: u64,
    ) -> crate::job_scheduler::JobId {
        use crate::job_scheduler::JobInterval;
        use std::time::Duration;
        let engine = Arc::clone(self);
        let job_name = format!("{}_backup_every_{}s", backup_type, interval_secs);
        self.ext.job_scheduler.register(
            &job_name,
            JobInterval::Every(Duration::from_secs(interval_secs)),
            Box::new(move || match backup_type {
                crate::backup::BackupType::Full => engine
                    .backup_full(&dest_dir, &label)
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                crate::backup::BackupType::Incremental => engine
                    .backup_incremental(&dest_dir, &label)
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
            }),
        )
    }

    /// Cancel a job by ID.
    pub fn cancel_job(&self, job_id: crate::job_scheduler::JobId) -> bool {
        self.ext.job_scheduler.cancel(job_id)
    }

    /// Get status of all jobs.
    pub fn job_statuses(&self) -> Vec<crate::job_scheduler::JobStatus> {
        self.ext.job_scheduler.all_statuses()
    }

    /// Start the background job scheduler. Call once after engine construction.
    pub fn start_job_scheduler(self: &Arc<Self>) {
        self.ext.job_scheduler.start();
    }

    pub fn configure_tde(
        &self,
        enabled: bool,
        key_env_var: &str,
        encrypt_wal: bool,
        encrypt_data: bool,
    ) {
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
        *self.ext.key_manager.write() = crate::encryption::KeyManager::new(&passphrase);
        if encrypt_wal {
            match self
                .ext
                .key_manager
                .write()
                .generate_dek(crate::encryption::EncryptionScope::Wal)
            {
                Ok(dek_id) => {
                    if let Some(ref wal) = self.wal {
                        let mut km = self.ext.key_manager.write();
                        if let Some(enc) =
                            crate::wal::WalEncryption::from_key_manager(&mut km, dek_id)
                        {
                            wal.set_encryption(enc);
                            tracing::info!("TDE: WAL encryption enabled (DEK {})", dek_id);
                        } else {
                            tracing::error!(
                                "TDE: failed to create WAL encryption context for DEK {}",
                                dek_id
                            );
                        }
                    }
                }
                Err(e) => tracing::error!("TDE: failed to generate WAL DEK: {}", e),
            }
        }
        if encrypt_data {
            match self
                .ext
                .key_manager
                .write()
                .generate_dek(crate::encryption::EncryptionScope::Sst(0))
            {
                Ok(dek_id) => match self.ext.key_manager.read().unwrap_dek(dek_id) {
                    Ok(dek) => {
                        let crypto = Arc::new(crate::lsm::sst::AesGcmBlockCrypto::new(&dek));
                        *self.ext.block_crypto.write() = Some(crypto);
                        tracing::info!("TDE: SST block encryption enabled (DEK {})", dek_id);
                    }
                    Err(e) => tracing::error!("TDE: failed to unwrap SST DEK {}: {}", dek_id, e),
                },
                Err(e) => tracing::error!("TDE: failed to generate SST DEK: {}", e),
            }
        }
        let km = self.ext.key_manager.read();
        tracing::info!(
            "TDE enabled: algorithm=AES-256-GCM, dek_count={}, encrypt_wal={}, encrypt_data={}",
            km.dek_count(),
            encrypt_wal,
            encrypt_data,
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

        // Reuse the single CdcManager instance so DML emit + SQL poll share state
        let cdc = Arc::clone(&self.ext.cdc_manager);

        let mut bridge = WalCdcBridge::new(cdc, resolver);

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

        tracing::info!("CDC WAL bridge enabled (persist_every={})", persist_every,);
    }

    /// Get TDE status summary for SHOW commands.
    pub fn tde_status(&self) -> TdeStatusInfo {
        let km = self.ext.key_manager.read();
        let wal_encrypted = self.wal.as_ref().is_some_and(|w| w.is_encrypted());
        TdeStatusInfo {
            enabled: km.is_enabled(),
            algorithm: if km.is_enabled() {
                "AES-256-GCM".into()
            } else {
                "none".into()
            },
            dek_count: km.dek_count(),
            wal_encrypted,
        }
    }

    pub fn tde_list_deks(&self) -> Vec<TdeDekInfo> {
        let km = self.ext.key_manager.read();
        km.list_deks()
            .iter()
            .map(|d| TdeDekInfo {
                id: d.id.0,
                label: d.label.clone(),
                active: d.active,
                created_at_ms: d.created_at_ms,
            })
            .collect()
    }

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

    pub fn tde_generate_dek(
        &self,
        scope: crate::encryption::EncryptionScope,
    ) -> Result<u64, String> {
        let mut km = self.ext.key_manager.write();
        if !km.is_enabled() {
            return Err("TDE is not enabled".into());
        }
        let dek_id = km
            .generate_dek(scope)
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
    pub fn check_tenant_quota(
        &self,
        tenant_id: falcon_common::tenant::TenantId,
    ) -> Result<(), String> {
        let result = self.ext.tenant_registry.check_begin_txn(tenant_id);
        match result {
            crate::tenant_registry::QuotaCheckResult::Allowed => Ok(()),
            crate::tenant_registry::QuotaCheckResult::QpsExceeded { limit, current } => {
                Err(format!("tenant QPS limit exceeded: {current}/{limit}"))
            }
            crate::tenant_registry::QuotaCheckResult::MemoryExceeded { limit, current } => Err(
                format!("tenant memory limit exceeded: {current}/{limit} bytes"),
            ),
            crate::tenant_registry::QuotaCheckResult::TxnLimitExceeded { limit, current } => Err(
                format!("tenant concurrent transaction limit exceeded: {current}/{limit}"),
            ),
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
        self.ext
            .wal_archiver
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
            label,
            backup_id,
            lsn
        );
        Ok((backup_id, lsn))
    }

    /// Stop a base backup and register it with the archiver.
    /// Returns end_lsn.
    pub fn stop_base_backup(
        &self,
        backup_id: u64,
        label: &str,
        backup_path: &str,
    ) -> Result<u64, StorageError> {
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
            label,
            backup_id,
            lsn
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
        self.ext
            .wal_archiver
            .read()
            .list_segments()
            .into_iter()
            .cloned()
            .collect()
    }

    /// List all base backups registered with the archiver.
    pub fn list_base_backups(&self) -> Vec<crate::pitr::BaseBackup> {
        self.ext
            .wal_archiver
            .read()
            .list_base_backups()
            .into_iter()
            .cloned()
            .collect()
    }

    /// List all named restore points.
    pub fn list_restore_points(&self) -> Vec<crate::pitr::RestorePoint> {
        self.ext
            .wal_archiver
            .read()
            .list_restore_points()
            .into_iter()
            .cloned()
            .collect()
    }

    /// PITR archiver stats: (enabled, segment_count, bytes_archived).
    pub fn pitr_stats(&self) -> (bool, u64, u64) {
        let archiver = self.ext.wal_archiver.read();
        (
            archiver.is_enabled(),
            archiver.segment_count(),
            archiver.total_bytes(),
        )
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

    // ── CDC slot management ───────────────────────────────────────────────

    pub fn cdc_create_slot(&self, name: &str) -> Result<u64, String> {
        if !self.ext.cdc_manager.is_enabled() {
            return Err("CDC is not enabled — set cdc.enabled = true in falcon.toml".into());
        }
        self.ext.cdc_manager.create_slot(name).map(|id| id.0)
    }

    pub fn cdc_drop_slot(&self, name: &str) -> Result<(), String> {
        self.ext.cdc_manager.drop_slot(name).map(|_| ())
    }

    pub fn cdc_list_slots(&self) -> Vec<crate::cdc::ReplicationSlot> {
        self.ext.cdc_manager.list_slots()
    }

    pub fn cdc_advance_slot(&self, slot_name: &str, confirmed_lsn: u64) -> Result<(), String> {
        let slot = self
            .ext
            .cdc_manager
            .get_slot_by_name(slot_name)
            .ok_or_else(|| format!("replication slot '{slot_name}' not found"))?;
        self.ext
            .cdc_manager
            .advance_slot(slot.id, crate::cdc::CdcLsn(confirmed_lsn))
    }

    /// Poll up to `max_events` change events from a named slot.
    pub fn cdc_poll(
        &self,
        slot_name: &str,
        max_events: usize,
    ) -> Result<Vec<crate::cdc::ChangeEvent>, String> {
        let slot = self
            .ext
            .cdc_manager
            .get_slot_by_name(slot_name)
            .ok_or_else(|| format!("replication slot '{slot_name}' not found"))?;
        Ok(self.ext.cdc_manager.poll_changes(slot.id, max_events))
    }

    pub fn cdc_status(&self) -> CdcStatusInfo {
        let enabled = self.ext.cdc_manager.is_enabled();
        let slot_count = self.ext.cdc_manager.slot_count();
        let buffer_len = self.ext.cdc_manager.buffer_len();
        let events_emitted = self
            .ext
            .cdc_manager
            .metrics
            .events_emitted
            .load(std::sync::atomic::Ordering::Relaxed);
        let events_evicted = self
            .ext
            .cdc_manager
            .metrics
            .events_evicted
            .load(std::sync::atomic::Ordering::Relaxed);
        let bridge_lsn = self
            .ext
            .cdc_bridge
            .read()
            .as_ref()
            .map_or(0, |b| b.latest_lsn());
        let inflight_txns = self
            .ext
            .cdc_bridge
            .read()
            .as_ref()
            .map_or(0, |b| b.inflight_txn_count());
        CdcStatusInfo {
            enabled,
            slot_count,
            buffer_len,
            events_emitted,
            events_evicted,
            bridge_lsn,
            inflight_txns,
        }
    }

    /// Configure resource isolation. Must be called at startup before wrapping
    /// the engine in Arc (i.e. before GC/workers are spawned).
    pub fn configure_resource_isolation(
        &mut self,
        enabled: bool,
        io_rate_bytes_per_sec: u64,
        io_burst_bytes: u64,
        max_bg_threads: u64,
        fg_pressure_threshold: u64,
    ) {
        if !enabled {
            tracing::info!("resource isolation disabled");
            return;
        }
        use crate::resource_isolation::{IoSchedulerConfig, ResourceIsolationConfig};
        let config = ResourceIsolationConfig {
            io: IoSchedulerConfig {
                capacity_bytes: io_burst_bytes,
                rate_bytes_per_sec: io_rate_bytes_per_sec,
            },
            max_compaction_threads: max_bg_threads,
            fg_pressure_threshold,
        };
        tracing::info!(
            "resource isolation enabled: io_rate={}B/s, burst={}B, bg_threads={}, fg_threshold={}",
            io_rate_bytes_per_sec,
            io_burst_bytes,
            max_bg_threads,
            fg_pressure_threshold
        );
        self.ext.resource_isolator = Arc::new(ResourceIsolator::new(config));
    }

    /// Get resource isolation statistics.
    pub fn resource_isolation_stats(&self) -> crate::resource_isolation::ResourceIsolationStats {
        self.ext.resource_isolator.stats()
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
    pub fn write_path_columnstore_violation_count(&self) -> u64 {
        0
    }

    /// Number of OLTP write-path disk-rowstore violations (should be 0 on Primary).
    #[cfg(feature = "disk_rowstore")]
    pub fn write_path_disk_violation_count(&self) -> u64 {
        self.write_path_disk_violations
            .load(AtomicOrdering::Relaxed)
    }

    /// Always 0 when disk_rowstore feature is disabled.
    #[cfg(not(feature = "disk_rowstore"))]
    pub fn write_path_disk_violation_count(&self) -> u64 {
        0
    }

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
        let mut engine = Self::base();
        engine.data_dir = Some(wal_dir.to_path_buf());
        engine.wal = wal;
        Ok(engine)
    }

    /// Create a new in-memory-only storage engine (no WAL).
    pub fn new_in_memory() -> Self {
        Self::base()
    }

    /// Create a new in-memory-only storage engine with a memory budget.
    pub fn new_in_memory_with_budget(budget: MemoryBudget) -> Self {
        let mut engine = Self::base();
        engine.memory_tracker = MemoryTracker::new(budget);
        engine
    }

    pub fn shutdown_wal_flush(&self) {
        if let Some(ref gc) = self.group_commit {
            gc.shutdown();
        }
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
    /// `deferred` contains pre-extracted WAL records from TxnLocalState.
    pub(crate) fn durable_wal_commit(
        &self,
        record: &WalRecord,
        deferred: Option<Vec<WalRecord>>,
        preserialized: Option<(Vec<u8>, usize)>,
    ) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            let lsn = if let Some((pre, pre_count)) = &preserialized {
                if deferred.as_ref().is_none_or(|b| b.is_empty()) {
                    // Pure preserialized path: no wal_buf records to mix in
                    let lsn = wal.append_preserialized(pre, *pre_count, record)?;
                    self.wal_stats
                        .records_written
                        .fetch_add((*pre_count + 1) as u64, AtomicOrdering::Relaxed);
                    lsn
                } else {
                    // Mixed: some preserialized + some in wal_buf — fall through to deferred
                    let buf = deferred.as_ref().unwrap();
                    let mut all: Vec<&WalRecord> = buf.iter().collect();
                    all.push(record);
                    let lsn = wal.append_multi(&all)?;
                    self.wal_stats
                        .records_written
                        .fetch_add(all.len() as u64, AtomicOrdering::Relaxed);
                    lsn
                }
            } else if let Some(buf) = &deferred {
                let n = buf.len() + 1;
                let lsn = if buf.len() == 1 {
                    wal.append_multi(&[&buf[0], record])?
                } else {
                    let mut all: Vec<&WalRecord> = buf.iter().collect();
                    all.push(record);
                    wal.append_multi(&all)?
                };
                self.wal_stats
                    .records_written
                    .fetch_add(n as u64, AtomicOrdering::Relaxed);
                lsn
            } else {
                let lsn = wal.append(record)?;
                self.wal_stats
                    .records_written
                    .fetch_add(1, AtomicOrdering::Relaxed);
                lsn
            };

            if let Some(ref observer) = self.wal_observer {
                if let Some(buf) = &deferred {
                    for r in buf {
                        observer(r);
                    }
                }
                observer(record);
            }

            if let Some(ref bridge) = *self.ext.cdc_bridge.read() {
                if let Some(buf) = &deferred {
                    for r in buf {
                        bridge.on_wal_record(lsn, r);
                    }
                }
                bridge.on_wal_record(lsn, record);
            }

            // Flush path: group commit syncer or inline leader-follower.
            if let Some(ref gc) = self.group_commit {
                let record_count = if let Some((_, pc)) = &preserialized {
                    *pc as u64 + 1
                } else if let Some(buf) = &deferred {
                    buf.len() as u64 + 1
                } else {
                    1
                };
                gc.notify_and_wait(record_count)?;
            } else {
                // Pipeline flush: try_lock leader election + spin-wait followers.
                // Followers spin on flushed_lsn (cheap read) instead of blocking on mutex.
                // Only one thread becomes leader per flush round; others piggyback.
                let mut attempt = 0u32;
                loop {
                    if self.wal_flushed_lsn.load(AtomicOrdering::Acquire) >= lsn {
                        return Ok(());
                    }
                    // Try to become flush leader (non-blocking)
                    if let Some(_guard) = self.wal_flush_lock.try_lock() {
                        if self.wal_flushed_lsn.load(AtomicOrdering::Acquire) >= lsn {
                            return Ok(());
                        }
                        // Coalescing spin: let concurrent appenders land in this batch
                        let mut spins = 0u32;
                        while wal.pending_count() <= 2 && spins < 50 {
                            std::hint::spin_loop();
                            spins += 1;
                        }
                        let flushed = wal.flush_split()?;
                        self.wal_flushed_lsn.store(flushed, AtomicOrdering::Release);
                        wal.flush_cvar.notify_all();
                        return Ok(());
                    }
                    // Follower: spin on flushed_lsn, not on the mutex
                    attempt += 1;
                    if attempt < 128 {
                        for _ in 0..8 {
                            std::hint::spin_loop();
                        }
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        } else {
            if let Some(ref observer) = self.wal_observer {
                if let Some(buf) = &deferred {
                    for r in buf {
                        observer(r);
                    }
                }
                observer(record);
            }
            if let Some(ref bridge) = *self.ext.cdc_bridge.read() {
                if let Some(buf) = &deferred {
                    for r in buf {
                        bridge.on_wal_record(0, r);
                    }
                }
                bridge.on_wal_record(0, record);
            }
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

    /// Enable group commit by attaching a GroupCommitSyncer.
    /// When enabled, `durable_wal_commit` delegates flush to the syncer thread
    /// instead of inline leader-follower flush.
    pub fn enable_group_commit(&mut self, syncer: Arc<crate::group_commit::GroupCommitSyncer>) {
        self.group_commit = Some(syncer);
    }

    /// Create and enable a GroupCommitSyncer from config, starting its background thread.
    /// Returns Ok(()) if enabled, or Ok(()) if WAL is not present (no-op).
    pub fn setup_group_commit(
        &mut self,
        config: crate::group_commit::GroupCommitConfig,
    ) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            let syncer = crate::group_commit::GroupCommitSyncer::new(Arc::clone(wal), config);
            syncer.start_syncer()?;
            self.group_commit = Some(syncer);
        }
        Ok(())
    }

    /// Number of in-flight transactions with local write state.
    /// Used by the cluster layer to drain active txns during promote/failover.
    pub fn active_txn_count(&self) -> usize {
        self.txn_local.len()
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

    /// Block until the given DDL operation reaches Completed or Failed (or timeout).
    /// Primarily for tests — production code should use ddl_progress() for polling.
    pub fn wait_for_ddl(&self, ddl_id: u64, timeout_ms: u64) -> bool {
        #[cfg(feature = "online_ddl_full")]
        {
            use crate::online_ddl::DdlPhase;
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
            loop {
                if let Some(p) = self.online_ddl.progress(ddl_id) {
                    if matches!(p.phase, DdlPhase::Completed | DdlPhase::Failed) {
                        return true;
                    }
                }
                if std::time::Instant::now() >= deadline {
                    return false;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        #[cfg(not(feature = "online_ddl_full"))]
        {
            let _ = (ddl_id, timeout_ms);
            true
        }
    }

    /// Replay a DdlBackfillUpdate WAL record by calling replace_latest on the chain.
    /// Used by replica catchup and the recovery path.
    pub fn ddl_backfill_replace(
        &self,
        table_id: &TableId,
        pk: &[u8],
        new_row: falcon_common::datum::OwnedRow,
    ) {
        if let Some(handle) = self.engine_tables.get(table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                if let Some(chain) = memtable.data.get(pk) {
                    chain.replace_latest(new_row);
                }
            }
        }
    }

    /// Return progress snapshots for all tracked DDL operations.
    /// Used by `SHOW DDL STATUS`.
    pub fn ddl_progress(&self) -> Vec<crate::online_ddl::DdlProgress> {
        #[cfg(feature = "online_ddl_full")]
        {
            self.online_ddl.all_progress()
        }
        #[cfg(not(feature = "online_ddl_full"))]
        {
            Vec::new()
        }
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

    pub fn record_write(&self, txn_id: TxnId, table_id: TableId, pk: PrimaryKey) {
        let mut state = self.txn_local.entry(txn_id).or_default();
        if !state
            .write_set
            .iter()
            .any(|op| op.table_id == table_id && op.pk == pk)
        {
            state.write_set.push(TxnWriteOp { table_id, pk });
        }
    }

    pub(crate) fn record_write_no_dedup(&self, txn_id: TxnId, table_id: TableId, pk: PrimaryKey) {
        self.txn_local
            .entry(txn_id)
            .or_default()
            .write_set
            .push(TxnWriteOp { table_id, pk });
    }

    pub(crate) fn record_read(&self, txn_id: TxnId, table_id: TableId, pk: &PrimaryKey) {
        if SKIP_READ_TRACKING.with(|c| c.get()) {
            return;
        }
        self.txn_read_sets
            .entry(txn_id)
            .or_default()
            .push(TxnReadOp {
                table_id,
                pk: pk.clone(),
            });
    }

    pub(crate) fn take_read_set(&self, txn_id: TxnId) -> Vec<TxnReadOp> {
        self.txn_read_sets
            .remove(&txn_id)
            .map(|(_, reads)| reads)
            .unwrap_or_default()
    }

    /// Flush thread-local cache to DashMap. Call this when inserts run on a
    /// spawned thread but commit will happen on a different thread.
    pub fn flush_local_cache(&self) {
        TXN_LOCAL_CACHE.with(|cell| {
            let mut slot = cell.borrow_mut();
            let dominated = matches!(slot.as_ref(), Some((eid, _, _)) if *eid == self.engine_id);
            if dominated {
                let (_, txn_id, state) = slot.take().unwrap();
                let mut entry = self.txn_local.entry(txn_id).or_default();
                entry.write_bytes += state.write_bytes;
                entry.wal_buf.extend(state.wal_buf);
                if !state.wal_preserialized.is_empty() {
                    entry.wal_preserialized.extend(state.wal_preserialized);
                    entry.wal_pre_count += state.wal_pre_count;
                }
                entry.write_set.extend(state.write_set);
                if entry.cached_table.is_none() {
                    entry.cached_table = state.cached_table;
                }
            }
        });
    }

    pub(crate) fn take_local_state(&self, txn_id: TxnId) -> TxnLocalState {
        let tl = TXN_LOCAL_CACHE.with(|cell| {
            let mut slot = cell.borrow_mut();
            match slot.as_ref() {
                Some((eid, id, _)) if *eid == self.engine_id && *id == txn_id => {
                    slot.take().map(|(_, _, s)| s)
                }
                _ => None,
            }
        });
        let dm = self.txn_local.remove(&txn_id).map(|(_, s)| s);
        match (tl, dm) {
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (Some(mut a), Some(b)) => {
                a.write_bytes += b.write_bytes;
                a.wal_buf.extend(b.wal_buf);
                if !b.wal_preserialized.is_empty() {
                    a.wal_preserialized.extend(b.wal_preserialized);
                    a.wal_pre_count += b.wal_pre_count;
                }
                a.write_set.extend(b.write_set);
                if a.cached_table.is_none() {
                    a.cached_table = b.cached_table;
                }
                a
            }
            (None, None) => TxnLocalState::default(),
        }
    }

    /// Return the current write-set length for a transaction.
    /// Used by the session layer to capture a savepoint snapshot.
    pub fn write_set_snapshot(&self, txn_id: TxnId) -> usize {
        self.flush_local_cache();
        self.txn_local.get(&txn_id).map_or(0, |s| s.write_set.len())
    }

    /// Rollback all writes performed after `snapshot_len` for the given transaction.
    /// Aborts the MVCC versions for those keys and truncates the write-set.
    pub fn rollback_write_set_after(&self, txn_id: TxnId, snapshot_len: usize) {
        self.flush_local_cache();
        let mut entry = match self.txn_local.get_mut(&txn_id) {
            Some(e) => e,
            None => return,
        };
        if entry.write_set.len() <= snapshot_len {
            return;
        }
        let ops_to_abort: Vec<TxnWriteOp> = entry.write_set.drain(snapshot_len..).collect();
        drop(entry);

        self.apply_abort_to_write_set(txn_id, &ops_to_abort);
    }

    /// Rollback the read-set to a snapshot point (truncate entries after snapshot_len).
    pub fn rollback_read_set_after(&self, txn_id: TxnId, snapshot_len: usize) {
        if let Some(mut entry) = self.txn_read_sets.get_mut(&txn_id) {
            entry.truncate(snapshot_len);
        }
    }

    /// Return write-set keys as (table_id_u64, pk_bytes) for SSI anti-dependency detection.
    pub fn write_set_keys(&self, txn_id: TxnId) -> Vec<(u64, Vec<u8>)> {
        self.flush_local_cache();
        self.txn_local.get(&txn_id).map_or_else(Vec::new, |s| {
            s.write_set
                .iter()
                .map(|op| (op.table_id.0, op.pk.clone()))
                .collect()
        })
    }

    /// Return the current read-set length for a transaction.
    pub fn read_set_snapshot(&self, txn_id: TxnId) -> usize {
        self.flush_local_cache();
        self.txn_read_sets.get(&txn_id).map_or(0, |rs| rs.len())
    }

    /// Pre-validate unique constraints for all rowstore tables in the write-set.
    /// Must be called BEFORE writing the Commit WAL record, so a crash between
    /// validation failure and WAL write cannot leave a false commit on disk.
    pub(crate) fn pre_validate_write_set(
        &self,
        txn_id: TxnId,
        write_set: &[TxnWriteOp],
    ) -> Result<(), StorageError> {
        if write_set.is_empty() {
            return Ok(());
        }
        // Single-key fast path
        if write_set.len() == 1 {
            let op = &write_set[0];
            if let Some(handle) = self.engine_tables.get(&op.table_id) {
                if let Some(table) = handle.as_rowstore() {
                    if table.has_secondary_idx.load(AtomicOrdering::Acquire) {
                        table.validate_unique_constraints_for_commit(
                            txn_id,
                            std::slice::from_ref(&op.pk),
                        )?;
                    }
                }
            }
            return Ok(());
        }
        // Group keys by table and validate each rowstore table
        let first_table = write_set[0].table_id;
        if write_set.iter().all(|op| op.table_id == first_table) {
            if let Some(handle) = self.engine_tables.get(&first_table) {
                if let Some(table) = handle.as_rowstore() {
                    if table.has_secondary_idx.load(AtomicOrdering::Acquire) {
                        let keys: Vec<PrimaryKey> =
                            write_set.iter().map(|op| op.pk.clone()).collect();
                        table.validate_unique_constraints_for_commit(txn_id, &keys)?;
                    }
                }
            }
            return Ok(());
        }
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table
                .entry(op.table_id)
                .or_default()
                .push(op.pk.clone());
        }
        for (table_id, keys) in &keys_by_table {
            if let Some(handle) = self.engine_tables.get(table_id) {
                if let Some(table) = handle.as_rowstore() {
                    if table.has_secondary_idx.load(AtomicOrdering::Acquire) {
                        table.validate_unique_constraints_for_commit(txn_id, keys)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply MVCC commits to the write-set. Called AFTER WAL commit and pre-validation.
    /// Unique constraints were already checked by `pre_validate_write_set`, so
    /// `commit_keys` will not fail on unique violations (only re-checks as a safety net).
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
            if let Some(handle) = self.engine_tables.get(&op.table_id) {
                if let Some(table) = handle.as_rowstore() {
                    if !table.has_secondary_idx.load(AtomicOrdering::Acquire) {
                        if let Some(chain) = table.data.get(&op.pk) {
                            let delta = chain.commit_no_report(txn_id, commit_ts);
                            match delta {
                                1 => {
                                    table
                                        .committed_row_count
                                        .fetch_add(1, AtomicOrdering::Relaxed);
                                    table.pk_order.write().insert(op.pk.clone());
                                }
                                -1 => {
                                    table
                                        .committed_row_count
                                        .fetch_sub(1, AtomicOrdering::Relaxed);
                                    table.pk_order.write().remove(&op.pk);
                                }
                                _ => {}
                            }
                        }
                        return Ok(());
                    }
                    return table.commit_keys(txn_id, commit_ts, std::slice::from_ref(&op.pk));
                }
                handle.commit_keys_batch(std::slice::from_ref(&op.pk), txn_id, commit_ts)?;
            }
            return Ok(());
        }
        let first_table = write_set[0].table_id;
        if write_set.iter().all(|op| op.table_id == first_table) {
            let keys: Vec<PrimaryKey> = write_set.iter().map(|op| op.pk.clone()).collect();
            if let Some(handle) = self.engine_tables.get(&first_table) {
                if let Some(table) = handle.as_rowstore() {
                    return table.commit_keys(txn_id, commit_ts, &keys);
                }
                handle.commit_keys_batch(&keys, txn_id, commit_ts)?;
            }
            return Ok(());
        }
        // Multi-table: commit each table type
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table
                .entry(op.table_id)
                .or_default()
                .push(op.pk.clone());
        }
        for (table_id, keys) in &keys_by_table {
            if let Some(handle) = self.engine_tables.get(table_id) {
                match handle.value() {
                    crate::table_handle::TableHandle::Rowstore(t) => {
                        t.commit_keys(txn_id, commit_ts, keys)?
                    }
                    #[cfg(feature = "columnstore")]
                    crate::table_handle::TableHandle::Columnstore(t) => {
                        t.commit(txn_id, commit_ts);
                    }
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
            let keys: Vec<PrimaryKey> = write_set.iter().map(|op| op.pk.clone()).collect();
            if let Some(handle) = self.engine_tables.get(&first_table) {
                if let Some(table) = handle.as_rowstore() {
                    table.abort_keys(txn_id, &keys);
                    return;
                }
                handle.abort_keys_batch(&keys, txn_id);
            }
            return;
        }
        // Multi-table
        let mut keys_by_table: HashMap<TableId, Vec<PrimaryKey>> = HashMap::new();
        for op in write_set {
            keys_by_table
                .entry(op.table_id)
                .or_default()
                .push(op.pk.clone());
        }
        for (table_id, keys) in keys_by_table {
            if let Some(handle) = self.engine_tables.get(&table_id) {
                match handle.value() {
                    crate::table_handle::TableHandle::Rowstore(t) => t.abort_keys(txn_id, &keys),
                    #[cfg(feature = "columnstore")]
                    crate::table_handle::TableHandle::Columnstore(t) => t.abort(txn_id),
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
        self.engine_tables
            .get(&id)
            .and_then(|h| h.as_rowstore().cloned())
    }

    pub fn get_table_schema(&self, name: &str) -> Option<TableSchema> {
        self.catalog.read().find_table(name).cloned()
    }

    pub fn get_table_schema_by_id(&self, id: TableId) -> Option<TableSchema> {
        self.catalog.read().find_table_by_id(id).cloned()
    }

    /// Return the set of indexed columns for a table: Vec<(column_idx, unique)>.
    pub fn get_indexed_columns(&self, table_id: TableId) -> Vec<(usize, bool)> {
        if let Some(handle) = self.engine_tables.get(&table_id) {
            let Some(table) = handle.as_rowstore() else {
                return vec![];
            };
            let indexes = table.secondary_indexes.read();
            indexes
                .iter()
                .map(|idx| (idx.column_idx, idx.unique))
                .collect()
        } else {
            vec![]
        }
    }

    /// GIN index scan: look up candidate PKs for a tsvector column matching the given search terms.
    /// Returns None if no GIN index exists for the column.
    /// The caller must still apply a residual `@@` filter for correctness (NOT/AND semantics).
    pub fn gin_scan(
        &self,
        table_id: TableId,
        column_idx: usize,
        terms: &[crate::memtable::GinSearchTerm],
        txn_id: TxnId,
        read_ts: Timestamp,
    ) -> Option<Vec<(Vec<u8>, falcon_common::datum::OwnedRow)>> {
        let handle = self.engine_tables.get(&table_id)?;
        let memtable = handle.as_rowstore()?;
        let candidate_pks = memtable.gin_search(column_idx, terms)?;
        if candidate_pks.is_empty() {
            return Some(vec![]);
        }
        let mut rows = Vec::with_capacity(candidate_pks.len());
        for pk in candidate_pks {
            if let Some(chain) = memtable.data.get(&pk) {
                if let Some(arc_row) = chain.read_for_txn(txn_id, read_ts) {
                    rows.push((pk, (*arc_row).clone()));
                }
            }
        }
        Some(rows)
    }

    /// Check whether a table has a GIN index on any tsvector column.
    pub fn has_gin_index(&self, table_id: TableId, column_idx: usize) -> bool {
        if let Some(handle) = self.engine_tables.get(&table_id) {
            if let Some(memtable) = handle.as_rowstore() {
                let gin = memtable.gin_indexes.read();
                return gin.iter().any(|g| g.column_idx == column_idx);
            }
        }
        false
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
        let handle = self
            .engine_tables
            .get(&schema.id)
            .ok_or(StorageError::TableNotFound(schema.id))?;

        // Snapshot scan: use a sentinel txn/ts to read all committed data
        let read_txn = TxnId(u64::MAX);
        let read_ts = Timestamp(u64::MAX);
        let rows: Vec<Vec<falcon_common::datum::Datum>> = handle
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

        // Update cached threshold on MemTable so commit fast path avoids table_stats lookup
        if let Some(table) = handle.as_rowstore() {
            let new_threshold = (stats.row_count / 5).max(500);
            table
                .analyze_threshold
                .store(new_threshold, AtomicOrdering::Relaxed);
        }

        self.table_stats.insert(schema.id, stats.clone());
        Ok(stats)
    }

    /// Increment the per-table modification counter after a commit.
    /// Triggers auto-analyze when the table has been significantly modified.
    fn track_rows_modified(&self, write_set: &[TxnWriteOp]) {
        if write_set.is_empty() {
            return;
        }
        for op in write_set {
            let counter = self
                .rows_modified
                .entry(op.table_id)
                .or_insert_with(|| AtomicU64::new(0));
            let prev = counter.fetch_add(1, AtomicOrdering::Relaxed);

            let threshold = if let Some(stats) = self.table_stats.get(&op.table_id) {
                (stats.row_count / 5).max(500)
            } else {
                500
            };

            if prev + 1 >= threshold {
                // Reset first to prevent stampede from concurrent commits.
                counter.store(0, AtomicOrdering::Relaxed);

                // Guard: only one auto-analyze thread at a time.
                if self
                    .auto_analyze_running
                    .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Relaxed)
                    .is_err()
                {
                    continue;
                }

                // Upgrade weak ref to Arc — skip if engine is being dropped.
                let engine = match self.self_weak.read().upgrade() {
                    Some(arc) => arc,
                    None => {
                        self.auto_analyze_running
                            .store(false, AtomicOrdering::Release);
                        continue;
                    }
                };
                let table_id = op.table_id;
                std::thread::Builder::new()
                    .name("falcon-auto-analyze".into())
                    .spawn(move || {
                        let table_name = engine
                            .catalog
                            .read()
                            .find_table_by_id(table_id)
                            .map(|s| s.name.clone());
                        if let Some(name) = table_name {
                            if let Ok(stats) = engine.analyze_table(&name) {
                                tracing::debug!(
                                    table = %name,
                                    row_count = stats.row_count,
                                    "auto-analyze completed"
                                );
                            }
                        }
                        engine
                            .auto_analyze_running
                            .store(false, AtomicOrdering::Release);
                    })
                    .ok();
            }
        }
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

    pub fn prepare_txn_named(&self, txn_id: TxnId, gid: String) -> Result<(), StorageError> {
        self.append_and_flush_wal(&WalRecord::PrepareTxnNamed { txn_id, gid })?;
        Ok(())
    }

    /// Prepare with coordinator mapping for cross-node 2PC recovery.
    pub fn prepare_txn_2pc(
        &self,
        txn_id: TxnId,
        coord_txn_id: u64,
        shard_id: u64,
    ) -> Result<(), StorageError> {
        self.append_and_flush_wal(&WalRecord::PrepareTxn2pc {
            txn_id,
            coord_txn_id,
            shard_id,
        })?;
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
                if let Some(handle) = self.engine_tables.get(&op.table_id) {
                    if let Some(table) = handle.as_rowstore() {
                        if table.has_committed_write_after(&op.pk, txn_id, start_ts) {
                            return Err(StorageError::SerializationFailure);
                        }
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
        let mut state = self.take_local_state(txn_id);

        if state.write_set.is_empty() {
            self.take_read_set(txn_id);
            return Ok(());
        }

        let ws_bytes = if state.write_bytes > 0 {
            state.write_bytes
        } else {
            self.estimate_write_set_bytes(txn_id, &state.write_set)
        };

        // Fused fast path: single-key rowstore, no secondary indexes.
        // Uses cached_table from insert path to avoid engine_tables DashMap lookup.
        if state.write_set.len() == 1 {
            self.take_read_set(txn_id);
            let op = &state.write_set[0];
            let table = state.cached_table.take().or_else(|| {
                self.engine_tables
                    .get(&op.table_id)
                    .and_then(|h| h.as_rowstore().cloned())
            });
            if let Some(table) = table {
                let no_idx = !table.has_secondary_idx.load(AtomicOrdering::Acquire);
                if !no_idx {
                    if let Err(e) = table.validate_unique_constraints_for_commit(
                        txn_id,
                        std::slice::from_ref(&op.pk),
                    ) {
                        self.memory_tracker.dealloc_write_buffer(ws_bytes);
                        table.abort_keys(txn_id, std::slice::from_ref(&op.pk));
                        let _ = self.append_wal(&WalRecord::AbortTxnLocal { txn_id });
                        return Err(e);
                    }
                }

                self.memory_tracker.dealloc_write_buffer(ws_bytes);
                self.memory_tracker.alloc_mvcc(ws_bytes);

                let deferred = if state.wal_buf.is_empty() {
                    None
                } else {
                    Some(state.wal_buf)
                };
                let pre = if state.wal_preserialized.is_empty() {
                    None
                } else {
                    Some((state.wal_preserialized, state.wal_pre_count))
                };
                self.durable_wal_commit(
                    &WalRecord::CommitTxnLocal { txn_id, commit_ts },
                    deferred,
                    pre,
                )?;

                if no_idx {
                    if let Some(chain) = table.data.get(&op.pk) {
                        let delta = chain.commit_no_report(txn_id, commit_ts);
                        match delta {
                            1 => {
                                table
                                    .committed_row_count
                                    .fetch_add(1, AtomicOrdering::Relaxed);
                                table.pk_order.write().insert(op.pk.clone());
                            }
                            -1 => {
                                table
                                    .committed_row_count
                                    .fetch_sub(1, AtomicOrdering::Relaxed);
                                table.pk_order.write().remove(&op.pk);
                            }
                            _ => {}
                        }
                    }
                } else if let Err(e) =
                    table.commit_keys(txn_id, commit_ts, std::slice::from_ref(&op.pk))
                {
                    self.memory_tracker.dealloc_mvcc(ws_bytes);
                    table.abort_keys(txn_id, std::slice::from_ref(&op.pk));
                    let _ = self.append_and_flush_wal(&WalRecord::AbortTxnLocal { txn_id });
                    return Err(e);
                }

                if self.ext.cdc_manager.is_enabled() {
                    self.ext.cdc_manager.emit_commit(txn_id);
                }
                let prev = table.rows_modified.fetch_add(1, AtomicOrdering::Relaxed);
                let threshold = table.analyze_threshold.load(AtomicOrdering::Relaxed);
                if prev + 1 >= threshold {
                    table.rows_modified.store(0, AtomicOrdering::Relaxed);
                    if self
                        .auto_analyze_running
                        .compare_exchange(
                            false,
                            true,
                            AtomicOrdering::AcqRel,
                            AtomicOrdering::Relaxed,
                        )
                        .is_ok()
                    {
                        if let Some(engine) = self.self_weak.read().upgrade() {
                            let tid = op.table_id;
                            std::thread::Builder::new()
                                .name("falcon-auto-analyze".into())
                                .spawn(move || {
                                    let name = engine
                                        .catalog
                                        .read()
                                        .find_table_by_id(tid)
                                        .map(|s| s.name.clone());
                                    if let Some(name) = name {
                                        let _ = engine.analyze_table(&name);
                                    }
                                    engine
                                        .auto_analyze_running
                                        .store(false, AtomicOrdering::Release);
                                })
                                .ok();
                        } else {
                            self.auto_analyze_running
                                .store(false, AtomicOrdering::Release);
                        }
                    }
                }
                return Ok(());
            }
            // Non-rowstore single-key: fall through to generic path
        } else {
            self.take_read_set(txn_id);
        }

        // Generic multi-key / non-rowstore path
        if let Err(e) = self.pre_validate_write_set(txn_id, &state.write_set) {
            self.memory_tracker.dealloc_write_buffer(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &state.write_set);
            if let Err(wal_err) = self.append_wal(&WalRecord::AbortTxnLocal { txn_id }) {
                tracing::warn!(
                    "Failed to write abort WAL for pre-validate failure (txn={:?}): {}",
                    txn_id,
                    wal_err
                );
            }
            return Err(e);
        }

        self.memory_tracker.dealloc_write_buffer(ws_bytes);
        self.memory_tracker.alloc_mvcc(ws_bytes);

        let deferred = if state.wal_buf.is_empty() {
            None
        } else {
            Some(state.wal_buf)
        };
        let pre = if state.wal_preserialized.is_empty() {
            None
        } else {
            Some((state.wal_preserialized, state.wal_pre_count))
        };
        self.durable_wal_commit(
            &WalRecord::CommitTxnLocal { txn_id, commit_ts },
            deferred,
            pre,
        )?;

        if let Err(e) = self.apply_commit_to_write_set(txn_id, commit_ts, &state.write_set) {
            self.memory_tracker.dealloc_mvcc(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &state.write_set);
            if let Err(wal_err) = self.append_and_flush_wal(&WalRecord::AbortTxnLocal { txn_id }) {
                tracing::error!("CRITICAL: commit WAL exists but abort WAL failed (txn={:?}): {} — recovery may see phantom commit", txn_id, wal_err);
            }
            return Err(e);
        }

        if self.ext.cdc_manager.is_enabled() {
            self.ext.cdc_manager.emit_commit(txn_id);
        }

        self.track_rows_modified(&state.write_set);

        Ok(())
    }

    pub(crate) fn commit_txn_global(
        &self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        let state = self.take_local_state(txn_id);
        let _read_set = self.take_read_set(txn_id);

        if state.write_set.is_empty() {
            return Ok(());
        }

        let ws_bytes = if state.write_bytes > 0 {
            state.write_bytes
        } else {
            self.estimate_write_set_bytes(txn_id, &state.write_set)
        };

        // Validate unique constraints BEFORE writing Commit to WAL.
        if let Err(e) = self.pre_validate_write_set(txn_id, &state.write_set) {
            self.memory_tracker.dealloc_write_buffer(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &state.write_set);
            if let Err(wal_err) = self.append_wal(&WalRecord::AbortTxn { txn_id }) {
                tracing::warn!(
                    "Failed to write abort WAL for pre-validate failure (txn={:?}): {}",
                    txn_id,
                    wal_err
                );
            }
            return Err(e);
        }

        self.memory_tracker.dealloc_write_buffer(ws_bytes);
        self.memory_tracker.alloc_mvcc(ws_bytes);

        let deferred = if state.wal_buf.is_empty() {
            None
        } else {
            Some(state.wal_buf)
        };
        let pre = if state.wal_preserialized.is_empty() {
            None
        } else {
            Some((state.wal_preserialized, state.wal_pre_count))
        };
        self.durable_wal_commit(
            &WalRecord::CommitTxnGlobal { txn_id, commit_ts },
            deferred,
            pre,
        )?;

        if let Err(e) = self.apply_commit_to_write_set(txn_id, commit_ts, &state.write_set) {
            // Safety net: should not happen after pre_validate, but handle defensively.
            self.memory_tracker.dealloc_mvcc(ws_bytes);
            self.apply_abort_to_write_set(txn_id, &state.write_set);
            if let Err(wal_err) = self.append_and_flush_wal(&WalRecord::AbortTxn { txn_id }) {
                tracing::error!("CRITICAL: commit WAL exists but abort WAL failed (txn={:?}): {} — recovery may see phantom commit", txn_id, wal_err);
            }
            return Err(e);
        }

        if self.ext.cdc_manager.is_enabled() {
            self.ext.cdc_manager.emit_commit(txn_id);
        }

        self.track_rows_modified(&state.write_set);

        Ok(())
    }

    pub(crate) fn abort_txn_local(&self, txn_id: TxnId) -> Result<(), StorageError> {
        let state = self.take_local_state(txn_id);
        let _read_set = self.take_read_set(txn_id);

        let ws_bytes = if state.write_bytes > 0 {
            state.write_bytes
        } else {
            self.estimate_write_set_bytes(txn_id, &state.write_set)
        };
        self.memory_tracker.dealloc_write_buffer(ws_bytes);

        self.apply_abort_to_write_set(txn_id, &state.write_set);

        self.append_wal(&WalRecord::AbortTxnLocal { txn_id })?;

        self.ext.cdc_manager.emit_rollback(txn_id);

        Ok(())
    }

    pub(crate) fn abort_txn_global(&self, txn_id: TxnId) -> Result<(), StorageError> {
        let state = self.take_local_state(txn_id);
        let _read_set = self.take_read_set(txn_id);

        let ws_bytes = if state.write_bytes > 0 {
            state.write_bytes
        } else {
            self.estimate_write_set_bytes(txn_id, &state.write_set)
        };
        self.memory_tracker.dealloc_write_buffer(ws_bytes);

        self.apply_abort_to_write_set(txn_id, &state.write_set);

        self.append_wal(&WalRecord::AbortTxnGlobal { txn_id })?;

        self.ext.cdc_manager.emit_rollback(txn_id);

        Ok(())
    }

    /// Estimate total bytes for a write-set by looking up the version data.
    fn estimate_write_set_bytes(&self, _txn_id: TxnId, write_set: &[TxnWriteOp]) -> u64 {
        let mut total = 0u64;
        for op in write_set {
            if let Some(handle) = self.engine_tables.get(&op.table_id) {
                if let Some(table) = handle.as_rowstore() {
                    if let Some(chain) = table.data.get(&op.pk) {
                        // Use the head version's estimated size
                        let head = chain.head.read();
                        if let Some(ref ver) = *head {
                            total += crate::mvcc::VersionChain::estimate_version_bytes(ver);
                        }
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
    /// Returns the LSN up to which the WAL has been durably flushed.
    /// Returns 0 if WAL is disabled or no flushes have occurred.
    pub fn flushed_wal_lsn(&self) -> u64 {
        self.wal_flushed_lsn.load(AtomicOrdering::Acquire)
    }

    /// Current WAL backlog bytes (written but not yet replicated/archived).
    /// Returns 0 if WAL is disabled.
    pub fn wal_backlog_bytes(&self) -> u64 {
        self.wal_stats
            .backlog_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
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
        self.ext
            .cold_store
            .metrics
            .cold_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
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

        for entry in &self.engine_tables {
            #[cfg(feature = "columnstore")]
            if let crate::table_handle::TableHandle::Columnstore(cs) = entry.value() {
                let purged = cs.gc_purge(watermark);
                aggregate.reclaimed_versions += purged as u64;
                continue;
            }
            let Some(memtable) = entry.value().as_rowstore() else {
                continue;
            };
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

    /// Estimate the in-memory size of a single table in bytes.
    pub fn estimate_table_size_bytes(&self, table_id: TableId) -> u64 {
        self.engine_tables.get(&table_id).map_or(0, |handle| {
            match handle.value() {
                #[cfg(feature = "columnstore")]
                crate::table_handle::TableHandle::Columnstore(cs) => {
                    (cs.row_count_approx() as u64) * 128
                }
                _ => handle.as_rowstore().map_or(0, |t| {
                    (t.data.len() as u64) * 128
                }),
            }
        })
    }

    /// Estimate total database size across all tables.
    pub fn estimate_total_size_bytes(&self) -> u64 {
        let snap = self.memory_tracker.snapshot();
        snap.total_bytes
    }

    /// Get the total number of version chain entries across all tables.
    /// Useful for observability (memory pressure estimation).
    pub fn total_chain_count(&self) -> usize {
        self.engine_tables
            .iter()
            .map(|e| {
                match e.value() {
                    #[cfg(feature = "columnstore")]
                    crate::table_handle::TableHandle::Columnstore(cs) => cs.row_count_approx(),
                    _ => e.value().as_rowstore().map_or(0, |t| t.data.len()),
                }
            })
            .sum()
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

    /// Check memory pressure before accepting a write operation.
    /// Returns `Err(MemoryLimitExceeded)` in Critical state so the client can retry later.
    pub fn check_write_pressure(&self) -> Result<(), StorageError> {
        if !self.memory_tracker.is_enabled() {
            return Ok(());
        }
        match self.memory_tracker.pressure_state() {
            crate::memory::PressureState::Critical => {
                self.memory_tracker.record_rejected();
                let used = self.memory_tracker.total_bytes();
                let limit = self.memory_tracker.budget().hard_limit;
                Err(StorageError::MemoryLimitExceeded {
                    used_bytes: used,
                    limit_bytes: limit,
                })
            }
            _ => Ok(()),
        }
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
        wal.flush()
            .map_err(|e| StorageError::Wal(format!("checkpoint: pre-flush failed: {e}")))?;

        // Capture segment_id BEFORE scan — WAL replay is idempotent so
        // replaying extra records from this segment is safe.
        let segment_id = wal.current_segment_id();

        // Use MAX-1 to capture all committed versions. The scan is not fully
        // atomic w.r.t. concurrent commits, but Fix #2 (replaying ALL WAL
        // records from the checkpoint segment without marker skipping) ensures
        // idempotent correction of any partially-captured transactions.
        let read_ts = Timestamp(u64::MAX - 1);
        let dummy_txn = TxnId(0);

        let catalog = self.catalog.read().clone();

        let mut table_data = Vec::new();
        let mut total_rows = 0;
        for entry in &self.engine_tables {
            let Some(memtable) = entry.value().as_rowstore() else {
                continue;
            };
            let table_id = *entry.key();
            let rows: Vec<(Vec<u8>, OwnedRow)> = memtable.scan(dummy_txn, read_ts);
            total_rows += rows.len();
            table_data.push((table_id, rows));
        }

        // Capture lsn AFTER scan for the checkpoint metadata
        let lsn = wal.current_lsn();

        let ckpt = CheckpointData {
            catalog,
            table_data,
            wal_segment_id: segment_id,
            wal_lsn: lsn,
        };

        let wal_dir = wal.wal_dir();
        ckpt.write_to_dir(&wal_dir).map_err(|e| {
            StorageError::Wal(format!(
                "checkpoint: write_to_dir({}) failed: {e}",
                wal_dir.display()
            ))
        })?;

        wal.append(&WalRecord::Checkpoint {
            timestamp: Timestamp(lsn),
        })
        .map_err(|e| StorageError::Wal(format!("checkpoint: append record failed: {e}")))?;
        wal.flush()
            .map_err(|e| StorageError::Wal(format!("checkpoint: post-flush failed: {e}")))?;

        // Verify checkpoint file actually landed before purging old segments.
        // On Windows, write_to_dir's remove+rename has a crash window;
        // if the file is missing we skip purge to avoid data loss.
        let checkpoint_path = wal_dir.join("checkpoint.bin");
        let purged_segments = if checkpoint_path.exists() {
            wal.purge_segments_before(segment_id)?
        } else {
            tracing::warn!("Checkpoint file not found after write — skipping WAL purge");
            0
        };

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
        for entry in &self.engine_tables {
            let table_id = *entry.key();
            let rows: Vec<(Vec<u8>, OwnedRow)> = entry.value().scan(dummy_txn, read_ts);
            if !rows.is_empty() {
                table_data.push((table_id, rows));
            }
        }

        let (segment_id, lsn) = self
            .wal
            .as_ref()
            .map_or((0, 0), |wal| (wal.current_segment_id(), wal.current_lsn()));

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
        self.engine_tables.clear();

        // 3. Restore table data from checkpoint
        for (table_id, rows) in &ckpt.table_data {
            let schema = self.catalog.read().find_table_by_id(*table_id).cloned();
            if let Some(schema) = schema {
                #[cfg(feature = "columnstore")]
                if schema.storage_type == falcon_common::schema::StorageType::Columnstore {
                    let cs = Arc::new(crate::columnstore::ColumnStoreTable::new(schema));
                    let sentinel_txn = TxnId(0);
                    let sentinel_ts = Timestamp(1);
                    for (_pk, row) in rows {
                        let _ = cs.insert(row.clone(), sentinel_txn);
                    }
                    cs.commit(sentinel_txn, sentinel_ts);
                    cs.freeze_buffer();
                    self.engine_tables.insert(
                        *table_id,
                        crate::table_handle::TableHandle::Columnstore(cs),
                    );
                    continue;
                }
                let memtable = Arc::new(MemTable::new(schema));
                let sentinel_txn = TxnId(0);
                let sentinel_ts = Timestamp(1);
                for (pk, row) in rows {
                    let chain = Arc::new(crate::mvcc::VersionChain::new());
                    chain.prepend(sentinel_txn, Some(row.clone()));
                    chain.commit(sentinel_txn, sentinel_ts);
                    memtable.data.insert(pk.clone(), chain);
                }
                memtable.ensure_gin_indexes();
                memtable.rebuild_secondary_indexes();
                self.engine_tables.insert(
                    *table_id,
                    crate::table_handle::TableHandle::Rowstore(memtable),
                );
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
                    let sentinel_txn = TxnId(0);
                    let sentinel_ts = Timestamp(1);
                    match schema.storage_type {
                        #[cfg(feature = "rocksdb")]
                        falcon_common::schema::StorageType::RocksDbRowstore => {
                            let data_dir = engine
                                .data_dir
                                .as_deref()
                                .unwrap_or_else(|| std::path::Path::new("."));
                            let rdb_dir = data_dir.join(format!("rocksdb_table_{}", table_id.0));
                            let _ = std::fs::remove_dir_all(&rdb_dir);
                            if let Ok(rdb) =
                                crate::rocksdb_table::RocksDbTable::open(schema, &rdb_dir)
                            {
                                // Write checkpoint rows as committed MVCC values
                                for (pk, row) in rows {
                                    if let Ok(data) = bincode::serialize(row) {
                                        let mv = crate::lsm::mvcc_encoding::MvccValue {
                                            txn_id: sentinel_txn,
                                            status:
                                                crate::lsm::mvcc_encoding::MvccStatus::Committed,
                                            commit_ts: sentinel_ts,
                                            is_tombstone: false,
                                            data,
                                        };
                                        let _ = rdb.insert_committed(pk, &mv);
                                    }
                                }
                                let rdb = Arc::new(rdb);
                                engine.engine_tables.insert(
                                    *table_id,
                                    crate::table_handle::TableHandle::RocksDb(Arc::clone(&rdb)),
                                );
                            }
                        }
                        #[cfg(feature = "columnstore")]
                        falcon_common::schema::StorageType::Columnstore => {
                            let cs = Arc::new(crate::columnstore::ColumnStoreTable::new(schema));
                            for (_pk, row) in rows {
                                let _ = cs.insert(row.clone(), sentinel_txn);
                            }
                            cs.commit(sentinel_txn, sentinel_ts);
                            cs.freeze_buffer();
                            engine.engine_tables.insert(
                                *table_id,
                                crate::table_handle::TableHandle::Columnstore(cs),
                            );
                        }
                        _ => {
                            let memtable = Arc::new(MemTable::new(schema));
                            for (pk, row) in rows {
                                let chain = Arc::new(crate::mvcc::VersionChain::new());
                                chain.prepend(sentinel_txn, Some(row.clone()));
                                chain.commit(sentinel_txn, sentinel_ts);
                                memtable.data.insert(pk.clone(), chain);
                            }
                            engine.engine_tables.insert(
                                *table_id,
                                crate::table_handle::TableHandle::Rowstore(memtable),
                            );
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
        let mut committed_write_sets: HashMap<TxnId, (Timestamp, Vec<TxnWriteOp>)> = HashMap::new();
        let mut prepared_txn_ids: std::collections::HashSet<TxnId> =
            std::collections::HashSet::new();
        let mut prepared_2pc_mappings: Vec<(u64, u64, TxnId)> = Vec::new();
        let mut prepared_gid_mappings: HashMap<String, TxnId> = HashMap::new();
        let mut max_commit_ts: u64 = 0;
        let mut max_txn_id: u64 = 0;

        // Replay ALL records from the checkpoint segment (no marker skipping).
        // Idempotent: rows already in checkpoint will fail insert silently,
        // and their commit records will find empty write_sets (no-op).
        for record in &records {
            match record {
                WalRecord::BeginTxn { .. }
                | WalRecord::Checkpoint { .. }
                | WalRecord::CoordinatorPrepare { .. }
                | WalRecord::CoordinatorCommit { .. }
                | WalRecord::CoordinatorAbort { .. } => {
                    // Metadata-only / no-op records during replay.
                }
                WalRecord::PrepareTxn { txn_id } => {
                    prepared_txn_ids.insert(*txn_id);
                }
                WalRecord::PrepareTxnNamed { txn_id, gid } => {
                    prepared_txn_ids.insert(*txn_id);
                    prepared_gid_mappings.insert(gid.clone(), *txn_id);
                }
                WalRecord::PrepareTxn2pc {
                    txn_id,
                    coord_txn_id,
                    shard_id,
                } => {
                    prepared_txn_ids.insert(*txn_id);
                    prepared_2pc_mappings.push((*coord_txn_id, *shard_id, *txn_id));
                }
                WalRecord::CreateDatabase { name, owner } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.create_database(name, owner) {
                        tracing::debug!("WAL recovery: create_database idempotent skip: {e}");
                    }
                }
                WalRecord::DropDatabase { name } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.drop_database(name) {
                        tracing::debug!("WAL recovery: drop_database idempotent skip: {e}");
                    }
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
                            engine.engine_tables.insert(
                                schema.id,
                                crate::table_handle::TableHandle::Rowstore(table),
                            );
                        }
                        #[cfg(feature = "rocksdb")]
                        StorageType::RocksDbRowstore => {
                            let data_dir = engine
                                .data_dir
                                .as_deref()
                                .unwrap_or_else(|| std::path::Path::new("."));
                            let rdb_dir = data_dir.join(format!("rocksdb_table_{}", schema.id.0));
                            // Wipe stale RocksDB state — WAL is the source of truth.
                            let _ = std::fs::remove_dir_all(&rdb_dir);
                            if let Ok(rdb) =
                                crate::rocksdb_table::RocksDbTable::open(schema.clone(), &rdb_dir)
                            {
                                let rdb = Arc::new(rdb);
                                engine.engine_tables.insert(
                                    schema.id,
                                    crate::table_handle::TableHandle::RocksDb(Arc::clone(&rdb)),
                                );
                            }
                        }
                        #[cfg(feature = "lsm")]
                        StorageType::LsmRowstore => {
                            let data_dir = engine
                                .data_dir
                                .as_deref()
                                .unwrap_or_else(|| std::path::Path::new("."));
                            let lsm_dir = data_dir.join(format!("lsm_table_{}", schema.id.0));
                            let _ = std::fs::remove_dir_all(&lsm_dir);
                            if let Ok(lsm_engine) = crate::lsm::engine::LsmEngine::open(
                                &lsm_dir,
                                crate::lsm::engine::LsmConfig::default(),
                            ) {
                                if let Some(crypto) = engine.ext.block_crypto.read().clone() {
                                    lsm_engine.set_block_crypto(crypto);
                                }
                                let lsm = Arc::new(crate::lsm_table::LsmTable::new(
                                    schema.clone(),
                                    Arc::new(lsm_engine),
                                ));
                                engine.engine_tables.insert(
                                    schema.id,
                                    crate::table_handle::TableHandle::Lsm(Arc::clone(&lsm)),
                                );
                            }
                        }
                        #[cfg(feature = "columnstore")]
                        StorageType::Columnstore => {
                            let cs = Arc::new(crate::columnstore::ColumnStoreTable::new(schema.clone()));
                            engine.engine_tables.insert(
                                schema.id,
                                crate::table_handle::TableHandle::Columnstore(cs),
                            );
                        }
                        _ => {
                            // Fallback: create as in-memory Rowstore
                            let table = Arc::new(MemTable::new(schema.clone()));
                            engine.engine_tables.insert(
                                schema.id,
                                crate::table_handle::TableHandle::Rowstore(table),
                            );
                        }
                    }
                    engine.catalog.write().add_table(schema.clone());
                }
                WalRecord::DropTable { table_name } => {
                    let mut catalog = engine.catalog.write();
                    if let Some(schema) = catalog.find_table(table_name) {
                        let tid = schema.id;
                        engine.engine_tables.remove(&tid);
                    }
                    catalog.drop_table(table_name);
                }
                WalRecord::Insert {
                    txn_id,
                    table_id,
                    row,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        match handle.insert(row, *txn_id) {
                            Ok(pk) => {
                                recovered_write_sets
                                    .entry(*txn_id)
                                    .or_default()
                                    .push(TxnWriteOp {
                                        table_id: *table_id,
                                        pk,
                                    });
                            }
                            Err(e) => {
                                tracing::debug!("WAL recovery: Insert idempotent skip (txn={:?}, table={:?}): {}", txn_id, table_id, e);
                            }
                        }
                    } else {
                        tracing::warn!(
                            "WAL recovery: table {:?} not found for Insert(txn={:?})",
                            table_id,
                            txn_id
                        );
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
                        match handle.update(pk, new_row, *txn_id) {
                            Ok(()) => {
                                recovered_write_sets
                                    .entry(*txn_id)
                                    .or_default()
                                    .push(TxnWriteOp {
                                        table_id: *table_id,
                                        pk: pk.clone(),
                                    });
                            }
                            Err(e) => {
                                tracing::debug!("WAL recovery: Update idempotent skip (txn={:?}, table={:?}): {}", txn_id, table_id, e);
                            }
                        }
                    } else {
                        tracing::warn!(
                            "WAL recovery: table {:?} not found for Update(txn={:?})",
                            table_id,
                            txn_id
                        );
                    }
                }
                WalRecord::DdlBackfillUpdate {
                    table_id,
                    pk,
                    new_row,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        if let Some(memtable) = handle.as_rowstore() {
                            if let Some(chain) = memtable.data.get(pk) {
                                chain.replace_latest(new_row.clone());
                            }
                        }
                    }
                }
                WalRecord::Delete {
                    txn_id,
                    table_id,
                    pk,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        match handle.delete(pk, *txn_id) {
                            Ok(()) => {
                                recovered_write_sets
                                    .entry(*txn_id)
                                    .or_default()
                                    .push(TxnWriteOp {
                                        table_id: *table_id,
                                        pk: pk.clone(),
                                    });
                            }
                            Err(e) => {
                                tracing::debug!("WAL recovery: Delete idempotent skip (txn={:?}, table={:?}): {}", txn_id, table_id, e);
                            }
                        }
                    } else {
                        tracing::warn!(
                            "WAL recovery: table {:?} not found for Delete(txn={:?})",
                            table_id,
                            txn_id
                        );
                    }
                }
                WalRecord::CommitTxn { txn_id, commit_ts }
                | WalRecord::CommitTxnLocal { txn_id, commit_ts }
                | WalRecord::CommitTxnGlobal { txn_id, commit_ts } => {
                    if commit_ts.0 > max_commit_ts {
                        max_commit_ts = commit_ts.0;
                    }
                    if txn_id.0 > max_txn_id {
                        max_txn_id = txn_id.0;
                    }
                    prepared_txn_ids.remove(txn_id);
                    prepared_gid_mappings.retain(|_, tid| tid != txn_id);
                    let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                    if let Err(e) =
                        engine.apply_commit_to_write_set(*txn_id, *commit_ts, &write_set)
                    {
                        tracing::warn!(
                            "WAL recovery: commit apply failed (txn={:?}, ts={:?}): {}",
                            txn_id,
                            commit_ts,
                            e
                        );
                    }
                    committed_write_sets.insert(*txn_id, (*commit_ts, write_set));
                }
                WalRecord::AbortTxn { txn_id }
                | WalRecord::AbortTxnLocal { txn_id }
                | WalRecord::AbortTxnGlobal { txn_id } => {
                    prepared_txn_ids.remove(txn_id);
                    prepared_gid_mappings.retain(|_, tid| tid != txn_id);
                    if let Some((_commit_ts, ws)) = committed_write_sets.remove(txn_id) {
                        // Compensating abort: undo a previously-applied commit.
                        // This happens when commit_keys failed after the WAL
                        // commit record was already durable.
                        engine.apply_abort_to_write_set(*txn_id, &ws);
                        tracing::info!("WAL recovery: undid phantom commit for txn {:?}", txn_id);
                    } else {
                        let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                        engine.apply_abort_to_write_set(*txn_id, &write_set);
                    }
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
                WalRecord::CreateMaterializedView {
                    name,
                    query_sql,
                    backing_table_id,
                } => {
                    let mut catalog = engine.catalog.write();
                    if catalog.find_materialized_view(name).is_none() {
                        catalog.add_materialized_view(falcon_common::schema::MaterializedViewDef {
                            name: name.clone(),
                            query_sql: query_sql.clone(),
                            backing_table_id: *backing_table_id,
                        });
                    }
                }
                WalRecord::DropMaterializedView { name } => {
                    engine.catalog.write().drop_materialized_view(name);
                }
                WalRecord::CreateFunction { def } => {
                    let _ = engine.catalog.write().create_function(def.clone());
                }
                WalRecord::DropFunction { name } => {
                    let _ = engine.catalog.write().drop_function(name);
                }
                WalRecord::CreateTrigger { def } => {
                    let _ = engine.catalog.write().create_trigger(def.clone());
                }
                WalRecord::DropTrigger { table_name, trigger_name } => {
                    let _ = engine.catalog.write().drop_trigger(table_name, trigger_name);
                }
                WalRecord::AlterTable { table_name, op } => {
                    use crate::wal::AlterTableOp;
                    match op {
                        AlterTableOp::AddColumn { column } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let new_id =
                                    falcon_common::types::ColumnId(schema.columns.len() as u32);
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
                                    let remap = |col: usize| -> Option<usize> {
                                        if col == idx {
                                            None
                                        } else if col > idx {
                                            Some(col - 1)
                                        } else {
                                            Some(col)
                                        }
                                    };
                                    schema.primary_key_columns = schema
                                        .primary_key_columns
                                        .iter()
                                        .filter_map(|&pk| remap(pk))
                                        .collect();
                                    schema.unique_constraints = schema
                                        .unique_constraints
                                        .drain(..)
                                        .filter_map(|grp| {
                                            let r: Vec<usize> =
                                                grp.iter().filter_map(|&c| remap(c)).collect();
                                            if r.is_empty() {
                                                None
                                            } else {
                                                Some(r)
                                            }
                                        })
                                        .collect();
                                    schema.foreign_keys = schema
                                        .foreign_keys
                                        .drain(..)
                                        .filter_map(|mut fk| {
                                            let r: Vec<usize> = fk
                                                .columns
                                                .iter()
                                                .filter_map(|&c| remap(c))
                                                .collect();
                                            if r.is_empty() {
                                                return None;
                                            }
                                            fk.columns = r;
                                            Some(fk)
                                        })
                                        .collect();
                                    schema.shard_key =
                                        schema.shard_key.iter().filter_map(|&c| remap(c)).collect();
                                    schema.next_serial_values = schema
                                        .next_serial_values
                                        .drain()
                                        .filter_map(|(k, v)| remap(k).map(|nk| (nk, v)))
                                        .collect();
                                    schema.dynamic_defaults = schema
                                        .dynamic_defaults
                                        .drain()
                                        .filter_map(|(k, v)| remap(k).map(|nk| (nk, v)))
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
                        AlterTableOp::ChangeColumnType {
                            column_name,
                            new_type,
                        } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = column_name.to_lowercase();
                                if let Some(col) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|c| c.name.to_lowercase() == lower)
                                {
                                    col.data_type = new_type.clone();
                                }
                            }
                        }
                        AlterTableOp::SetNotNull { column_name } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = column_name.to_lowercase();
                                if let Some(col) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|c| c.name.to_lowercase() == lower)
                                {
                                    col.nullable = false;
                                }
                            }
                        }
                        AlterTableOp::DropNotNull { column_name } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = column_name.to_lowercase();
                                if let Some(col) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|c| c.name.to_lowercase() == lower)
                                {
                                    col.nullable = true;
                                }
                            }
                        }
                        AlterTableOp::SetDefault {
                            column_name,
                            default_value,
                        } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = column_name.to_lowercase();
                                if let Some(col) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|c| c.name.to_lowercase() == lower)
                                {
                                    col.default_value = Some(default_value.clone());
                                }
                            }
                        }
                        AlterTableOp::DropDefault { column_name } => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                let lower = column_name.to_lowercase();
                                if let Some(col) = schema
                                    .columns
                                    .iter_mut()
                                    .find(|c| c.name.to_lowercase() == lower)
                                {
                                    col.default_value = None;
                                }
                            }
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
                        engine.engine_tables.insert(
                            table_id,
                            crate::table_handle::TableHandle::Rowstore(new_table),
                        );
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
                        if let Some(handle) = engine.engine_tables.get(&meta.table_id) {
                            if let Some(memtable) = handle.as_rowstore() {
                                let mut indexes = memtable.secondary_indexes.write();
                                indexes.retain(|idx| idx.column_idx != *column_idx);
                                memtable
                                    .has_secondary_idx
                                    .store(!indexes.is_empty(), AtomicOrdering::Release);
                            }
                        }
                    }
                }
                WalRecord::CreateSchema { name, owner } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.create_schema(name, owner) {
                        tracing::debug!("WAL recovery: create_schema idempotent skip: {e}");
                    }
                }
                WalRecord::DropSchema { name } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.drop_schema(name) {
                        tracing::debug!("WAL recovery: drop_schema idempotent skip: {e}");
                    }
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
                    if let Err(e) = catalog.create_role(
                        name,
                        *can_login,
                        *is_superuser,
                        *can_create_db,
                        *can_create_role,
                        password_hash.clone(),
                    ) {
                        tracing::debug!("WAL recovery: create_role idempotent skip: {e}");
                    }
                }
                WalRecord::DropRole { name } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.drop_role(name) {
                        tracing::debug!("WAL recovery: drop_role idempotent skip: {e}");
                    }
                }
                WalRecord::AlterRole { name, opts } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.alter_role(
                        name,
                        opts.password.clone(),
                        opts.can_login,
                        opts.is_superuser,
                        opts.can_create_db,
                        opts.can_create_role,
                    ) {
                        tracing::debug!("WAL recovery: alter_role idempotent skip: {e}");
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
                    if let Err(e) = catalog.grant_privilege(
                        grantee,
                        privilege,
                        object_type,
                        object_name,
                        grantor,
                    ) {
                        tracing::debug!("WAL recovery: grant_privilege idempotent skip: {e}");
                    }
                }
                WalRecord::RevokePrivilege {
                    grantee,
                    privilege,
                    object_type,
                    object_name,
                } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) =
                        catalog.revoke_privilege(grantee, privilege, object_type, object_name)
                    {
                        tracing::debug!("WAL recovery: revoke_privilege idempotent skip: {e}");
                    }
                }
                WalRecord::GrantRole { member, group } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.grant_role_membership(member, group) {
                        tracing::debug!("WAL recovery: grant_role idempotent skip: {e}");
                    }
                }
                WalRecord::RevokeRole { member, group } => {
                    let mut catalog = engine.catalog.write();
                    if let Err(e) = catalog.revoke_role_membership(member, group) {
                        tracing::debug!("WAL recovery: revoke_role idempotent skip: {e}");
                    }
                }
            }
        }

        // Separate prepared (in-doubt) txns from truly uncommitted ones.
        // Prepared txns had PrepareTxn WAL record but no Commit/Abort — they're
        // in-doubt and must wait for coordinator resolution. Only abort the rest.
        let mut indoubt_count = 0usize;
        let mut aborted_count = 0usize;
        let drain: Vec<(TxnId, Vec<TxnWriteOp>)> = recovered_write_sets.drain().collect();
        for (txn_id, write_set) in drain {
            if prepared_txn_ids.contains(&txn_id) {
                // In-doubt: keep writes visible but uncommitted for resolver
                indoubt_count += 1;
                tracing::warn!(
                    txn_id = txn_id.0,
                    "WAL recovery: in-doubt prepared txn preserved"
                );
            } else {
                engine.apply_abort_to_write_set(txn_id, &write_set);
                aborted_count += 1;
            }
        }
        if aborted_count > 0 {
            tracing::info!(
                "WAL recovery: aborted {} uncommitted transaction(s)",
                aborted_count
            );
        }
        if indoubt_count > 0 {
            tracing::warn!(
                "WAL recovery: {} in-doubt prepared transaction(s) await resolution",
                indoubt_count
            );
        }

        // Store recovered prepared txn IDs for the in-doubt resolver
        {
            let mut guard = engine.recovered_prepared_txns.write();
            *guard = prepared_txn_ids.into_iter().collect();
        }
        // Store 2PC coordinator mappings for WalReplicationService DashMap rebuild
        if !prepared_2pc_mappings.is_empty() {
            let mut guard = engine.recovered_prepared_2pc.write();
            *guard = prepared_2pc_mappings;
        }
        if !prepared_gid_mappings.is_empty() {
            *engine.recovered_prepared_gids.write() = prepared_gid_mappings;
        }

        // Rebuild secondary indexes and GIN indexes from committed rows only.
        // Limitation: rows written by in-doubt (prepared but unresolved) transactions
        // are NOT indexed until the transaction is resolved via COMMIT/ROLLBACK PREPARED
        // or the in-doubt resolver. Queries using secondary index scans may miss these
        // rows; primary key lookups are unaffected.
        for entry in &engine.engine_tables {
            if let Some(memtable) = entry.value().as_rowstore() {
                memtable.ensure_gin_indexes();
                memtable.rebuild_secondary_indexes();
            }
        }

        engine
            .recovered_max_ts
            .store(max_commit_ts, AtomicOrdering::Relaxed);
        engine
            .recovered_max_txn_id
            .store(max_txn_id, AtomicOrdering::Relaxed);
        tracing::info!(
            "WAL recovery complete: {} records replayed, max_commit_ts={}, max_txn_id={}, in_doubt={}",
            records.len(), max_commit_ts, max_txn_id, indoubt_count
        );
        Ok(engine)
    }

    /// PITR: Recover from WAL up to a specified target (LSN, time, or XID).
    /// Returns (records_replayed, final_lsn) on success.
    pub fn recover_to_target(
        wal_dir: &Path,
        target: crate::pitr::RecoveryTarget,
    ) -> Result<(Self, u64, u64), StorageError> {
        use crate::wal::WalReader;

        let engine = Self::new(Some(wal_dir))?;

        let reader = WalReader::new(wal_dir);
        let records = reader.read_all()?;

        let mut recovery = crate::pitr::RecoveryExecutor::new(target);
        recovery.begin();

        let mut recovered_write_sets: HashMap<TxnId, Vec<TxnWriteOp>> = HashMap::new();
        let mut committed_write_sets: HashMap<TxnId, (Timestamp, Vec<TxnWriteOp>)> = HashMap::new();
        let mut max_commit_ts: u64 = 0;
        let mut max_txn_id: u64 = 0;
        let mut lsn_counter: u64 = 0;

        for record in &records {
            lsn_counter += 1;
            let current_lsn = crate::pitr::Lsn(lsn_counter);

            // Check if we should stop replay (only on commit boundaries for consistency)
            if let WalRecord::CommitTxn { commit_ts, txn_id }
            | WalRecord::CommitTxnLocal { commit_ts, txn_id }
            | WalRecord::CommitTxnGlobal { commit_ts, txn_id } = record
            {
                if recovery.target_reached(current_lsn, commit_ts.0, txn_id.0) {
                    tracing::info!(
                        "PITR: target reached at LSN {} (commit_ts={}, txn_id={})",
                        current_lsn,
                        commit_ts.0,
                        txn_id.0
                    );
                    // Still process this commit record, then stop
                    if commit_ts.0 > max_commit_ts {
                        max_commit_ts = commit_ts.0;
                    }
                    if txn_id.0 > max_txn_id {
                        max_txn_id = txn_id.0;
                    }
                    let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                    let _ = engine.apply_commit_to_write_set(*txn_id, *commit_ts, &write_set);
                    recovery.record_replayed(current_lsn);
                    break;
                }
            }

            // Dispatch record — same logic as recover()
            match record {
                WalRecord::BeginTxn { .. }
                | WalRecord::PrepareTxn { .. }
                | WalRecord::PrepareTxnNamed { .. }
                | WalRecord::PrepareTxn2pc { .. }
                | WalRecord::Checkpoint { .. }
                | WalRecord::CoordinatorPrepare { .. }
                | WalRecord::CoordinatorCommit { .. }
                | WalRecord::CoordinatorAbort { .. } => {}
                WalRecord::CreateDatabase { name, owner } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.create_database(name, owner);
                }
                WalRecord::DropDatabase { name } => {
                    let mut catalog = engine.catalog.write();
                    let _ = catalog.drop_database(name);
                }
                WalRecord::CreateTable { schema } => {
                    if !engine.engine_tables.contains_key(&schema.id) {
                        let table = Arc::new(MemTable::new(schema.clone()));
                        engine
                            .engine_tables
                            .insert(schema.id, crate::table_handle::TableHandle::Rowstore(table));
                    }
                    engine.catalog.write().add_table(schema.clone());
                }
                WalRecord::DropTable { table_name } => {
                    let mut catalog = engine.catalog.write();
                    if let Some(schema) = catalog.find_table(table_name) {
                        let tid = schema.id;
                        engine.engine_tables.remove(&tid);
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
                WalRecord::DdlBackfillUpdate {
                    table_id,
                    pk,
                    new_row,
                } => {
                    if let Some(handle) = engine.engine_tables.get(table_id) {
                        if let Some(memtable) = handle.as_rowstore() {
                            if let Some(chain) = memtable.data.get(pk) {
                                chain.replace_latest(new_row.clone());
                            }
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
                    if commit_ts.0 > max_commit_ts {
                        max_commit_ts = commit_ts.0;
                    }
                    if txn_id.0 > max_txn_id {
                        max_txn_id = txn_id.0;
                    }
                    let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                    let _ = engine.apply_commit_to_write_set(*txn_id, *commit_ts, &write_set);
                    committed_write_sets.insert(*txn_id, (*commit_ts, write_set));
                }
                WalRecord::AbortTxn { txn_id }
                | WalRecord::AbortTxnLocal { txn_id }
                | WalRecord::AbortTxnGlobal { txn_id } => {
                    if let Some((_commit_ts, ws)) = committed_write_sets.remove(txn_id) {
                        engine.apply_abort_to_write_set(*txn_id, &ws);
                        tracing::info!("PITR recovery: undid phantom commit for txn {:?}", txn_id);
                    } else {
                        let write_set = recovered_write_sets.remove(txn_id).unwrap_or_default();
                        engine.apply_abort_to_write_set(*txn_id, &write_set);
                    }
                }
                WalRecord::AlterTable { table_name, op } => {
                    use crate::wal::AlterTableOp;
                    match op {
                        AlterTableOp::RenameTable { new_name } => {
                            engine.catalog.write().rename_table(table_name, new_name);
                        }
                        _ => {
                            let mut catalog = engine.catalog.write();
                            if let Some(schema) = catalog.find_table_mut(table_name) {
                                match op {
                                    AlterTableOp::AddColumn { column } => {
                                        let new_id = falcon_common::types::ColumnId(
                                            schema.columns.len() as u32,
                                        );
                                        let mut new_col = column.clone();
                                        new_col.id = new_id;
                                        schema.columns.push(new_col);
                                    }
                                    AlterTableOp::DropColumn { column_name } => {
                                        let lower = column_name.to_lowercase();
                                        if let Some(idx) = schema
                                            .columns
                                            .iter()
                                            .position(|c| c.name.to_lowercase() == lower)
                                        {
                                            schema.columns.remove(idx);
                                        }
                                    }
                                    AlterTableOp::RenameColumn { old_name, new_name } => {
                                        let lower = old_name.to_lowercase();
                                        if let Some(col) = schema
                                            .columns
                                            .iter_mut()
                                            .find(|c| c.name.to_lowercase() == lower)
                                        {
                                            col.name = new_name.clone();
                                        }
                                    }
                                    AlterTableOp::ChangeColumnType {
                                        column_name,
                                        new_type,
                                    } => {
                                        let lower = column_name.to_lowercase();
                                        if let Some(col) = schema
                                            .columns
                                            .iter_mut()
                                            .find(|c| c.name.to_lowercase() == lower)
                                        {
                                            col.data_type = new_type.clone();
                                        }
                                    }
                                    AlterTableOp::SetNotNull { column_name } => {
                                        let lower = column_name.to_lowercase();
                                        if let Some(col) = schema
                                            .columns
                                            .iter_mut()
                                            .find(|c| c.name.to_lowercase() == lower)
                                        {
                                            col.nullable = false;
                                        }
                                    }
                                    AlterTableOp::DropNotNull { column_name } => {
                                        let lower = column_name.to_lowercase();
                                        if let Some(col) = schema
                                            .columns
                                            .iter_mut()
                                            .find(|c| c.name.to_lowercase() == lower)
                                        {
                                            col.nullable = true;
                                        }
                                    }
                                    AlterTableOp::SetDefault {
                                        column_name,
                                        default_value,
                                    } => {
                                        let lower = column_name.to_lowercase();
                                        if let Some(col) = schema
                                            .columns
                                            .iter_mut()
                                            .find(|c| c.name.to_lowercase() == lower)
                                        {
                                            col.default_value = Some(default_value.clone());
                                        }
                                    }
                                    AlterTableOp::DropDefault { column_name } => {
                                        let lower = column_name.to_lowercase();
                                        if let Some(col) = schema
                                            .columns
                                            .iter_mut()
                                            .find(|c| c.name.to_lowercase() == lower)
                                        {
                                            col.default_value = None;
                                        }
                                    }
                                    AlterTableOp::RenameTable { .. } => unreachable!(),
                                }
                            }
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
                    if let Some(ts) = catalog.find_table(table_name) {
                        let tid = ts.id;
                        let schema = ts.clone();
                        drop(catalog);
                        engine.engine_tables.insert(
                            tid,
                            crate::table_handle::TableHandle::Rowstore(Arc::new(MemTable::new(
                                schema,
                            ))),
                        );
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
                        if let Some(ts) = engine.catalog.read().find_table(table_name) {
                            engine.index_registry.insert(
                                index_name.to_lowercase(),
                                IndexMeta {
                                    table_id: ts.id,
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
                    column_idx,
                    ..
                } => {
                    if let Some((_, meta)) = engine.index_registry.remove(index_name.as_str()) {
                        if let Some(handle) = engine.engine_tables.get(&meta.table_id) {
                            if let Some(memtable) = handle.as_rowstore() {
                                let mut indexes = memtable.secondary_indexes.write();
                                indexes.retain(|idx| idx.column_idx != *column_idx);
                                memtable
                                    .has_secondary_idx
                                    .store(!indexes.is_empty(), AtomicOrdering::Release);
                            }
                        }
                    }
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
                WalRecord::CreateMaterializedView {
                    name,
                    query_sql,
                    backing_table_id,
                } => {
                    let mut catalog = engine.catalog.write();
                    if catalog.find_materialized_view(name).is_none() {
                        catalog.add_materialized_view(falcon_common::schema::MaterializedViewDef {
                            name: name.clone(),
                            query_sql: query_sql.clone(),
                            backing_table_id: *backing_table_id,
                        });
                    }
                }
                WalRecord::DropMaterializedView { name } => {
                    engine.catalog.write().drop_materialized_view(name);
                }
                WalRecord::CreateFunction { def } => {
                    let _ = engine.catalog.write().create_function(def.clone());
                }
                WalRecord::DropFunction { name } => {
                    let _ = engine.catalog.write().drop_function(name);
                }
                WalRecord::CreateTrigger { def } => {
                    let _ = engine.catalog.write().create_trigger(def.clone());
                }
                WalRecord::DropTrigger { table_name, trigger_name } => {
                    let _ = engine.catalog.write().drop_trigger(table_name, trigger_name);
                }
                WalRecord::CreateSchema { name, owner } => {
                    let _ = engine.catalog.write().create_schema(name, owner);
                }
                WalRecord::DropSchema { name } => {
                    let _ = engine.catalog.write().drop_schema(name);
                }
                WalRecord::CreateRole {
                    name,
                    can_login,
                    is_superuser,
                    can_create_db,
                    can_create_role,
                    password_hash,
                } => {
                    let _ = engine.catalog.write().create_role(
                        name,
                        *can_login,
                        *is_superuser,
                        *can_create_db,
                        *can_create_role,
                        password_hash.clone(),
                    );
                }
                WalRecord::DropRole { name } => {
                    let _ = engine.catalog.write().drop_role(name);
                }
                WalRecord::AlterRole { name, opts } => {
                    let _ = engine.catalog.write().alter_role(
                        name,
                        opts.password.clone(),
                        opts.can_login,
                        opts.is_superuser,
                        opts.can_create_db,
                        opts.can_create_role,
                    );
                }
                WalRecord::GrantPrivilege {
                    grantee,
                    privilege,
                    object_type,
                    object_name,
                    grantor,
                } => {
                    let _ = engine.catalog.write().grant_privilege(
                        grantee,
                        privilege,
                        object_type,
                        object_name,
                        grantor,
                    );
                }
                WalRecord::RevokePrivilege {
                    grantee,
                    privilege,
                    object_type,
                    object_name,
                } => {
                    let _ = engine.catalog.write().revoke_privilege(
                        grantee,
                        privilege,
                        object_type,
                        object_name,
                    );
                }
                WalRecord::GrantRole { member, group } => {
                    let _ = engine.catalog.write().grant_role_membership(member, group);
                }
                WalRecord::RevokeRole { member, group } => {
                    let _ = engine.catalog.write().revoke_role_membership(member, group);
                }
            }
            recovery.record_replayed(current_lsn);
        }

        // Abort uncommitted transactions
        for (txn_id, write_set) in recovered_write_sets.drain() {
            engine.apply_abort_to_write_set(txn_id, &write_set);
        }

        // Rebuild secondary indexes and GIN indexes
        for entry in &engine.engine_tables {
            if let Some(memtable) = entry.value().as_rowstore() {
                memtable.ensure_gin_indexes();
                memtable.rebuild_secondary_indexes();
            }
        }

        engine
            .recovered_max_ts
            .store(max_commit_ts, AtomicOrdering::Relaxed);
        engine
            .recovered_max_txn_id
            .store(max_txn_id, AtomicOrdering::Relaxed);

        recovery.complete();
        let final_lsn = recovery.current_lsn.0;
        let records_replayed = recovery.records_replayed;

        tracing::info!(
            "PITR recovery complete: {} records replayed, final_lsn={}, max_commit_ts={}",
            records_replayed,
            final_lsn,
            max_commit_ts
        );
        Ok((engine, records_replayed, final_lsn))
    }

    /// PITR: Recover from archived WAL segments.
    ///
    /// 1. Builds a recovery plan from the archiver (selects base backup + WAL segments).
    /// 2. Stages archived segments into a temporary directory.
    /// 3. Delegates to `recover_to_target` for actual replay.
    ///
    /// Returns `(engine, records_replayed, final_lsn)` on success.
    pub fn recover_from_archive(
        archiver: &crate::pitr::WalArchiver,
        target: crate::pitr::RecoveryTarget,
        staging_dir: &Path,
    ) -> Result<(Self, u64, u64), StorageError> {
        let plan = archiver
            .plan_recovery(&target)
            .ok_or_else(|| StorageError::Other(
                "no suitable base backup found for the requested recovery target".into(),
            ))?;

        tracing::info!(
            "PITR: recovery plan — base_backup='{}' (LSN {}..{}), {} WAL segments, target={}",
            plan.base_backup.label,
            plan.base_backup.start_lsn,
            plan.base_backup.end_lsn,
            plan.segments.len(),
            plan.target,
        );

        let staged = archiver.stage_segments(&plan, staging_dir).map_err(|e| {
            StorageError::Other(format!("failed to stage WAL segments: {e}"))
        })?;
        tracing::info!("PITR: {staged} segments staged to {:?}", staging_dir);

        Self::recover_to_target(staging_dir, plan.target)
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
impl Drop for StorageEngine {
    fn drop(&mut self) {
        self.shutdown_wal_flush();
    }
}

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
        DataType::TsVector => "tsvector".into(),
        DataType::TsQuery => "tsquery".into(),
    }
}
