//! Admission control and global backpressure for FalconDB.
//!
//! Provides a unified resource gate (permit system) for:
//! - Connection permits
//! - Query permits (concurrent inflight queries)
//! - Write permits (per-shard)
//! - WAL flush queue permits
//! - Replication apply permits
//!
//! When any threshold is exceeded, returns `FalconError::Transient` with
//! a `retry_after_ms` hint for the caller.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use falcon_common::error::{FalconError, FalconResult};

/// Configuration for admission control thresholds.
/// All values can be updated at runtime via `AdmissionControl::update_config`.
#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    /// Maximum concurrent inflight queries (across all sessions).
    pub max_inflight_queries: usize,
    /// Maximum concurrent inflight writes per shard.
    pub max_inflight_writes_per_shard: usize,
    /// Maximum WAL flush queue size in bytes before backpressure.
    pub max_wal_flush_queue_bytes: u64,
    /// Maximum replication apply lag in bytes before backpressure.
    pub max_replication_apply_lag_bytes: u64,
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// Global memory budget in bytes (soft limit = 80%, hard = 95%).
    pub memory_budget_bytes: u64,
    /// Soft memory limit ratio (triggers backpressure).
    pub memory_pressure_ratio: f64,
    /// Hard memory limit ratio (rejects all writes).
    pub memory_hard_limit_ratio: f64,
    /// Default retry delay for backpressure rejections (ms).
    pub default_retry_after_ms: u64,
    /// Maximum concurrent inflight DDL operations.
    pub max_inflight_ddl: usize,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_inflight_queries: 1000,
            max_inflight_writes_per_shard: 500,
            max_wal_flush_queue_bytes: 64 * 1024 * 1024, // 64MB
            max_replication_apply_lag_bytes: 256 * 1024 * 1024, // 256MB
            max_connections: 10_000,
            memory_budget_bytes: 4 * 1024 * 1024 * 1024, // 4GB
            memory_pressure_ratio: 0.80,
            memory_hard_limit_ratio: 0.95,
            default_retry_after_ms: 50,
            max_inflight_ddl: 4,
        }
    }
}

/// Metrics exposed by the admission control system.
#[derive(Debug, Clone, Default)]
pub struct AdmissionMetrics {
    /// Total requests rejected due to backpressure.
    pub total_rejected: u64,
    /// Total requests rejected due to connection limit.
    pub connection_rejected: u64,
    /// Total requests rejected due to query limit.
    pub query_rejected: u64,
    /// Total requests rejected due to write limit.
    pub write_rejected: u64,
    /// Total requests rejected due to WAL backlog.
    pub wal_rejected: u64,
    /// Total requests rejected due to replication lag.
    pub replication_rejected: u64,
    /// Total requests rejected due to memory pressure.
    pub memory_rejected: u64,
    /// Total requests rejected due to DDL limit.
    pub ddl_rejected: u64,
    /// Current inflight queries.
    pub inflight_queries: usize,
    /// Current inflight writes (sum across all shards).
    pub inflight_writes: usize,
    /// Current active connections.
    pub active_connections: usize,
}

/// RAII guard that releases a query permit when dropped.
#[derive(Debug)]
pub struct QueryPermit {
    control: Arc<AdmissionControl>,
}

impl Drop for QueryPermit {
    fn drop(&mut self) {
        self.control
            .inflight_queries
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII guard that releases a write permit when dropped.
#[derive(Debug)]
pub struct WritePermit {
    control: Arc<AdmissionControl>,
    shard_idx: usize,
}

impl Drop for WritePermit {
    fn drop(&mut self) {
        if let Some(counter) = self.control.inflight_writes_per_shard.get(self.shard_idx) {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
        self.control
            .total_inflight_writes
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII guard that releases a connection permit when dropped.
#[derive(Debug)]
pub struct ConnectionPermit {
    control: Arc<AdmissionControl>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.control
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Global admission control gate.
///
/// Thread-safe; clone the `Arc<AdmissionControl>` to share across sessions.
pub struct AdmissionControl {
    config: parking_lot::RwLock<AdmissionConfig>,

    // Counters
    inflight_queries: AtomicUsize,
    inflight_writes_per_shard: Vec<AtomicUsize>,
    total_inflight_writes: AtomicUsize,
    active_connections: AtomicUsize,

    // Metrics counters
    total_rejected: AtomicU64,
    connection_rejected: AtomicU64,
    query_rejected: AtomicU64,
    write_rejected: AtomicU64,
    wal_rejected: AtomicU64,
    replication_rejected: AtomicU64,
    memory_rejected: AtomicU64,
    /// v1.4: Inflight DDL operations counter.
    inflight_ddl: AtomicUsize,
    ddl_rejected: AtomicU64,
}

impl std::fmt::Debug for AdmissionControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionControl")
            .field(
                "inflight_queries",
                &self.inflight_queries.load(Ordering::Relaxed),
            )
            .field(
                "inflight_writes",
                &self.total_inflight_writes.load(Ordering::Relaxed),
            )
            .field(
                "active_connections",
                &self.active_connections.load(Ordering::Relaxed),
            )
            .field(
                "total_rejected",
                &self.total_rejected.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl AdmissionControl {
    /// Create a new admission control gate with the given config and shard count.
    pub fn new(config: AdmissionConfig, shard_count: usize) -> Arc<Self> {
        let shard_count = shard_count.max(1);
        Arc::new(Self {
            config: parking_lot::RwLock::new(config),
            inflight_queries: AtomicUsize::new(0),
            inflight_writes_per_shard: (0..shard_count).map(|_| AtomicUsize::new(0)).collect(),
            total_inflight_writes: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            total_rejected: AtomicU64::new(0),
            connection_rejected: AtomicU64::new(0),
            query_rejected: AtomicU64::new(0),
            write_rejected: AtomicU64::new(0),
            wal_rejected: AtomicU64::new(0),
            replication_rejected: AtomicU64::new(0),
            memory_rejected: AtomicU64::new(0),
            inflight_ddl: AtomicUsize::new(0),
            ddl_rejected: AtomicU64::new(0),
        })
    }

    /// Create with default config.
    pub fn new_default(shard_count: usize) -> Arc<Self> {
        Self::new(AdmissionConfig::default(), shard_count)
    }

    /// Update the admission control configuration at runtime.
    pub fn update_config(&self, config: AdmissionConfig) {
        *self.config.write() = config;
    }

    /// Acquire a connection permit. Returns RAII guard or `Transient` error.
    pub fn acquire_connection(self: &Arc<Self>) -> FalconResult<ConnectionPermit> {
        let cfg = self.config.read();
        let current = self.active_connections.fetch_add(1, Ordering::Relaxed);
        if current >= cfg.max_connections {
            self.active_connections.fetch_sub(1, Ordering::Relaxed);
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.connection_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "connection limit reached ({}/{})",
                    current, cfg.max_connections
                ),
                cfg.default_retry_after_ms,
            ));
        }
        Ok(ConnectionPermit {
            control: Arc::clone(self),
        })
    }

    /// Acquire a query permit. Returns RAII guard or `Transient` error.
    pub fn acquire_query(self: &Arc<Self>) -> FalconResult<QueryPermit> {
        let cfg = self.config.read();
        let current = self.inflight_queries.fetch_add(1, Ordering::Relaxed);
        if current >= cfg.max_inflight_queries {
            self.inflight_queries.fetch_sub(1, Ordering::Relaxed);
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.query_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "query concurrency limit reached ({}/{})",
                    current, cfg.max_inflight_queries
                ),
                cfg.default_retry_after_ms,
            ));
        }
        Ok(QueryPermit {
            control: Arc::clone(self),
        })
    }

    /// Acquire a write permit for a specific shard. Returns RAII guard or `Transient` error.
    pub fn acquire_write(self: &Arc<Self>, shard_idx: usize) -> FalconResult<WritePermit> {
        let cfg = self.config.read();
        let shard_idx = shard_idx.min(self.inflight_writes_per_shard.len().saturating_sub(1));

        if let Some(counter) = self.inflight_writes_per_shard.get(shard_idx) {
            let current = counter.fetch_add(1, Ordering::Relaxed);
            if current >= cfg.max_inflight_writes_per_shard {
                counter.fetch_sub(1, Ordering::Relaxed);
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                self.write_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(FalconError::transient(
                    format!(
                        "write concurrency limit reached on shard {} ({}/{})",
                        shard_idx, current, cfg.max_inflight_writes_per_shard
                    ),
                    cfg.default_retry_after_ms,
                ));
            }
        }
        self.total_inflight_writes.fetch_add(1, Ordering::Relaxed);
        Ok(WritePermit {
            control: Arc::clone(self),
            shard_idx,
        })
    }

    /// Check WAL flush queue size. Returns `Transient` if backlog exceeds threshold.
    pub fn check_wal_backlog(&self, backlog_bytes: u64) -> FalconResult<()> {
        let cfg = self.config.read();
        if backlog_bytes > cfg.max_wal_flush_queue_bytes {
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.wal_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "WAL flush queue backlog too large ({} > {} bytes)",
                    backlog_bytes, cfg.max_wal_flush_queue_bytes
                ),
                200,
            ));
        }
        Ok(())
    }

    /// Check replication apply lag. Returns `Transient` if lag exceeds threshold.
    pub fn check_replication_lag(&self, lag_bytes: u64) -> FalconResult<()> {
        let cfg = self.config.read();
        if lag_bytes > cfg.max_replication_apply_lag_bytes {
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.replication_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "replication apply lag too large ({} > {} bytes)",
                    lag_bytes, cfg.max_replication_apply_lag_bytes
                ),
                500,
            ));
        }
        Ok(())
    }

    /// Check memory usage against budget.
    /// Returns `Transient` at soft limit (backpressure), `Transient` at hard limit (reject all).
    pub fn check_memory(&self, used_bytes: u64) -> FalconResult<()> {
        let cfg = self.config.read();
        let budget = cfg.memory_budget_bytes;
        let ratio = used_bytes as f64 / budget as f64;

        if ratio >= cfg.memory_hard_limit_ratio {
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.memory_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "memory hard limit reached ({:.1}% of {} bytes)",
                    ratio * 100.0,
                    budget
                ),
                500,
            ));
        }
        if ratio >= cfg.memory_pressure_ratio {
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.memory_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "memory pressure: {:.1}% of budget used ({} / {} bytes)",
                    ratio * 100.0,
                    used_bytes,
                    budget
                ),
                50,
            ));
        }
        Ok(())
    }

    /// Snapshot current metrics.
    pub fn metrics(&self) -> AdmissionMetrics {
        AdmissionMetrics {
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            connection_rejected: self.connection_rejected.load(Ordering::Relaxed),
            query_rejected: self.query_rejected.load(Ordering::Relaxed),
            write_rejected: self.write_rejected.load(Ordering::Relaxed),
            wal_rejected: self.wal_rejected.load(Ordering::Relaxed),
            replication_rejected: self.replication_rejected.load(Ordering::Relaxed),
            memory_rejected: self.memory_rejected.load(Ordering::Relaxed),
            ddl_rejected: self.ddl_rejected.load(Ordering::Relaxed),
            inflight_queries: self.inflight_queries.load(Ordering::Relaxed),
            inflight_writes: self.total_inflight_writes.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
        }
    }

    /// Current inflight query count.
    pub fn inflight_queries(&self) -> usize {
        self.inflight_queries.load(Ordering::Relaxed)
    }

    /// Current active connection count.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Current total inflight write count.
    pub fn inflight_writes(&self) -> usize {
        self.total_inflight_writes.load(Ordering::Relaxed)
    }

    /// Total requests rejected since startup.
    pub fn total_rejected(&self) -> u64 {
        self.total_rejected.load(Ordering::Relaxed)
    }

    /// Acquire a DDL permit. Returns RAII guard or `Transient` error.
    pub fn acquire_ddl(self: &Arc<Self>) -> FalconResult<DdlPermit> {
        let cfg = self.config.read();
        let current = self.inflight_ddl.fetch_add(1, Ordering::Relaxed);
        if current >= cfg.max_inflight_ddl {
            self.inflight_ddl.fetch_sub(1, Ordering::Relaxed);
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            self.ddl_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "DDL concurrency limit reached ({}/{})",
                    current, cfg.max_inflight_ddl
                ),
                cfg.default_retry_after_ms,
            ));
        }
        Ok(DdlPermit {
            control: Arc::clone(self),
        })
    }

    /// Current inflight DDL count.
    pub fn inflight_ddl(&self) -> usize {
        self.inflight_ddl.load(Ordering::Relaxed)
    }
}

/// Operation type for fine-grained admission control (v1.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Read-only query (SELECT, SHOW, EXPLAIN).
    Read,
    /// Write operation (INSERT, UPDATE, DELETE).
    Write,
    /// DDL operation (CREATE, ALTER, DROP, TRUNCATE).
    Ddl,
}

/// RAII guard that releases a DDL permit when dropped.
#[derive(Debug)]
pub struct DdlPermit {
    control: Arc<AdmissionControl>,
}

impl Drop for DdlPermit {
    fn drop(&mut self) {
        self.control.inflight_ddl.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Global memory budget tracker.
///
/// Tracks memory usage across subsystems and enforces soft/hard limits.
pub struct MemoryBudget {
    /// Total budget in bytes.
    budget_bytes: u64,
    /// Current usage in bytes.
    used_bytes: AtomicU64,
    /// Soft limit ratio (triggers backpressure).
    pressure_ratio: f64,
    /// Hard limit ratio (rejects all writes).
    hard_limit_ratio: f64,
    /// Rejection counter.
    rejections: AtomicU64,
}

impl MemoryBudget {
    pub fn new(budget_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            budget_bytes,
            used_bytes: AtomicU64::new(0),
            pressure_ratio: 0.80,
            hard_limit_ratio: 0.95,
            rejections: AtomicU64::new(0),
        })
    }

    /// Record memory allocation. Returns error if over budget.
    pub fn allocate(&self, bytes: u64) -> FalconResult<()> {
        let current = self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        let new_total = current + bytes;
        let ratio = new_total as f64 / self.budget_bytes as f64;

        if ratio >= self.hard_limit_ratio {
            self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "memory hard limit: {:.1}% of {} bytes used",
                    ratio * 100.0,
                    self.budget_bytes
                ),
                500,
            ));
        }
        if ratio >= self.pressure_ratio {
            // Allow but signal backpressure (don't roll back the allocation)
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(FalconError::transient(
                format!(
                    "memory pressure: {:.1}% of budget ({} / {} bytes)",
                    ratio * 100.0,
                    new_total,
                    self.budget_bytes
                ),
                50,
            ));
        }
        Ok(())
    }

    /// Release previously allocated memory.
    pub fn release(&self, bytes: u64) {
        self.used_bytes.fetch_sub(
            bytes.min(self.used_bytes.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    /// Current usage in bytes.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Budget limit in bytes.
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Usage ratio (0.0 to 1.0+).
    pub fn usage_ratio(&self) -> f64 {
        self.used_bytes.load(Ordering::Relaxed) as f64 / self.budget_bytes as f64
    }

    /// Total rejection count.
    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_control(max_queries: usize, max_writes: usize) -> Arc<AdmissionControl> {
        let mut cfg = AdmissionConfig::default();
        cfg.max_inflight_queries = max_queries;
        cfg.max_inflight_writes_per_shard = max_writes;
        cfg.max_connections = 10;
        AdmissionControl::new(cfg, 4)
    }

    #[test]
    fn test_query_permit_acquired_and_released() {
        let ctrl = make_control(10, 10);
        assert_eq!(ctrl.inflight_queries(), 0);
        {
            let _permit = ctrl.acquire_query().unwrap();
            assert_eq!(ctrl.inflight_queries(), 1);
        }
        assert_eq!(ctrl.inflight_queries(), 0);
    }

    #[test]
    fn test_query_limit_enforced() {
        let ctrl = make_control(2, 10);
        let _p1 = ctrl.acquire_query().unwrap();
        let _p2 = ctrl.acquire_query().unwrap();
        let result = ctrl.acquire_query();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_transient(), "over-limit should be Transient");
        assert_eq!(ctrl.total_rejected(), 1);
    }

    #[test]
    fn test_write_permit_acquired_and_released() {
        let ctrl = make_control(10, 10);
        assert_eq!(ctrl.inflight_writes(), 0);
        {
            let _permit = ctrl.acquire_write(0).unwrap();
            assert_eq!(ctrl.inflight_writes(), 1);
        }
        assert_eq!(ctrl.inflight_writes(), 0);
    }

    #[test]
    fn test_write_limit_per_shard_enforced() {
        let ctrl = make_control(10, 1);
        let _p1 = ctrl.acquire_write(0).unwrap();
        let result = ctrl.acquire_write(0);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transient());
        // Different shard should still work
        let _p2 = ctrl.acquire_write(1).unwrap();
    }

    #[test]
    fn test_connection_limit_enforced() {
        let ctrl = make_control(10, 10);
        let mut permits = Vec::new();
        for _ in 0..10 {
            permits.push(ctrl.acquire_connection().unwrap());
        }
        let result = ctrl.acquire_connection();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transient());
    }

    #[test]
    fn test_wal_backlog_check() {
        let ctrl = make_control(10, 10);
        // Under limit: OK
        assert!(ctrl.check_wal_backlog(1024).is_ok());
        // Over limit: Transient
        let result = ctrl.check_wal_backlog(u64::MAX);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transient());
    }

    #[test]
    fn test_replication_lag_check() {
        let ctrl = make_control(10, 10);
        assert!(ctrl.check_replication_lag(1024).is_ok());
        let result = ctrl.check_replication_lag(u64::MAX);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transient());
    }

    #[test]
    fn test_memory_budget_soft_limit() {
        let budget = MemoryBudget::new(1000);
        // 85% usage → pressure
        let result = budget.allocate(850);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_transient());
        assert!(err.to_string().contains("pressure"));
    }

    #[test]
    fn test_memory_budget_hard_limit() {
        let budget = MemoryBudget::new(1000);
        // 96% usage → hard limit
        let result = budget.allocate(960);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_transient());
        assert!(err.to_string().contains("hard limit"));
    }

    #[test]
    fn test_memory_budget_under_limit_ok() {
        let budget = MemoryBudget::new(1000);
        assert!(budget.allocate(500).is_ok());
        assert_eq!(budget.used_bytes(), 500);
        budget.release(500);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn test_metrics_snapshot() {
        let ctrl = make_control(10, 10);
        let _p1 = ctrl.acquire_query().unwrap();
        let _p2 = ctrl.acquire_write(0).unwrap();
        let m = ctrl.metrics();
        assert_eq!(m.inflight_queries, 1);
        assert_eq!(m.inflight_writes, 1);
    }

    #[test]
    fn test_admission_error_retry_after_ms() {
        let ctrl = make_control(0, 10);
        let err = ctrl.acquire_query().unwrap_err();
        assert!(err.retry_after_ms() > 0);
    }

    #[test]
    fn test_ddl_permit_acquired_and_released() {
        let ctrl = make_control(10, 10);
        assert_eq!(ctrl.inflight_ddl(), 0);
        {
            let _permit = ctrl.acquire_ddl().unwrap();
            assert_eq!(ctrl.inflight_ddl(), 1);
        }
        assert_eq!(ctrl.inflight_ddl(), 0);
    }

    #[test]
    fn test_ddl_limit_enforced() {
        let mut cfg = AdmissionConfig::default();
        cfg.max_inflight_queries = 10;
        cfg.max_inflight_writes_per_shard = 10;
        cfg.max_connections = 10;
        cfg.max_inflight_ddl = 2;
        let ctrl = AdmissionControl::new(cfg, 4);
        let _p1 = ctrl.acquire_ddl().unwrap();
        let _p2 = ctrl.acquire_ddl().unwrap();
        let result = ctrl.acquire_ddl();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transient());
        let m = ctrl.metrics();
        assert_eq!(m.ddl_rejected, 1);
    }

    #[test]
    fn test_memory_check_on_admission_control() {
        let ctrl = make_control(10, 10);
        // Under budget: OK
        assert!(ctrl.check_memory(100).is_ok());
        // Over hard limit: Transient
        let cfg = ctrl.config.read();
        let hard = (cfg.memory_budget_bytes as f64 * cfg.memory_hard_limit_ratio) as u64 + 1;
        drop(cfg);
        let result = ctrl.check_memory(hard);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_transient());
    }
}
