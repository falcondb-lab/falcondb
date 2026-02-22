//! # Module Status: STUB — not on the production OLTP write path.
//! Do NOT reference from planner/executor/txn for production workloads.
//!
//! P2-1b: Tenant registry — manages tenant lifecycle, resource tracking, and quota enforcement.
//!
//! The registry is the single source of truth for tenant metadata and runtime resource usage.
//! It enforces QPS limits, memory quotas, and concurrent transaction caps per tenant.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use dashmap::DashMap;

use falcon_common::tenant::{TenantConfig, TenantId, TenantQuota, TenantStatus};

/// Per-tenant runtime resource counters (lock-free atomics).
#[derive(Debug)]
pub struct TenantResourceCounters {
    /// Current number of active transactions for this tenant.
    pub active_txns: AtomicU32,
    /// Current memory usage in bytes attributed to this tenant.
    pub memory_bytes: AtomicU64,
    /// Total queries executed since tenant creation (monotonic).
    pub total_queries: AtomicU64,
    /// Queries executed in the current QPS window.
    pub window_queries: AtomicU64,
    /// Start of the current QPS measurement window (unix millis).
    pub window_start_ms: AtomicU64,
    /// Total transactions committed.
    pub txns_committed: AtomicU64,
    /// Total transactions aborted.
    pub txns_aborted: AtomicU64,
    /// Number of times quota was exceeded (QPS, memory, or txn limit).
    pub quota_exceeded_count: AtomicU64,
}

impl TenantResourceCounters {
    fn new() -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            active_txns: AtomicU32::new(0),
            memory_bytes: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            window_queries: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(now_ms),
            txns_committed: AtomicU64::new(0),
            txns_aborted: AtomicU64::new(0),
            quota_exceeded_count: AtomicU64::new(0),
        }
    }
}

/// Immutable snapshot of per-tenant metrics for observability.
#[derive(Debug, Clone)]
pub struct TenantMetricsSnapshot {
    pub tenant_id: TenantId,
    pub tenant_name: String,
    pub status: TenantStatus,
    pub active_txns: u32,
    pub memory_bytes: u64,
    pub total_queries: u64,
    pub current_qps: f64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub quota_exceeded_count: u64,
}

/// A registered tenant with its config and runtime counters.
struct TenantEntry {
    config: TenantConfig,
    counters: TenantResourceCounters,
}

/// Result of a quota check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaCheckResult {
    /// Quota allows the operation.
    Allowed,
    /// QPS limit exceeded.
    QpsExceeded { limit: u64, current: u64 },
    /// Memory limit exceeded.
    MemoryExceeded { limit: u64, current: u64 },
    /// Concurrent transaction limit exceeded.
    TxnLimitExceeded { limit: u32, current: u32 },
    /// Tenant is suspended or deleting.
    TenantNotActive { status: TenantStatus },
}

impl QuotaCheckResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// Central tenant registry. Thread-safe via DashMap.
pub struct TenantRegistry {
    tenants: DashMap<TenantId, TenantEntry>,
    /// QPS measurement window duration in milliseconds.
    qps_window_ms: u64,
    /// Monotonic counter for generating unique tenant IDs.
    next_id: AtomicU64,
}

impl TenantRegistry {
    /// Create a new registry with the system tenant pre-registered.
    pub fn new() -> Self {
        let reg = Self {
            tenants: DashMap::new(),
            qps_window_ms: 1000,        // 1-second QPS windows
            next_id: AtomicU64::new(2), // 1 is reserved for SYSTEM_TENANT_ID
        };
        // Always register the system tenant.
        reg.register_tenant(TenantConfig::system());
        reg
    }

    /// Register a new tenant. Returns false if tenant_id or tenant name already exists.
    pub fn register_tenant(&self, config: TenantConfig) -> bool {
        let tenant_id = config.tenant_id;
        if self.tenants.contains_key(&tenant_id) {
            return false;
        }
        // Also reject duplicate names (case-insensitive).
        let name_lower = config.name.to_lowercase();
        if self
            .tenants
            .iter()
            .any(|e| e.value().config.name.to_lowercase() == name_lower)
        {
            return false;
        }
        self.tenants.insert(
            tenant_id,
            TenantEntry {
                config,
                counters: TenantResourceCounters::new(),
            },
        );
        true
    }

    /// Allocate a unique tenant ID. Thread-safe (atomic increment).
    pub fn alloc_tenant_id(&self) -> TenantId {
        TenantId(self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Remove a tenant from the registry. Returns the config if found.
    pub fn remove_tenant(&self, tenant_id: TenantId) -> Option<TenantConfig> {
        self.tenants
            .remove(&tenant_id)
            .map(|(_, entry)| entry.config)
    }

    /// Get a tenant's config (cloned).
    pub fn get_config(&self, tenant_id: TenantId) -> Option<TenantConfig> {
        self.tenants.get(&tenant_id).map(|e| e.config.clone())
    }

    /// Update a tenant's quota.
    pub fn update_quota(&self, tenant_id: TenantId, quota: TenantQuota) -> bool {
        if let Some(mut entry) = self.tenants.get_mut(&tenant_id) {
            entry.config.quota = quota;
            true
        } else {
            false
        }
    }

    /// Update a tenant's status.
    pub fn update_status(&self, tenant_id: TenantId, status: TenantStatus) -> bool {
        if let Some(mut entry) = self.tenants.get_mut(&tenant_id) {
            entry.config.status = status;
            true
        } else {
            false
        }
    }

    /// Check whether a tenant can begin a new transaction (QPS + txn limit + status).
    pub fn check_begin_txn(&self, tenant_id: TenantId) -> QuotaCheckResult {
        let entry = match self.tenants.get(&tenant_id) {
            Some(e) => e,
            None => {
                return QuotaCheckResult::TenantNotActive {
                    status: TenantStatus::Deleting,
                }
            }
        };

        // Status check
        if entry.config.status != TenantStatus::Active {
            return QuotaCheckResult::TenantNotActive {
                status: entry.config.status,
            };
        }

        let quota = &entry.config.quota;

        // Concurrent txn limit
        if quota.max_concurrent_txns > 0 {
            let current = entry.counters.active_txns.load(Ordering::Relaxed);
            if current >= quota.max_concurrent_txns {
                entry
                    .counters
                    .quota_exceeded_count
                    .fetch_add(1, Ordering::Relaxed);
                return QuotaCheckResult::TxnLimitExceeded {
                    limit: quota.max_concurrent_txns,
                    current,
                };
            }
        }

        // QPS limit (sliding window approximation)
        if quota.max_qps > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let window_start = entry.counters.window_start_ms.load(Ordering::Relaxed);

            if now_ms.saturating_sub(window_start) >= self.qps_window_ms {
                // New window — reset
                entry.counters.window_queries.store(0, Ordering::Relaxed);
                entry
                    .counters
                    .window_start_ms
                    .store(now_ms, Ordering::Relaxed);
            }

            let current_qps = entry.counters.window_queries.load(Ordering::Relaxed);
            if current_qps >= quota.max_qps {
                entry
                    .counters
                    .quota_exceeded_count
                    .fetch_add(1, Ordering::Relaxed);
                return QuotaCheckResult::QpsExceeded {
                    limit: quota.max_qps,
                    current: current_qps,
                };
            }
        }

        // Memory limit
        if quota.max_memory_bytes > 0 {
            let current = entry.counters.memory_bytes.load(Ordering::Relaxed);
            if current >= quota.max_memory_bytes {
                entry
                    .counters
                    .quota_exceeded_count
                    .fetch_add(1, Ordering::Relaxed);
                return QuotaCheckResult::MemoryExceeded {
                    limit: quota.max_memory_bytes,
                    current,
                };
            }
        }

        QuotaCheckResult::Allowed
    }

    /// Record that a transaction has begun for a tenant.
    pub fn record_txn_begin(&self, tenant_id: TenantId) {
        if let Some(entry) = self.tenants.get(&tenant_id) {
            entry.counters.active_txns.fetch_add(1, Ordering::Relaxed);
            entry
                .counters
                .window_queries
                .fetch_add(1, Ordering::Relaxed);
            entry.counters.total_queries.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record that a transaction has committed for a tenant.
    pub fn record_txn_commit(&self, tenant_id: TenantId) {
        if let Some(entry) = self.tenants.get(&tenant_id) {
            entry.counters.active_txns.fetch_sub(1, Ordering::Relaxed);
            entry
                .counters
                .txns_committed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record that a transaction has been aborted for a tenant.
    pub fn record_txn_abort(&self, tenant_id: TenantId) {
        if let Some(entry) = self.tenants.get(&tenant_id) {
            entry.counters.active_txns.fetch_sub(1, Ordering::Relaxed);
            entry.counters.txns_aborted.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record memory allocation for a tenant.
    pub fn record_memory_alloc(&self, tenant_id: TenantId, bytes: u64) {
        if let Some(entry) = self.tenants.get(&tenant_id) {
            entry
                .counters
                .memory_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record memory deallocation for a tenant.
    pub fn record_memory_dealloc(&self, tenant_id: TenantId, bytes: u64) {
        if let Some(entry) = self.tenants.get(&tenant_id) {
            entry
                .counters
                .memory_bytes
                .fetch_sub(bytes, Ordering::Relaxed);
        }
    }

    /// Number of registered tenants.
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// List all tenant IDs.
    pub fn tenant_ids(&self) -> Vec<TenantId> {
        self.tenants.iter().map(|e| *e.key()).collect()
    }

    /// Take a metrics snapshot for a specific tenant.
    pub fn tenant_snapshot(&self, tenant_id: TenantId) -> Option<TenantMetricsSnapshot> {
        let entry = self.tenants.get(&tenant_id)?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let window_start = entry.counters.window_start_ms.load(Ordering::Relaxed);
        let window_queries = entry.counters.window_queries.load(Ordering::Relaxed);
        let elapsed_sec = (now_ms.saturating_sub(window_start) as f64) / 1000.0;
        let current_qps = if elapsed_sec > 0.0 {
            window_queries as f64 / elapsed_sec
        } else {
            0.0
        };

        Some(TenantMetricsSnapshot {
            tenant_id,
            tenant_name: entry.config.name.clone(),
            status: entry.config.status,
            active_txns: entry.counters.active_txns.load(Ordering::Relaxed),
            memory_bytes: entry.counters.memory_bytes.load(Ordering::Relaxed),
            total_queries: entry.counters.total_queries.load(Ordering::Relaxed),
            current_qps,
            txns_committed: entry.counters.txns_committed.load(Ordering::Relaxed),
            txns_aborted: entry.counters.txns_aborted.load(Ordering::Relaxed),
            quota_exceeded_count: entry.counters.quota_exceeded_count.load(Ordering::Relaxed),
        })
    }

    /// Take metrics snapshots for all tenants.
    pub fn all_tenant_snapshots(&self) -> Vec<TenantMetricsSnapshot> {
        self.tenants
            .iter()
            .filter_map(|entry| self.tenant_snapshot(*entry.key()))
            .collect()
    }
}

impl Default for TenantRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::tenant::{TenantConfig, TenantId, TenantQuota, SYSTEM_TENANT_ID};

    #[test]
    fn test_system_tenant_pre_registered() {
        let reg = TenantRegistry::new();
        assert_eq!(reg.tenant_count(), 1);
        assert!(reg.get_config(SYSTEM_TENANT_ID).is_some());
    }

    #[test]
    fn test_register_and_remove_tenant() {
        let reg = TenantRegistry::new();
        let cfg = TenantConfig::new(TenantId(1), "acme".into());
        assert!(reg.register_tenant(cfg));
        assert_eq!(reg.tenant_count(), 2);

        // Duplicate registration fails
        let cfg2 = TenantConfig::new(TenantId(1), "acme2".into());
        assert!(!reg.register_tenant(cfg2));

        // Remove
        let removed = reg.remove_tenant(TenantId(1));
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "acme");
        assert_eq!(reg.tenant_count(), 1);
    }

    #[test]
    fn test_quota_unlimited_always_allowed() {
        let reg = TenantRegistry::new();
        // System tenant has unlimited quotas
        assert!(reg.check_begin_txn(SYSTEM_TENANT_ID).is_allowed());
    }

    #[test]
    fn test_txn_limit_enforced() {
        let reg = TenantRegistry::new();
        let mut cfg = TenantConfig::new(TenantId(10), "limited".into());
        cfg.quota.max_concurrent_txns = 2;
        reg.register_tenant(cfg);

        // Begin 2 txns — should be allowed
        assert!(reg.check_begin_txn(TenantId(10)).is_allowed());
        reg.record_txn_begin(TenantId(10));
        assert!(reg.check_begin_txn(TenantId(10)).is_allowed());
        reg.record_txn_begin(TenantId(10));

        // Third txn — should be rejected
        let result = reg.check_begin_txn(TenantId(10));
        assert!(!result.is_allowed());
        assert!(matches!(
            result,
            QuotaCheckResult::TxnLimitExceeded {
                limit: 2,
                current: 2
            }
        ));

        // Commit one — now allowed again
        reg.record_txn_commit(TenantId(10));
        assert!(reg.check_begin_txn(TenantId(10)).is_allowed());
    }

    #[test]
    fn test_memory_limit_enforced() {
        let reg = TenantRegistry::new();
        let mut cfg = TenantConfig::new(TenantId(20), "mem_limited".into());
        cfg.quota.max_memory_bytes = 1000;
        reg.register_tenant(cfg);

        reg.record_memory_alloc(TenantId(20), 999);
        assert!(reg.check_begin_txn(TenantId(20)).is_allowed());

        reg.record_memory_alloc(TenantId(20), 1);
        let result = reg.check_begin_txn(TenantId(20));
        assert!(matches!(
            result,
            QuotaCheckResult::MemoryExceeded { limit: 1000, .. }
        ));

        reg.record_memory_dealloc(TenantId(20), 500);
        assert!(reg.check_begin_txn(TenantId(20)).is_allowed());
    }

    #[test]
    fn test_suspended_tenant_rejected() {
        let reg = TenantRegistry::new();
        let cfg = TenantConfig::new(TenantId(30), "suspended_co".into());
        reg.register_tenant(cfg);
        reg.update_status(TenantId(30), TenantStatus::Suspended);

        let result = reg.check_begin_txn(TenantId(30));
        assert!(matches!(
            result,
            QuotaCheckResult::TenantNotActive {
                status: TenantStatus::Suspended
            }
        ));
    }

    #[test]
    fn test_tenant_snapshot() {
        let reg = TenantRegistry::new();
        let cfg = TenantConfig::new(TenantId(40), "metrics_co".into());
        reg.register_tenant(cfg);

        reg.record_txn_begin(TenantId(40));
        reg.record_txn_begin(TenantId(40));
        reg.record_txn_commit(TenantId(40));

        let snap = reg.tenant_snapshot(TenantId(40)).unwrap();
        assert_eq!(snap.tenant_id, TenantId(40));
        assert_eq!(snap.active_txns, 1);
        assert_eq!(snap.txns_committed, 1);
        assert_eq!(snap.total_queries, 2);
    }

    #[test]
    fn test_nonexistent_tenant_rejected() {
        let reg = TenantRegistry::new();
        let result = reg.check_begin_txn(TenantId(999));
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_update_quota() {
        let reg = TenantRegistry::new();
        let cfg = TenantConfig::new(TenantId(50), "dynamic".into());
        reg.register_tenant(cfg);

        let new_quota = TenantQuota {
            max_qps: 500,
            max_concurrent_txns: 10,
            ..Default::default()
        };
        assert!(reg.update_quota(TenantId(50), new_quota));

        let config = reg.get_config(TenantId(50)).unwrap();
        assert_eq!(config.quota.max_qps, 500);
        assert_eq!(config.quota.max_concurrent_txns, 10);
    }
}
