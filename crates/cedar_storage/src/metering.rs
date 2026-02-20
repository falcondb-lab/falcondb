//! P3-3: Resource metering and billing infrastructure.
//!
//! Tracks per-tenant resource consumption for commercial billing:
//! - CPU time (microseconds)
//! - Memory usage (bytes)
//! - Storage occupancy (bytes)
//! - QPS (queries per second)
//! - Transaction count (committed / aborted)
//!
//! Supports quota-based throttling and overage alerts.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use cedar_common::tenant::TenantId;

/// Per-tenant resource counters (lock-free atomics).
#[derive(Debug)]
pub struct TenantMeteringCounters {
    /// Cumulative CPU time consumed (microseconds).
    pub cpu_us: AtomicU64,
    /// Current memory usage (bytes).
    pub memory_bytes: AtomicU64,
    /// Current storage usage (bytes).
    pub storage_bytes: AtomicU64,
    /// Total queries executed.
    pub query_count: AtomicU64,
    /// Total transactions committed.
    pub txn_committed: AtomicU64,
    /// Total transactions aborted.
    pub txn_aborted: AtomicU64,
    /// Total bytes read.
    pub bytes_read: AtomicU64,
    /// Total bytes written.
    pub bytes_written: AtomicU64,
    /// Timestamp of last reset (for billing period).
    pub period_start: Instant,
}

impl TenantMeteringCounters {
    fn new() -> Self {
        Self {
            cpu_us: AtomicU64::new(0),
            memory_bytes: AtomicU64::new(0),
            storage_bytes: AtomicU64::new(0),
            query_count: AtomicU64::new(0),
            txn_committed: AtomicU64::new(0),
            txn_aborted: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            period_start: Instant::now(),
        }
    }
}

/// Snapshot of a tenant's resource usage for a billing period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantUsageSnapshot {
    pub tenant_id: TenantId,
    /// CPU time consumed (microseconds).
    pub cpu_us: u64,
    /// Current memory usage (bytes).
    pub memory_bytes: u64,
    /// Current storage usage (bytes).
    pub storage_bytes: u64,
    /// Total queries in this period.
    pub query_count: u64,
    /// Transactions committed.
    pub txn_committed: u64,
    /// Transactions aborted.
    pub txn_aborted: u64,
    /// Total bytes read.
    pub bytes_read: u64,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Duration of the billing period (seconds).
    pub period_duration_secs: u64,
    /// Computed QPS (queries / period_duration).
    pub qps: f64,
}

/// Throttle action to take when a tenant exceeds quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThrottleAction {
    /// Allow the request (no throttle).
    Allow,
    /// Delay the request (soft throttle).
    Delay { delay_ms: u64 },
    /// Reject the request (hard throttle).
    Reject,
}

impl std::fmt::Display for ThrottleAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allow => write!(f, "allow"),
            Self::Delay { delay_ms } => write!(f, "delay({}ms)", delay_ms),
            Self::Reject => write!(f, "reject"),
        }
    }
}

/// Per-tenant metering quota limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteringQuota {
    /// Max CPU microseconds per billing period (0 = unlimited).
    pub max_cpu_us: u64,
    /// Max memory bytes (0 = unlimited).
    pub max_memory_bytes: u64,
    /// Max storage bytes (0 = unlimited).
    pub max_storage_bytes: u64,
    /// Max queries per billing period (0 = unlimited).
    pub max_queries: u64,
    /// Max transactions per billing period (0 = unlimited).
    pub max_txns: u64,
    /// Soft limit ratio (0.0-1.0). At this ratio, start delaying.
    pub soft_limit_ratio: f64,
}

impl Default for MeteringQuota {
    fn default() -> Self {
        Self {
            max_cpu_us: 0,
            max_memory_bytes: 0,
            max_storage_bytes: 0,
            max_queries: 0,
            max_txns: 0,
            soft_limit_ratio: 0.8,
        }
    }
}

/// Resource meter — tracks per-tenant resource consumption and enforces quotas.
pub struct ResourceMeter {
    counters: DashMap<TenantId, TenantMeteringCounters>,
    quotas: DashMap<TenantId, MeteringQuota>,
    /// Total overage alerts triggered.
    overage_alerts: AtomicU64,
    /// Total requests throttled.
    throttled_count: AtomicU64,
}

impl ResourceMeter {
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
            quotas: DashMap::new(),
            overage_alerts: AtomicU64::new(0),
            throttled_count: AtomicU64::new(0),
        }
    }

    /// Ensure a tenant has counters initialized.
    pub fn register_tenant(&self, tenant_id: TenantId) {
        self.counters.entry(tenant_id).or_insert_with(TenantMeteringCounters::new);
    }

    /// Set quota for a tenant.
    pub fn set_quota(&self, tenant_id: TenantId, quota: MeteringQuota) {
        self.quotas.insert(tenant_id, quota);
    }

    /// Record CPU time consumed by a tenant.
    pub fn record_cpu(&self, tenant_id: TenantId, cpu_us: u64) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
        }
    }

    /// Update memory usage for a tenant.
    pub fn set_memory(&self, tenant_id: TenantId, bytes: u64) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.memory_bytes.store(bytes, Ordering::Relaxed);
        }
    }

    /// Update storage usage for a tenant.
    pub fn set_storage(&self, tenant_id: TenantId, bytes: u64) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.storage_bytes.store(bytes, Ordering::Relaxed);
        }
    }

    /// Record a query execution.
    pub fn record_query(&self, tenant_id: TenantId) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.query_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a committed transaction.
    pub fn record_txn_commit(&self, tenant_id: TenantId) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.txn_committed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an aborted transaction.
    pub fn record_txn_abort(&self, tenant_id: TenantId) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.txn_aborted.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record bytes read.
    pub fn record_read(&self, tenant_id: TenantId, bytes: u64) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.bytes_read.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record bytes written.
    pub fn record_write(&self, tenant_id: TenantId, bytes: u64) {
        if let Some(c) = self.counters.get(&tenant_id) {
            c.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Check if a tenant should be throttled based on their quota.
    pub fn check_throttle(&self, tenant_id: TenantId) -> ThrottleAction {
        let quota = match self.quotas.get(&tenant_id) {
            Some(q) => q.clone(),
            None => return ThrottleAction::Allow,
        };
        let counters = match self.counters.get(&tenant_id) {
            Some(c) => c,
            None => return ThrottleAction::Allow,
        };

        // Check hard limits
        if quota.max_queries > 0 && counters.query_count.load(Ordering::Relaxed) >= quota.max_queries {
            self.throttled_count.fetch_add(1, Ordering::Relaxed);
            self.overage_alerts.fetch_add(1, Ordering::Relaxed);
            return ThrottleAction::Reject;
        }
        if quota.max_memory_bytes > 0 && counters.memory_bytes.load(Ordering::Relaxed) >= quota.max_memory_bytes {
            self.throttled_count.fetch_add(1, Ordering::Relaxed);
            return ThrottleAction::Reject;
        }
        if quota.max_storage_bytes > 0 && counters.storage_bytes.load(Ordering::Relaxed) >= quota.max_storage_bytes {
            self.throttled_count.fetch_add(1, Ordering::Relaxed);
            return ThrottleAction::Reject;
        }

        // Check soft limits (delay)
        if quota.max_queries > 0 {
            let ratio = counters.query_count.load(Ordering::Relaxed) as f64 / quota.max_queries as f64;
            if ratio >= quota.soft_limit_ratio {
                let delay_ms = ((ratio - quota.soft_limit_ratio) / (1.0 - quota.soft_limit_ratio) * 100.0) as u64;
                return ThrottleAction::Delay { delay_ms: delay_ms.min(1000) };
            }
        }

        ThrottleAction::Allow
    }

    /// Get a usage snapshot for a tenant.
    pub fn usage_snapshot(&self, tenant_id: TenantId) -> Option<TenantUsageSnapshot> {
        let counters = self.counters.get(&tenant_id)?;
        let period_secs = counters.period_start.elapsed().as_secs().max(1);
        let query_count = counters.query_count.load(Ordering::Relaxed);
        Some(TenantUsageSnapshot {
            tenant_id,
            cpu_us: counters.cpu_us.load(Ordering::Relaxed),
            memory_bytes: counters.memory_bytes.load(Ordering::Relaxed),
            storage_bytes: counters.storage_bytes.load(Ordering::Relaxed),
            query_count,
            txn_committed: counters.txn_committed.load(Ordering::Relaxed),
            txn_aborted: counters.txn_aborted.load(Ordering::Relaxed),
            bytes_read: counters.bytes_read.load(Ordering::Relaxed),
            bytes_written: counters.bytes_written.load(Ordering::Relaxed),
            period_duration_secs: period_secs,
            qps: query_count as f64 / period_secs as f64,
        })
    }

    /// Get usage snapshots for all tenants.
    pub fn all_usage_snapshots(&self) -> Vec<TenantUsageSnapshot> {
        self.counters.iter()
            .filter_map(|entry| self.usage_snapshot(*entry.key()))
            .collect()
    }

    /// Reset counters for a new billing period.
    pub fn reset_period(&self, tenant_id: TenantId) {
        if let Some(mut c) = self.counters.get_mut(&tenant_id) {
            c.cpu_us.store(0, Ordering::Relaxed);
            c.query_count.store(0, Ordering::Relaxed);
            c.txn_committed.store(0, Ordering::Relaxed);
            c.txn_aborted.store(0, Ordering::Relaxed);
            c.bytes_read.store(0, Ordering::Relaxed);
            c.bytes_written.store(0, Ordering::Relaxed);
            // memory_bytes and storage_bytes are current values, not cumulative
        }
    }

    /// Total overage alerts triggered.
    pub fn overage_alerts(&self) -> u64 {
        self.overage_alerts.load(Ordering::Relaxed)
    }

    /// Total requests throttled.
    pub fn throttled_count(&self) -> u64 {
        self.throttled_count.load(Ordering::Relaxed)
    }

    /// Number of registered tenants.
    pub fn tenant_count(&self) -> usize {
        self.counters.len()
    }
}

impl Default for ResourceMeter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cedar_common::tenant::TenantId;

    #[test]
    fn test_register_and_record() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);

        meter.record_cpu(tid, 5000);
        meter.record_query(tid);
        meter.record_query(tid);
        meter.record_txn_commit(tid);
        meter.set_memory(tid, 1024);
        meter.set_storage(tid, 2048);
        meter.record_read(tid, 100);
        meter.record_write(tid, 50);

        let snap = meter.usage_snapshot(tid).unwrap();
        assert_eq!(snap.cpu_us, 5000);
        assert_eq!(snap.query_count, 2);
        assert_eq!(snap.txn_committed, 1);
        assert_eq!(snap.memory_bytes, 1024);
        assert_eq!(snap.storage_bytes, 2048);
        assert_eq!(snap.bytes_read, 100);
        assert_eq!(snap.bytes_written, 50);
    }

    #[test]
    fn test_throttle_allow() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);
        assert_eq!(meter.check_throttle(tid), ThrottleAction::Allow);
    }

    #[test]
    fn test_throttle_reject_query_limit() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);
        meter.set_quota(tid, MeteringQuota {
            max_queries: 10,
            ..MeteringQuota::default()
        });

        for _ in 0..10 {
            meter.record_query(tid);
        }

        assert_eq!(meter.check_throttle(tid), ThrottleAction::Reject);
        assert_eq!(meter.throttled_count(), 1);
    }

    #[test]
    fn test_throttle_delay_soft_limit() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);
        meter.set_quota(tid, MeteringQuota {
            max_queries: 100,
            soft_limit_ratio: 0.8,
            ..MeteringQuota::default()
        });

        // Record 85 queries (85% > 80% soft limit)
        for _ in 0..85 {
            meter.record_query(tid);
        }

        match meter.check_throttle(tid) {
            ThrottleAction::Delay { delay_ms } => assert!(delay_ms > 0),
            other => panic!("Expected Delay, got {:?}", other),
        }
    }

    #[test]
    fn test_throttle_reject_memory_limit() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);
        meter.set_quota(tid, MeteringQuota {
            max_memory_bytes: 1024,
            ..MeteringQuota::default()
        });

        meter.set_memory(tid, 2048);
        assert_eq!(meter.check_throttle(tid), ThrottleAction::Reject);
    }

    #[test]
    fn test_no_quota_means_no_throttle() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);
        // No quota set — unlimited
        for _ in 0..1000 {
            meter.record_query(tid);
        }
        assert_eq!(meter.check_throttle(tid), ThrottleAction::Allow);
    }

    #[test]
    fn test_reset_period() {
        let meter = ResourceMeter::new();
        let tid = TenantId(1);
        meter.register_tenant(tid);
        meter.record_cpu(tid, 5000);
        meter.record_query(tid);
        meter.record_txn_commit(tid);
        meter.record_read(tid, 100);
        meter.set_memory(tid, 1024);

        meter.reset_period(tid);

        let snap = meter.usage_snapshot(tid).unwrap();
        assert_eq!(snap.cpu_us, 0);
        assert_eq!(snap.query_count, 0);
        assert_eq!(snap.txn_committed, 0);
        assert_eq!(snap.bytes_read, 0);
        // memory is current, not reset
        assert_eq!(snap.memory_bytes, 1024);
    }

    #[test]
    fn test_all_usage_snapshots() {
        let meter = ResourceMeter::new();
        meter.register_tenant(TenantId(1));
        meter.register_tenant(TenantId(2));
        meter.record_query(TenantId(1));

        let snaps = meter.all_usage_snapshots();
        assert_eq!(snaps.len(), 2);
    }

    #[test]
    fn test_unregistered_tenant_noop() {
        let meter = ResourceMeter::new();
        meter.record_cpu(TenantId(99), 100); // should not panic
        assert!(meter.usage_snapshot(TenantId(99)).is_none());
    }
}
