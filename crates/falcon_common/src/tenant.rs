//! P2-1: Multi-tenant isolation model.
//!
//! Provides the foundational types for tenant-level resource isolation,
//! schema isolation, and cross-tenant transaction prevention.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TenantId(pub u64);

/// Reserved tenant ID for the system / default tenant (single-tenant mode).
pub const SYSTEM_TENANT_ID: TenantId = TenantId(0);

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tenant:{}", self.0)
    }
}

/// Per-tenant resource quota configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct TenantQuota {
    /// Maximum queries per second (0 = unlimited).
    pub max_qps: u64,
    /// Maximum memory bytes this tenant may use across all shards (0 = unlimited).
    pub max_memory_bytes: u64,
    /// Maximum concurrent active transactions (0 = unlimited).
    pub max_concurrent_txns: u32,
    /// Maximum number of tables the tenant may create (0 = unlimited).
    pub max_tables: u32,
    /// Maximum total storage bytes across all tables (0 = unlimited).
    pub max_storage_bytes: u64,
}


impl TenantQuota {
    /// Returns true if all quotas are unlimited (no enforcement).
    pub fn is_unlimited(&self) -> bool {
        self.max_qps == 0
            && self.max_memory_bytes == 0
            && self.max_concurrent_txns == 0
            && self.max_tables == 0
            && self.max_storage_bytes == 0
    }
}

/// Tenant status in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantStatus {
    /// Tenant is active and can serve requests.
    Active,
    /// Tenant is suspended (e.g. quota exceeded, admin action).
    /// Reads may still be allowed; writes are rejected.
    Suspended,
    /// Tenant is being deleted. No new operations allowed.
    Deleting,
}

impl fmt::Display for TenantStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Deleting => write!(f, "deleting"),
        }
    }
}

/// Static configuration for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    pub tenant_id: TenantId,
    pub name: String,
    pub status: TenantStatus,
    pub quota: TenantQuota,
    /// Default schema name for this tenant (e.g. "tenant_<id>").
    pub default_schema: String,
    /// Created-at timestamp (unix millis).
    pub created_at_ms: u64,
}

impl TenantConfig {
    /// Create a new tenant config with default quota.
    pub fn new(tenant_id: TenantId, name: String) -> Self {
        Self {
            tenant_id,
            name: name.clone(),
            status: TenantStatus::Active,
            quota: TenantQuota::default(),
            default_schema: format!("tenant_{}", tenant_id.0),
            created_at_ms: 0,
        }
    }

    /// Create the system/default tenant config.
    pub fn system() -> Self {
        Self {
            tenant_id: SYSTEM_TENANT_ID,
            name: "system".into(),
            status: TenantStatus::Active,
            quota: TenantQuota::default(),
            default_schema: "public".into(),
            created_at_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id_display() {
        assert_eq!(TenantId(42).to_string(), "tenant:42");
    }

    #[test]
    fn test_system_tenant() {
        let sys = TenantConfig::system();
        assert_eq!(sys.tenant_id, SYSTEM_TENANT_ID);
        assert_eq!(sys.default_schema, "public");
        assert_eq!(sys.status, TenantStatus::Active);
    }

    #[test]
    fn test_default_quota_is_unlimited() {
        let q = TenantQuota::default();
        assert!(q.is_unlimited());
    }

    #[test]
    fn test_quota_with_limits_not_unlimited() {
        let q = TenantQuota {
            max_qps: 1000,
            ..Default::default()
        };
        assert!(!q.is_unlimited());
    }

    #[test]
    fn test_tenant_config_new() {
        let cfg = TenantConfig::new(TenantId(7), "acme".into());
        assert_eq!(cfg.tenant_id, TenantId(7));
        assert_eq!(cfg.name, "acme");
        assert_eq!(cfg.default_schema, "tenant_7");
        assert_eq!(cfg.status, TenantStatus::Active);
    }

    #[test]
    fn test_tenant_status_display() {
        assert_eq!(TenantStatus::Active.to_string(), "active");
        assert_eq!(TenantStatus::Suspended.to_string(), "suspended");
        assert_eq!(TenantStatus::Deleting.to_string(), "deleting");
    }
}
