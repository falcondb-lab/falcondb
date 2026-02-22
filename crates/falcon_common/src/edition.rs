//! P3-1: Product edition tiers, feature gating, and license model.
//!
//! Defines the commercial product structure:
//! - Community Edition: basic OLTP, open-source core
//! - Enterprise Edition: multi-tenancy, RBAC, HA, SLA, audit
//! - Cloud Edition: managed service, control plane, cross-region

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

/// Product edition tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EditionTier {
    /// Basic OLTP — open-source core features.
    Community,
    /// Multi-tenancy, RBAC, HA, SLA, audit, backup.
    Enterprise,
    /// Managed service with control plane, cross-region, billing.
    Cloud,
}

impl fmt::Display for EditionTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Community => write!(f, "community"),
            Self::Enterprise => write!(f, "enterprise"),
            Self::Cloud => write!(f, "cloud"),
        }
    }
}

impl EditionTier {
    /// Parse from string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "community" | "ce" => Some(Self::Community),
            "enterprise" | "ee" => Some(Self::Enterprise),
            "cloud" => Some(Self::Cloud),
            _ => None,
        }
    }
}

/// Individual gated features that require a specific edition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Feature {
    // ── Community (always available) ──
    BasicTransaction,
    BasicSQL,
    SingleNodeDeployment,
    BasicMonitoring,

    // ── Enterprise ──
    MultiTenancy,
    RbacSecurity,
    AuditLog,
    HighAvailability,
    SlaPriority,
    OnlineBackup,
    OnlineScaling,
    AdvancedMonitoring,
    RollingUpgrade,

    // ── Cloud ──
    ControlPlane,
    ResourceMetering,
    TenantBilling,
    CrossRegionReplication,
    ManagedEncryption,
    IpAllowList,
    KmsIntegration,
    WebConsole,
    CapacityPrediction,
    AutoFailover,
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Feature gate — determines which features are available in the running instance.
#[derive(Debug, Clone)]
pub struct FeatureGate {
    edition: EditionTier,
    enabled_features: HashSet<Feature>,
}

impl FeatureGate {
    /// Create a feature gate for the given edition with default feature sets.
    pub fn for_edition(edition: EditionTier) -> Self {
        let mut features = HashSet::new();

        // Community features (always on)
        features.insert(Feature::BasicTransaction);
        features.insert(Feature::BasicSQL);
        features.insert(Feature::SingleNodeDeployment);
        features.insert(Feature::BasicMonitoring);

        if edition == EditionTier::Enterprise || edition == EditionTier::Cloud {
            features.insert(Feature::MultiTenancy);
            features.insert(Feature::RbacSecurity);
            features.insert(Feature::AuditLog);
            features.insert(Feature::HighAvailability);
            features.insert(Feature::SlaPriority);
            features.insert(Feature::OnlineBackup);
            features.insert(Feature::OnlineScaling);
            features.insert(Feature::AdvancedMonitoring);
            features.insert(Feature::RollingUpgrade);
        }

        if edition == EditionTier::Cloud {
            features.insert(Feature::ControlPlane);
            features.insert(Feature::ResourceMetering);
            features.insert(Feature::TenantBilling);
            features.insert(Feature::CrossRegionReplication);
            features.insert(Feature::ManagedEncryption);
            features.insert(Feature::IpAllowList);
            features.insert(Feature::KmsIntegration);
            features.insert(Feature::WebConsole);
            features.insert(Feature::CapacityPrediction);
            features.insert(Feature::AutoFailover);
        }

        Self {
            edition,
            enabled_features: features,
        }
    }

    /// Check if a specific feature is enabled.
    pub fn is_enabled(&self, feature: Feature) -> bool {
        self.enabled_features.contains(&feature)
    }

    /// Require a feature, returning an error message if not available.
    pub fn require(&self, feature: Feature) -> Result<(), String> {
        if self.is_enabled(feature) {
            Ok(())
        } else {
            Err(format!(
                "Feature '{}' requires {} edition or higher (current: {})",
                feature,
                Self::minimum_edition(feature),
                self.edition,
            ))
        }
    }

    /// Get the current edition.
    pub fn edition(&self) -> EditionTier {
        self.edition
    }

    /// Number of enabled features.
    pub fn enabled_count(&self) -> usize {
        self.enabled_features.len()
    }

    /// Minimum edition required for a feature.
    pub fn minimum_edition(feature: Feature) -> EditionTier {
        match feature {
            Feature::BasicTransaction
            | Feature::BasicSQL
            | Feature::SingleNodeDeployment
            | Feature::BasicMonitoring => EditionTier::Community,

            Feature::MultiTenancy
            | Feature::RbacSecurity
            | Feature::AuditLog
            | Feature::HighAvailability
            | Feature::SlaPriority
            | Feature::OnlineBackup
            | Feature::OnlineScaling
            | Feature::AdvancedMonitoring
            | Feature::RollingUpgrade => EditionTier::Enterprise,

            Feature::ControlPlane
            | Feature::ResourceMetering
            | Feature::TenantBilling
            | Feature::CrossRegionReplication
            | Feature::ManagedEncryption
            | Feature::IpAllowList
            | Feature::KmsIntegration
            | Feature::WebConsole
            | Feature::CapacityPrediction
            | Feature::AutoFailover => EditionTier::Cloud,
        }
    }
}

/// License information for commercial deployments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseInfo {
    /// Edition this license authorizes.
    pub edition: EditionTier,
    /// Organization / customer name.
    pub organization: String,
    /// License key (opaque token).
    pub license_key: String,
    /// Expiration date (unix timestamp seconds, 0 = never).
    pub expires_at: u64,
    /// Maximum number of nodes allowed.
    pub max_nodes: u32,
    /// Maximum number of tenants allowed (0 = unlimited).
    pub max_tenants: u32,
    /// Whether the license has been validated.
    pub validated: bool,
}

impl LicenseInfo {
    /// Create a community (free, unlimited) license.
    pub fn community() -> Self {
        Self {
            edition: EditionTier::Community,
            organization: "community".into(),
            license_key: String::new(),
            expires_at: 0,
            max_nodes: 1,
            max_tenants: 1,
            validated: true,
        }
    }

    /// Create an enterprise license.
    pub fn enterprise(
        org: String,
        key: String,
        expires_at: u64,
        max_nodes: u32,
        max_tenants: u32,
    ) -> Self {
        Self {
            edition: EditionTier::Enterprise,
            organization: org,
            license_key: key,
            expires_at,
            max_nodes,
            max_tenants,
            validated: false,
        }
    }

    /// Check if the license has expired (relative to a unix timestamp).
    pub fn is_expired(&self, now_unix: u64) -> bool {
        self.expires_at > 0 && now_unix > self.expires_at
    }

    /// Check if adding a node would exceed the license limit.
    pub fn can_add_node(&self, current_nodes: u32) -> bool {
        self.max_nodes == 0 || current_nodes < self.max_nodes
    }

    /// Check if adding a tenant would exceed the license limit.
    pub fn can_add_tenant(&self, current_tenants: u32) -> bool {
        self.max_tenants == 0 || current_tenants < self.max_tenants
    }
}

impl Default for LicenseInfo {
    fn default() -> Self {
        Self::community()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_community_features() {
        let gate = FeatureGate::for_edition(EditionTier::Community);
        assert!(gate.is_enabled(Feature::BasicTransaction));
        assert!(gate.is_enabled(Feature::BasicSQL));
        assert!(!gate.is_enabled(Feature::MultiTenancy));
        assert!(!gate.is_enabled(Feature::ControlPlane));
        assert_eq!(gate.enabled_count(), 4);
    }

    #[test]
    fn test_enterprise_features() {
        let gate = FeatureGate::for_edition(EditionTier::Enterprise);
        assert!(gate.is_enabled(Feature::BasicTransaction));
        assert!(gate.is_enabled(Feature::MultiTenancy));
        assert!(gate.is_enabled(Feature::RbacSecurity));
        assert!(gate.is_enabled(Feature::HighAvailability));
        assert!(!gate.is_enabled(Feature::ControlPlane));
        assert!(!gate.is_enabled(Feature::CrossRegionReplication));
        assert_eq!(gate.enabled_count(), 13);
    }

    #[test]
    fn test_cloud_features_superset() {
        let gate = FeatureGate::for_edition(EditionTier::Cloud);
        assert!(gate.is_enabled(Feature::BasicTransaction));
        assert!(gate.is_enabled(Feature::MultiTenancy));
        assert!(gate.is_enabled(Feature::ControlPlane));
        assert!(gate.is_enabled(Feature::CrossRegionReplication));
        assert!(gate.is_enabled(Feature::KmsIntegration));
        assert_eq!(gate.enabled_count(), 23);
    }

    #[test]
    fn test_require_denied() {
        let gate = FeatureGate::for_edition(EditionTier::Community);
        assert!(gate.require(Feature::BasicTransaction).is_ok());
        let err = gate.require(Feature::MultiTenancy);
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("enterprise"));
    }

    #[test]
    fn test_minimum_edition() {
        assert_eq!(
            FeatureGate::minimum_edition(Feature::BasicSQL),
            EditionTier::Community
        );
        assert_eq!(
            FeatureGate::minimum_edition(Feature::AuditLog),
            EditionTier::Enterprise
        );
        assert_eq!(
            FeatureGate::minimum_edition(Feature::ControlPlane),
            EditionTier::Cloud
        );
    }

    #[test]
    fn test_edition_display_and_parse() {
        assert_eq!(EditionTier::Community.to_string(), "community");
        assert_eq!(
            EditionTier::from_str_loose("EE"),
            Some(EditionTier::Enterprise)
        );
        assert_eq!(
            EditionTier::from_str_loose("cloud"),
            Some(EditionTier::Cloud)
        );
        assert_eq!(EditionTier::from_str_loose("invalid"), None);
    }

    #[test]
    fn test_license_community() {
        let lic = LicenseInfo::community();
        assert_eq!(lic.edition, EditionTier::Community);
        assert!(lic.validated);
        assert!(!lic.is_expired(9999999999));
        assert!(lic.can_add_node(0));
        assert!(!lic.can_add_node(1));
    }

    #[test]
    fn test_license_enterprise_expiry() {
        let lic = LicenseInfo::enterprise("AcmeCorp".into(), "key-123".into(), 1000, 10, 50);
        assert!(!lic.is_expired(500));
        assert!(lic.is_expired(1500));
        assert!(lic.can_add_node(9));
        assert!(!lic.can_add_node(10));
        assert!(lic.can_add_tenant(49));
        assert!(!lic.can_add_tenant(50));
    }

    #[test]
    fn test_license_unlimited() {
        let mut lic = LicenseInfo::enterprise("X".into(), "k".into(), 0, 0, 0);
        lic.validated = true;
        assert!(!lic.is_expired(99999999));
        assert!(lic.can_add_node(1000));
        assert!(lic.can_add_tenant(1000));
    }
}
