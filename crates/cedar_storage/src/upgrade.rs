//! P2-7: Upgrade compatibility — version headers, rolling upgrade safety checks.
//!
//! Provides infrastructure for safe online upgrades:
//! - Snapshot version headers for forward/backward compatibility
//! - Rolling upgrade readiness checks
//! - Version compatibility matrix

use serde::{Deserialize, Serialize};

/// Current CedarDB software version.
pub const CEDAR_VERSION_MAJOR: u32 = 0;
pub const CEDAR_VERSION_MINOR: u32 = 2;
pub const CEDAR_VERSION_PATCH: u32 = 0;

/// Minimum compatible version for rolling upgrades.
/// Nodes running a version older than this cannot participate in a mixed-version cluster.
pub const MIN_COMPATIBLE_MAJOR: u32 = 0;
pub const MIN_COMPATIBLE_MINOR: u32 = 1;

/// Snapshot format version. Increment when the snapshot wire format changes.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Version information embedded in snapshots and WAL headers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionHeader {
    /// Software version that created this artifact.
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    /// WAL format version at time of creation.
    pub wal_format_version: u32,
    /// Snapshot format version (for checkpoint files).
    pub snapshot_format_version: u32,
}

impl VersionHeader {
    /// Create a header reflecting the current software version.
    pub fn current() -> Self {
        Self {
            major: CEDAR_VERSION_MAJOR,
            minor: CEDAR_VERSION_MINOR,
            patch: CEDAR_VERSION_PATCH,
            wal_format_version: crate::wal::WAL_FORMAT_VERSION,
            snapshot_format_version: SNAPSHOT_FORMAT_VERSION,
        }
    }

    /// Version string (e.g. "0.2.0").
    pub fn version_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Result of a compatibility check between two nodes or between a node and an artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityResult {
    /// Fully compatible — no issues.
    Compatible,
    /// Compatible with warnings (e.g. minor version difference).
    CompatibleWithWarnings(Vec<String>),
    /// Incompatible — cannot proceed.
    Incompatible(Vec<String>),
}

impl CompatibilityResult {
    pub fn is_compatible(&self) -> bool {
        !matches!(self, Self::Incompatible(_))
    }
}

/// Check whether a remote node's version is compatible with the local node for rolling upgrade.
pub fn check_rolling_upgrade_compatibility(
    local: &VersionHeader,
    remote: &VersionHeader,
) -> CompatibilityResult {
    let mut warnings = Vec::new();
    let mut errors = Vec::new();

    // Major version must match (no cross-major rolling upgrades)
    if local.major != remote.major {
        errors.push(format!(
            "Major version mismatch: local={}, remote={}",
            local.major, remote.major
        ));
    }

    // Check minimum compatible version
    if remote.minor < MIN_COMPATIBLE_MINOR && remote.major <= MIN_COMPATIBLE_MAJOR {
        errors.push(format!(
            "Remote version {}.{}.{} is below minimum compatible {}.{}",
            remote.major, remote.minor, remote.patch,
            MIN_COMPATIBLE_MAJOR, MIN_COMPATIBLE_MINOR
        ));
    }

    // WAL format must be compatible
    if local.wal_format_version != remote.wal_format_version {
        if local.wal_format_version > remote.wal_format_version {
            warnings.push(format!(
                "WAL format version mismatch: local={}, remote={} (local is newer)",
                local.wal_format_version, remote.wal_format_version
            ));
        } else {
            warnings.push(format!(
                "WAL format version mismatch: local={}, remote={} (remote is newer)",
                local.wal_format_version, remote.wal_format_version
            ));
        }
    }

    // Snapshot format compatibility
    if local.snapshot_format_version != remote.snapshot_format_version {
        warnings.push(format!(
            "Snapshot format version mismatch: local={}, remote={}",
            local.snapshot_format_version, remote.snapshot_format_version
        ));
    }

    // Minor version difference warning
    if local.minor != remote.minor {
        warnings.push(format!(
            "Minor version difference: local={}.{}, remote={}.{}",
            local.major, local.minor, remote.major, remote.minor
        ));
    }

    if !errors.is_empty() {
        CompatibilityResult::Incompatible(errors)
    } else if !warnings.is_empty() {
        CompatibilityResult::CompatibleWithWarnings(warnings)
    } else {
        CompatibilityResult::Compatible
    }
}

/// Check whether a snapshot can be loaded by the current version.
pub fn check_snapshot_compatibility(header: &VersionHeader) -> CompatibilityResult {
    let current = VersionHeader::current();
    let mut warnings = Vec::new();
    let mut errors = Vec::new();

    if header.snapshot_format_version > current.snapshot_format_version {
        errors.push(format!(
            "Snapshot format version {} is newer than supported {}",
            header.snapshot_format_version, current.snapshot_format_version
        ));
    }

    if header.major > current.major {
        errors.push(format!(
            "Snapshot from newer major version {}.{}.{} (current: {}.{}.{})",
            header.major, header.minor, header.patch,
            current.major, current.minor, current.patch,
        ));
    }

    if header.wal_format_version > current.wal_format_version {
        warnings.push(format!(
            "Snapshot WAL format {} is newer than current {}",
            header.wal_format_version, current.wal_format_version
        ));
    }

    if !errors.is_empty() {
        CompatibilityResult::Incompatible(errors)
    } else if !warnings.is_empty() {
        CompatibilityResult::CompatibleWithWarnings(warnings)
    } else {
        CompatibilityResult::Compatible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_version_header() {
        let h = VersionHeader::current();
        assert_eq!(h.major, CEDAR_VERSION_MAJOR);
        assert_eq!(h.minor, CEDAR_VERSION_MINOR);
        assert_eq!(h.patch, CEDAR_VERSION_PATCH);
        assert_eq!(h.wal_format_version, crate::wal::WAL_FORMAT_VERSION);
        assert_eq!(h.snapshot_format_version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(h.version_string(), "0.2.0");
    }

    #[test]
    fn test_same_version_compatible() {
        let v = VersionHeader::current();
        let result = check_rolling_upgrade_compatibility(&v, &v);
        assert_eq!(result, CompatibilityResult::Compatible);
    }

    #[test]
    fn test_major_version_mismatch_incompatible() {
        let local = VersionHeader::current();
        let mut remote = VersionHeader::current();
        remote.major = 1;
        let result = check_rolling_upgrade_compatibility(&local, &remote);
        assert!(matches!(result, CompatibilityResult::Incompatible(_)));
    }

    #[test]
    fn test_minor_version_difference_warning() {
        let local = VersionHeader::current();
        let mut remote = VersionHeader::current();
        remote.minor = local.minor + 1;
        let result = check_rolling_upgrade_compatibility(&local, &remote);
        assert!(matches!(result, CompatibilityResult::CompatibleWithWarnings(_)));
        assert!(result.is_compatible());
    }

    #[test]
    fn test_wal_format_mismatch_warning() {
        let local = VersionHeader::current();
        let mut remote = VersionHeader::current();
        remote.wal_format_version += 1;
        let result = check_rolling_upgrade_compatibility(&local, &remote);
        assert!(matches!(result, CompatibilityResult::CompatibleWithWarnings(_)));
    }

    #[test]
    fn test_below_minimum_version_incompatible() {
        let local = VersionHeader::current();
        let remote = VersionHeader {
            major: 0,
            minor: 0,
            patch: 1,
            wal_format_version: 1,
            snapshot_format_version: 1,
        };
        let result = check_rolling_upgrade_compatibility(&local, &remote);
        assert!(!result.is_compatible());
    }

    #[test]
    fn test_snapshot_same_version_compatible() {
        let h = VersionHeader::current();
        let result = check_snapshot_compatibility(&h);
        assert_eq!(result, CompatibilityResult::Compatible);
    }

    #[test]
    fn test_snapshot_newer_format_incompatible() {
        let mut h = VersionHeader::current();
        h.snapshot_format_version = 999;
        let result = check_snapshot_compatibility(&h);
        assert!(!result.is_compatible());
    }

    #[test]
    fn test_snapshot_newer_major_incompatible() {
        let mut h = VersionHeader::current();
        h.major = 99;
        let result = check_snapshot_compatibility(&h);
        assert!(!result.is_compatible());
    }

    #[test]
    fn test_compatibility_result_is_compatible() {
        assert!(CompatibilityResult::Compatible.is_compatible());
        assert!(CompatibilityResult::CompatibleWithWarnings(vec!["w".into()]).is_compatible());
        assert!(!CompatibilityResult::Incompatible(vec!["e".into()]).is_compatible());
    }
}
