//! P3-6: Advanced security infrastructure.
//!
//! Provides enterprise/cloud security capabilities:
//! - Encryption at rest configuration
//! - TLS transport configuration
//! - IP allowlist / access control
//! - KMS (Key Management Service) integration interface
//! - Immutable audit log verification

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Encryption-at-rest configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Whether encryption at rest is enabled.
    pub enabled: bool,
    /// Encryption algorithm (e.g. "AES-256-GCM").
    pub algorithm: String,
    /// Key identifier in the KMS (or local key path).
    pub key_id: String,
    /// Whether to encrypt WAL segments.
    pub encrypt_wal: bool,
    /// Whether to encrypt snapshot/checkpoint files.
    pub encrypt_snapshots: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: "AES-256-GCM".into(),
            key_id: String::new(),
            encrypt_wal: false,
            encrypt_snapshots: false,
        }
    }
}

/// TLS transport configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Whether TLS is enabled for client connections.
    pub enabled: bool,
    /// Path to the server certificate (PEM).
    pub cert_path: String,
    /// Path to the server private key (PEM).
    pub key_path: String,
    /// Path to the CA certificate for client verification (optional).
    pub ca_cert_path: Option<String>,
    /// Whether to require client certificates (mTLS).
    pub require_client_cert: bool,
    /// Minimum TLS version (e.g. "1.2", "1.3").
    pub min_version: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: None,
            require_client_cert: false,
            min_version: "1.2".into(),
        }
    }
}

/// KMS (Key Management Service) integration configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfig {
    /// KMS provider (e.g. "aws-kms", "gcp-kms", "azure-keyvault", "local").
    pub provider: String,
    /// Endpoint URL for the KMS.
    pub endpoint: String,
    /// Region for cloud KMS providers.
    pub region: String,
    /// Master key ARN / ID.
    pub master_key_id: String,
    /// Key rotation interval in days (0 = no auto-rotation).
    pub rotation_interval_days: u32,
}

impl Default for KmsConfig {
    fn default() -> Self {
        Self {
            provider: "local".into(),
            endpoint: String::new(),
            region: String::new(),
            master_key_id: String::new(),
            rotation_interval_days: 0,
        }
    }
}

/// Result of an IP allowlist check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpCheckResult {
    Allowed,
    Denied,
    /// Allowlist is disabled (all IPs allowed).
    Disabled,
}

/// Security manager — orchestrates encryption, TLS, IP control, and KMS.
pub struct SecurityManager {
    encryption: RwLock<EncryptionConfig>,
    tls: RwLock<TlsConfig>,
    kms: RwLock<KmsConfig>,
    /// IP allowlist. Empty = disabled (all allowed).
    ip_allowlist: RwLock<HashSet<IpAddr>>,
    /// Whether the IP allowlist is enabled.
    ip_allowlist_enabled: RwLock<bool>,
    /// Total connection attempts blocked by IP filter.
    blocked_by_ip: AtomicU64,
    /// Total connection attempts.
    total_connection_attempts: AtomicU64,
}

impl SecurityManager {
    pub fn new() -> Self {
        Self {
            encryption: RwLock::new(EncryptionConfig::default()),
            tls: RwLock::new(TlsConfig::default()),
            kms: RwLock::new(KmsConfig::default()),
            ip_allowlist: RwLock::new(HashSet::new()),
            ip_allowlist_enabled: RwLock::new(false),
            blocked_by_ip: AtomicU64::new(0),
            total_connection_attempts: AtomicU64::new(0),
        }
    }

    // ── Encryption ──

    pub fn set_encryption_config(&self, config: EncryptionConfig) {
        *self.encryption.write() = config;
    }

    pub fn encryption_config(&self) -> EncryptionConfig {
        self.encryption.read().clone()
    }

    pub fn is_encryption_enabled(&self) -> bool {
        self.encryption.read().enabled
    }

    // ── TLS ──

    pub fn set_tls_config(&self, config: TlsConfig) {
        *self.tls.write() = config;
    }

    pub fn tls_config(&self) -> TlsConfig {
        self.tls.read().clone()
    }

    pub fn is_tls_enabled(&self) -> bool {
        self.tls.read().enabled
    }

    // ── KMS ──

    pub fn set_kms_config(&self, config: KmsConfig) {
        *self.kms.write() = config;
    }

    pub fn kms_config(&self) -> KmsConfig {
        self.kms.read().clone()
    }

    // ── IP Allowlist ──

    /// Enable the IP allowlist. Only listed IPs will be allowed.
    pub fn enable_ip_allowlist(&self) {
        *self.ip_allowlist_enabled.write() = true;
    }

    /// Disable the IP allowlist. All IPs are allowed.
    pub fn disable_ip_allowlist(&self) {
        *self.ip_allowlist_enabled.write() = false;
    }

    /// Add an IP to the allowlist.
    pub fn add_allowed_ip(&self, ip: IpAddr) {
        self.ip_allowlist.write().insert(ip);
    }

    /// Remove an IP from the allowlist.
    pub fn remove_allowed_ip(&self, ip: IpAddr) {
        self.ip_allowlist.write().remove(&ip);
    }

    /// Set the full allowlist at once.
    pub fn set_allowlist(&self, ips: HashSet<IpAddr>) {
        *self.ip_allowlist.write() = ips;
    }

    /// Get the current allowlist.
    pub fn allowlist(&self) -> HashSet<IpAddr> {
        self.ip_allowlist.read().clone()
    }

    /// Check if an IP address is allowed to connect.
    pub fn check_ip(&self, ip: IpAddr) -> IpCheckResult {
        self.total_connection_attempts.fetch_add(1, Ordering::Relaxed);

        if !*self.ip_allowlist_enabled.read() {
            return IpCheckResult::Disabled;
        }

        let allowed = self.ip_allowlist.read().contains(&ip);
        if !allowed {
            self.blocked_by_ip.fetch_add(1, Ordering::Relaxed);
            IpCheckResult::Denied
        } else {
            IpCheckResult::Allowed
        }
    }

    // ── Metrics ──

    pub fn blocked_by_ip_count(&self) -> u64 {
        self.blocked_by_ip.load(Ordering::Relaxed)
    }

    pub fn total_connection_attempts(&self) -> u64 {
        self.total_connection_attempts.load(Ordering::Relaxed)
    }

    /// Summary for SHOW falcon.security.
    pub fn security_summary(&self) -> SecuritySummary {
        SecuritySummary {
            encryption_enabled: self.is_encryption_enabled(),
            encryption_algorithm: self.encryption.read().algorithm.clone(),
            tls_enabled: self.is_tls_enabled(),
            tls_min_version: self.tls.read().min_version.clone(),
            tls_require_client_cert: self.tls.read().require_client_cert,
            kms_provider: self.kms.read().provider.clone(),
            ip_allowlist_enabled: *self.ip_allowlist_enabled.read(),
            ip_allowlist_size: self.ip_allowlist.read().len(),
            blocked_by_ip: self.blocked_by_ip.load(Ordering::Relaxed),
            total_connection_attempts: self.total_connection_attempts.load(Ordering::Relaxed),
        }
    }
}

impl Default for SecurityManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of security configuration for observability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySummary {
    pub encryption_enabled: bool,
    pub encryption_algorithm: String,
    pub tls_enabled: bool,
    pub tls_min_version: String,
    pub tls_require_client_cert: bool,
    pub kms_provider: String,
    pub ip_allowlist_enabled: bool,
    pub ip_allowlist_size: usize,
    pub blocked_by_ip: u64,
    pub total_connection_attempts: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_default_security_disabled() {
        let mgr = SecurityManager::new();
        assert!(!mgr.is_encryption_enabled());
        assert!(!mgr.is_tls_enabled());
        let summary = mgr.security_summary();
        assert!(!summary.ip_allowlist_enabled);
    }

    #[test]
    fn test_enable_encryption() {
        let mgr = SecurityManager::new();
        mgr.set_encryption_config(EncryptionConfig {
            enabled: true,
            algorithm: "AES-256-GCM".into(),
            key_id: "master-key-1".into(),
            encrypt_wal: true,
            encrypt_snapshots: true,
        });
        assert!(mgr.is_encryption_enabled());
        assert_eq!(mgr.encryption_config().key_id, "master-key-1");
    }

    #[test]
    fn test_enable_tls() {
        let mgr = SecurityManager::new();
        mgr.set_tls_config(TlsConfig {
            enabled: true,
            cert_path: "/certs/server.pem".into(),
            key_path: "/certs/server-key.pem".into(),
            ca_cert_path: Some("/certs/ca.pem".into()),
            require_client_cert: true,
            min_version: "1.3".into(),
        });
        assert!(mgr.is_tls_enabled());
        let tls = mgr.tls_config();
        assert!(tls.require_client_cert);
        assert_eq!(tls.min_version, "1.3");
    }

    #[test]
    fn test_ip_allowlist_disabled_allows_all() {
        let mgr = SecurityManager::new();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(mgr.check_ip(ip), IpCheckResult::Disabled);
    }

    #[test]
    fn test_ip_allowlist_blocks_unlisted() {
        let mgr = SecurityManager::new();
        let allowed = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let denied = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        mgr.add_allowed_ip(allowed);
        mgr.enable_ip_allowlist();

        assert_eq!(mgr.check_ip(allowed), IpCheckResult::Allowed);
        assert_eq!(mgr.check_ip(denied), IpCheckResult::Denied);
        assert_eq!(mgr.blocked_by_ip_count(), 1);
    }

    #[test]
    fn test_ip_allowlist_set_and_remove() {
        let mgr = SecurityManager::new();
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        let mut ips = HashSet::new();
        ips.insert(ip1);
        ips.insert(ip2);
        mgr.set_allowlist(ips);
        mgr.enable_ip_allowlist();

        assert_eq!(mgr.check_ip(ip1), IpCheckResult::Allowed);
        mgr.remove_allowed_ip(ip1);
        assert_eq!(mgr.check_ip(ip1), IpCheckResult::Denied);
    }

    #[test]
    fn test_kms_config() {
        let mgr = SecurityManager::new();
        mgr.set_kms_config(KmsConfig {
            provider: "aws-kms".into(),
            endpoint: "https://kms.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
            master_key_id: "arn:aws:kms:us-east-1:123:key/abc".into(),
            rotation_interval_days: 90,
        });
        let kms = mgr.kms_config();
        assert_eq!(kms.provider, "aws-kms");
        assert_eq!(kms.rotation_interval_days, 90);
    }

    #[test]
    fn test_security_summary() {
        let mgr = SecurityManager::new();
        mgr.set_encryption_config(EncryptionConfig { enabled: true, ..Default::default() });
        mgr.set_tls_config(TlsConfig { enabled: true, ..Default::default() });
        mgr.enable_ip_allowlist();
        mgr.add_allowed_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        let summary = mgr.security_summary();
        assert!(summary.encryption_enabled);
        assert!(summary.tls_enabled);
        assert!(summary.ip_allowlist_enabled);
        assert_eq!(summary.ip_allowlist_size, 1);
    }
}
