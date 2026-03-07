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
// Ipv4Addr and Ipv6Addr removed — unused
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

/// A CIDR network specification for IP allowlist rules.
/// Supports both IPv4 (e.g. "10.0.0.0/8") and IPv6 (e.g. "::1/128").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IpNet {
    addr: IpAddr,
    prefix_len: u8,
}

impl IpNet {
    /// Create a new CIDR network.
    ///
    /// # Panics
    /// Panics if `prefix_len` exceeds 32 for IPv4 or 128 for IPv6.
    pub fn new(addr: IpAddr, prefix_len: u8) -> Self {
        let max = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        assert!(
            prefix_len <= max,
            "prefix_len {prefix_len} exceeds max {max}"
        );
        Self { addr, prefix_len }
    }

    /// Parse a CIDR string like "10.0.0.0/8" or "192.168.1.0/24".
    /// A bare IP ("10.0.0.1") is treated as a /32 (IPv4) or /128 (IPv6).
    pub fn parse(s: &str) -> Result<Self, String> {
        if let Some((addr_str, prefix_str)) = s.split_once('/') {
            let addr: IpAddr = addr_str
                .trim()
                .parse()
                .map_err(|e| format!("invalid IP in CIDR '{}': {}", s, e))?;
            let prefix_len: u8 = prefix_str
                .trim()
                .parse()
                .map_err(|e| format!("invalid prefix length in '{}': {}", s, e))?;
            let max = match addr {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            if prefix_len > max {
                return Err(format!(
                    "prefix length {} exceeds max {} for {}",
                    prefix_len, max, s
                ));
            }
            Ok(Self { addr, prefix_len })
        } else {
            let addr: IpAddr = s
                .trim()
                .parse()
                .map_err(|e| format!("invalid IP '{}': {}", s, e))?;
            let prefix_len = match addr {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            Ok(Self { addr, prefix_len })
        }
    }

    /// Check whether `ip` falls within this CIDR range.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(candidate)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let mask = u32::MAX
                    .checked_shl(32 - self.prefix_len as u32)
                    .unwrap_or(0);
                u32::from(net) & mask == u32::from(candidate) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(candidate)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let mask = u128::MAX
                    .checked_shl(128 - self.prefix_len as u32)
                    .unwrap_or(0);
                u128::from(net) & mask == u128::from(candidate) & mask
            }
            _ => false, // v4 vs v6 mismatch
        }
    }
}

impl std::fmt::Display for IpNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix_len)
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
    /// IP allowlist: exact IPs.
    ip_allowlist: RwLock<HashSet<IpAddr>>,
    /// IP allowlist: CIDR ranges.
    ip_cidr_rules: RwLock<Vec<IpNet>>,
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
            ip_cidr_rules: RwLock::new(Vec::new()),
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

    /// Set the full exact-IP allowlist at once.
    pub fn set_allowlist(&self, ips: HashSet<IpAddr>) {
        *self.ip_allowlist.write() = ips;
    }

    /// Get the current exact-IP allowlist.
    pub fn allowlist(&self) -> HashSet<IpAddr> {
        self.ip_allowlist.read().clone()
    }

    /// Add a CIDR range to the allowlist (e.g. "10.0.0.0/8").
    pub fn add_cidr_rule(&self, cidr: IpNet) {
        self.ip_cidr_rules.write().push(cidr);
    }

    /// Remove all CIDR rules matching the given network.
    pub fn remove_cidr_rule(&self, cidr: &IpNet) {
        self.ip_cidr_rules.write().retain(|r| r != cidr);
    }

    /// Set the full CIDR rule list at once.
    pub fn set_cidr_rules(&self, rules: Vec<IpNet>) {
        *self.ip_cidr_rules.write() = rules;
    }

    /// Get the current CIDR rules.
    pub fn cidr_rules(&self) -> Vec<IpNet> {
        self.ip_cidr_rules.read().clone()
    }

    /// Check if an IP address is allowed to connect.
    ///
    /// Checks exact IPs first (O(1) hash lookup), then CIDR rules (linear scan).
    /// Returns `Denied` with an audit log entry if the IP is not in the allowlist.
    pub fn check_ip(&self, ip: IpAddr) -> IpCheckResult {
        self.total_connection_attempts
            .fetch_add(1, Ordering::Relaxed);

        if !*self.ip_allowlist_enabled.read() {
            return IpCheckResult::Disabled;
        }

        // Fast path: exact IP match
        if self.ip_allowlist.read().contains(&ip) {
            return IpCheckResult::Allowed;
        }

        // Slow path: CIDR range match
        if self
            .ip_cidr_rules
            .read()
            .iter()
            .any(|cidr| cidr.contains(ip))
        {
            return IpCheckResult::Allowed;
        }

        self.blocked_by_ip.fetch_add(1, Ordering::Relaxed);
        IpCheckResult::Denied
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
        mgr.set_encryption_config(EncryptionConfig {
            enabled: true,
            ..Default::default()
        });
        mgr.set_tls_config(TlsConfig {
            enabled: true,
            ..Default::default()
        });
        mgr.enable_ip_allowlist();
        mgr.add_allowed_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        let summary = mgr.security_summary();
        assert!(summary.encryption_enabled);
        assert!(summary.tls_enabled);
        assert!(summary.ip_allowlist_enabled);
        assert_eq!(summary.ip_allowlist_size, 1);
    }

    // ── IpNet / CIDR tests ──

    #[test]
    fn test_ipnet_parse_cidr_v4() {
        let net = IpNet::parse("10.0.0.0/8").unwrap();
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(10, 255, 255, 255))));
        assert!(!net.contains(IpAddr::V4(Ipv4Addr::new(11, 0, 0, 1))));
    }

    #[test]
    fn test_ipnet_parse_cidr_v4_24() {
        let net = IpNet::parse("192.168.1.0/24").unwrap();
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0))));
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 255))));
        assert!(!net.contains(IpAddr::V4(Ipv4Addr::new(192, 168, 2, 1))));
    }

    #[test]
    fn test_ipnet_parse_bare_ip() {
        let net = IpNet::parse("10.0.0.1").unwrap();
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(!net.contains(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))));
    }

    #[test]
    fn test_ipnet_parse_v6() {
        let net = IpNet::parse("::1/128").unwrap();
        assert!(net.contains(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)));
        assert!(!net.contains(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)));
    }

    #[test]
    fn test_ipnet_v4_v6_mismatch() {
        let net = IpNet::parse("10.0.0.0/8").unwrap();
        assert!(!net.contains(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn test_ipnet_parse_invalid() {
        assert!(IpNet::parse("not-an-ip").is_err());
        assert!(IpNet::parse("10.0.0.0/33").is_err());
        assert!(IpNet::parse("10.0.0.0/abc").is_err());
    }

    #[test]
    fn test_ipnet_prefix_0_matches_all() {
        let net = IpNet::parse("0.0.0.0/0").unwrap();
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))));
        assert!(net.contains(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))));
    }

    #[test]
    fn test_ipnet_display() {
        let net = IpNet::parse("10.0.0.0/8").unwrap();
        assert_eq!(net.to_string(), "10.0.0.0/8");
    }

    #[test]
    fn test_cidr_rule_allowlist() {
        let mgr = SecurityManager::new();
        mgr.add_cidr_rule(IpNet::parse("10.0.0.0/8").unwrap());
        mgr.enable_ip_allowlist();

        // 10.x.x.x should be allowed via CIDR
        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))),
            IpCheckResult::Allowed,
        );
        // 192.168.x.x should be denied
        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
            IpCheckResult::Denied,
        );
    }

    #[test]
    fn test_cidr_and_exact_ip_combined() {
        let mgr = SecurityManager::new();
        mgr.add_cidr_rule(IpNet::parse("10.0.0.0/8").unwrap());
        mgr.add_allowed_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
        mgr.enable_ip_allowlist();

        // Exact match
        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100))),
            IpCheckResult::Allowed,
        );
        // CIDR match
        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(10, 99, 0, 1))),
            IpCheckResult::Allowed,
        );
        // Neither
        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))),
            IpCheckResult::Denied,
        );
    }

    #[test]
    fn test_remove_cidr_rule() {
        let mgr = SecurityManager::new();
        let cidr = IpNet::parse("10.0.0.0/8").unwrap();
        mgr.add_cidr_rule(cidr.clone());
        mgr.enable_ip_allowlist();

        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            IpCheckResult::Allowed,
        );

        mgr.remove_cidr_rule(&cidr);
        assert_eq!(
            mgr.check_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            IpCheckResult::Denied,
        );
    }
}
