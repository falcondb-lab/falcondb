//! v1.1.0 §3–6: Enterprise Security — AuthN/AuthZ v1, TLS Rotation,
//! Backup/Restore v1, Enterprise Audit Log.
//!
//! This module builds on existing RBAC (`falcon_common::security`),
//! TLS (`falcon_protocol_pg::tls`), and backup (`falcon_storage::backup`)
//! foundations to deliver enterprise-grade security:
//!
//! - **AuthN**: password / token / mTLS credential types
//! - **AuthZ**: RBAC with cluster / database / table scope
//! - **TLS**: full-chain encryption with certificate hot-reload
//! - **Backup**: enterprise backup orchestrator with S3/local targets
//! - **Audit**: tamper-proof, exportable, SIEM-ready audit log

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::{Mutex, RwLock};


// ═══════════════════════════════════════════════════════════════════════════
// §3 — AuthN: Credential Model
// ═══════════════════════════════════════════════════════════════════════════

/// Authentication credential type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialType {
    /// Password-based (SCRAM-SHA-256 or MD5).
    Password,
    /// Bearer token (JWT or opaque).
    Token,
    /// Mutual TLS (client certificate).
    MtlsCertificate,
}

impl fmt::Display for CredentialType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Password => write!(f, "PASSWORD"),
            Self::Token => write!(f, "TOKEN"),
            Self::MtlsCertificate => write!(f, "MTLS"),
        }
    }
}

/// A stored credential for a user.
#[derive(Debug, Clone)]
pub struct StoredCredential {
    pub user_id: u64,
    pub credential_type: CredentialType,
    /// For password: hash (SCRAM-SHA-256 or bcrypt).
    /// For token: hash of the token.
    /// For mTLS: SHA-256 fingerprint of the client cert.
    pub credential_hash: String,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub last_used_at: Option<u64>,
    pub is_revoked: bool,
}

/// Authentication request.
#[derive(Debug, Clone)]
pub struct AuthnRequest {
    pub username: String,
    pub credential_type: CredentialType,
    /// The raw credential (password, token, or cert fingerprint).
    pub credential: String,
    pub source_ip: String,
}

/// Authentication result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthnResult {
    Success { user_id: u64, session_token: String },
    Failed { reason: String },
    Locked { remaining_secs: u64 },
    CredentialExpired,
}

impl fmt::Display for AuthnResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success { user_id, .. } => write!(f, "SUCCESS user_id={user_id}"),
            Self::Failed { reason } => write!(f, "FAILED: {reason}"),
            Self::Locked { remaining_secs } => write!(f, "LOCKED {remaining_secs}s"),
            Self::CredentialExpired => write!(f, "EXPIRED"),
        }
    }
}

/// User record in the authentication system.
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub user_id: u64,
    pub username: String,
    pub credentials: Vec<StoredCredential>,
    pub is_active: bool,
    pub created_at: u64,
    pub last_login_at: Option<u64>,
    pub failed_login_count: u32,
    pub locked_until: Option<u64>,
}

/// Enterprise authentication manager.
pub struct AuthnManager {
    users: RwLock<HashMap<u64, UserRecord>>,
    username_index: RwLock<HashMap<String, u64>>,
    next_user_id: AtomicU64,
    lockout_threshold: u32,
    lockout_duration_secs: u64,
    pub metrics: AuthnMetrics,
}

#[derive(Debug, Default)]
pub struct AuthnMetrics {
    pub auth_attempts: AtomicU64,
    pub auth_successes: AtomicU64,
    pub auth_failures: AtomicU64,
    pub auth_lockouts: AtomicU64,
    pub credential_expirations: AtomicU64,
}

impl AuthnManager {
    pub fn new(lockout_threshold: u32, lockout_duration_secs: u64) -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            username_index: RwLock::new(HashMap::new()),
            next_user_id: AtomicU64::new(1),
            lockout_threshold,
            lockout_duration_secs,
            metrics: AuthnMetrics::default(),
        }
    }

    /// Create a user with a password credential.
    pub fn create_user(&self, username: &str, password_hash: &str) -> u64 {
        let user_id = self.next_user_id.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cred = StoredCredential {
            user_id,
            credential_type: CredentialType::Password,
            credential_hash: password_hash.to_owned(),
            created_at: now,
            expires_at: None,
            last_used_at: None,
            is_revoked: false,
        };
        let user = UserRecord {
            user_id,
            username: username.to_owned(),
            credentials: vec![cred],
            is_active: true,
            created_at: now,
            last_login_at: None,
            failed_login_count: 0,
            locked_until: None,
        };
        self.users.write().insert(user_id, user);
        self.username_index.write().insert(username.to_owned(), user_id);
        user_id
    }

    /// Add a token credential to a user.
    pub fn add_token(&self, user_id: u64, token_hash: &str, expires_at: Option<u64>) -> bool {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(&user_id) {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            user.credentials.push(StoredCredential {
                user_id,
                credential_type: CredentialType::Token,
                credential_hash: token_hash.to_owned(),
                created_at: now,
                expires_at,
                last_used_at: None,
                is_revoked: false,
            });
            true
        } else {
            false
        }
    }

    /// Add an mTLS certificate fingerprint to a user.
    pub fn add_mtls_cert(&self, user_id: u64, cert_fingerprint: &str) -> bool {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(&user_id) {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            user.credentials.push(StoredCredential {
                user_id,
                credential_type: CredentialType::MtlsCertificate,
                credential_hash: cert_fingerprint.to_owned(),
                created_at: now,
                expires_at: None,
                last_used_at: None,
                is_revoked: false,
            });
            true
        } else {
            false
        }
    }

    /// Authenticate a user.
    pub fn authenticate(&self, request: &AuthnRequest) -> AuthnResult {
        self.metrics.auth_attempts.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Look up user
        let user_id = {
            let idx = self.username_index.read();
            if let Some(id) = idx.get(&request.username) { *id } else {
                self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
                return AuthnResult::Failed {
                    reason: "user not found".into(),
                };
            }
        };

        let mut users = self.users.write();
        let user = if let Some(u) = users.get_mut(&user_id) { u } else {
            self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
            return AuthnResult::Failed {
                reason: "user not found".into(),
            };
        };

        // Check lockout
        if let Some(locked) = user.locked_until {
            if now < locked {
                self.metrics.auth_lockouts.fetch_add(1, Ordering::Relaxed);
                return AuthnResult::Locked {
                    remaining_secs: locked - now,
                };
            }
            user.locked_until = None;
            user.failed_login_count = 0;
        }

        if !user.is_active {
            self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
            return AuthnResult::Failed {
                reason: "account disabled".into(),
            };
        }

        // Find matching credential
        let matched = user.credentials.iter_mut().find(|c| {
            c.credential_type == request.credential_type
                && c.credential_hash == request.credential
                && !c.is_revoked
        });

        if let Some(cred) = matched {
            // Check expiration
            if let Some(exp) = cred.expires_at {
                if now > exp {
                    self.metrics.credential_expirations.fetch_add(1, Ordering::Relaxed);
                    return AuthnResult::CredentialExpired;
                }
            }
            cred.last_used_at = Some(now);
            user.last_login_at = Some(now);
            user.failed_login_count = 0;
            self.metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
            // Generate session token (simplified — in production use JWT)
            let session = format!("session-{user_id}-{now}");
            AuthnResult::Success {
                user_id,
                session_token: session,
            }
        } else {
            user.failed_login_count += 1;
            if user.failed_login_count >= self.lockout_threshold {
                user.locked_until = Some(now + self.lockout_duration_secs);
                self.metrics.auth_lockouts.fetch_add(1, Ordering::Relaxed);
            }
            self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
            AuthnResult::Failed {
                reason: "invalid credentials".into(),
            }
        }
    }

    /// Revoke a credential.
    pub fn revoke_credential(&self, user_id: u64, cred_type: CredentialType, hash: &str) -> bool {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(&user_id) {
            for c in &mut user.credentials {
                if c.credential_type == cred_type && c.credential_hash == hash {
                    c.is_revoked = true;
                    return true;
                }
            }
        }
        false
    }

    /// Disable a user account.
    pub fn disable_user(&self, user_id: u64) -> bool {
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(&user_id) {
            user.is_active = false;
            true
        } else {
            false
        }
    }

    /// Get user by username.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let idx = self.username_index.read();
        let id = idx.get(username)?;
        self.users.read().get(id).cloned()
    }

    /// Get user count.
    pub fn user_count(&self) -> usize {
        self.users.read().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3b — AuthZ: Enterprise RBAC Scope Model
// ═══════════════════════════════════════════════════════════════════════════

/// RBAC scope level — which level the permission applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RbacScope {
    /// Cluster-wide (e.g., CREATE DATABASE, MANAGE CLUSTER).
    Cluster,
    /// Database-level (e.g., CONNECT, CREATE TABLE).
    Database,
    /// Table-level (e.g., SELECT, INSERT, UPDATE, DELETE).
    Table,
}

impl fmt::Display for RbacScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster => write!(f, "CLUSTER"),
            Self::Database => write!(f, "DATABASE"),
            Self::Table => write!(f, "TABLE"),
        }
    }
}

/// Enterprise permission — more granular than PG Privilege.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EnterprisePermission {
    // Cluster-level
    ManageCluster,
    CreateDatabase,
    ManageUsers,
    ViewAuditLog,
    ManageBackups,
    // Database-level
    Connect,
    CreateTable,
    DropTable,
    // Table-level
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
}

impl fmt::Display for EnterprisePermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManageCluster => write!(f, "MANAGE_CLUSTER"),
            Self::CreateDatabase => write!(f, "CREATE_DATABASE"),
            Self::ManageUsers => write!(f, "MANAGE_USERS"),
            Self::ViewAuditLog => write!(f, "VIEW_AUDIT_LOG"),
            Self::ManageBackups => write!(f, "MANAGE_BACKUPS"),
            Self::Connect => write!(f, "CONNECT"),
            Self::CreateTable => write!(f, "CREATE_TABLE"),
            Self::DropTable => write!(f, "DROP_TABLE"),
            Self::Select => write!(f, "SELECT"),
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
            Self::Truncate => write!(f, "TRUNCATE"),
        }
    }
}

/// An RBAC grant entry.
#[derive(Debug, Clone)]
pub struct RbacGrant {
    pub role_id: u64,
    pub scope: RbacScope,
    /// Resource name (e.g., database name, table name, "*" for all).
    pub resource: String,
    pub permission: EnterprisePermission,
    pub granted_by: String,
    pub granted_at: u64,
}

/// RBAC check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RbacCheckResult {
    Allowed,
    Denied { scope: RbacScope, permission: String, resource: String },
}

/// Enterprise RBAC manager — extends existing RoleCatalog with scoped permissions.
pub struct EnterpriseRbac {
    grants: RwLock<Vec<RbacGrant>>,
    /// Role→superuser mapping.
    superuser_roles: RwLock<HashSet<u64>>,
    pub metrics: RbacMetrics,
}

#[derive(Debug, Default)]
pub struct RbacMetrics {
    pub checks: AtomicU64,
    pub grants_count: AtomicU64,
    pub denials: AtomicU64,
}

impl Default for EnterpriseRbac {
    fn default() -> Self {
        Self::new()
    }
}

impl EnterpriseRbac {
    pub fn new() -> Self {
        let mut su = HashSet::new();
        su.insert(0); // superuser role_id=0
        Self {
            grants: RwLock::new(Vec::new()),
            superuser_roles: RwLock::new(su),
            metrics: RbacMetrics::default(),
        }
    }

    /// Grant a permission.
    pub fn grant(
        &self,
        role_id: u64,
        scope: RbacScope,
        resource: &str,
        permission: EnterprisePermission,
        granted_by: &str,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.grants.write().push(RbacGrant {
            role_id,
            scope,
            resource: resource.to_owned(),
            permission,
            granted_by: granted_by.to_owned(),
            granted_at: now,
        });
        self.metrics.grants_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Revoke a permission.
    pub fn revoke(
        &self,
        role_id: u64,
        scope: RbacScope,
        resource: &str,
        permission: &EnterprisePermission,
    ) -> bool {
        let mut grants = self.grants.write();
        let before = grants.len();
        grants.retain(|g| {
            !(g.role_id == role_id
                && g.scope == scope
                && g.resource == resource
                && g.permission == *permission)
        });
        grants.len() < before
    }

    /// Mark a role as superuser.
    pub fn set_superuser(&self, role_id: u64) {
        self.superuser_roles.write().insert(role_id);
    }

    /// Check if a role has a permission on a resource.
    pub fn check(
        &self,
        role_id: u64,
        effective_roles: &HashSet<u64>,
        scope: RbacScope,
        resource: &str,
        permission: &EnterprisePermission,
    ) -> RbacCheckResult {
        self.metrics.checks.fetch_add(1, Ordering::Relaxed);

        // Superuser bypass
        let su = self.superuser_roles.read();
        if su.contains(&role_id) || effective_roles.iter().any(|r| su.contains(r)) {
            return RbacCheckResult::Allowed;
        }

        let grants = self.grants.read();
        let allowed = grants.iter().any(|g| {
            effective_roles.contains(&g.role_id)
                && g.scope == scope
                && (g.resource == resource || g.resource == "*")
                && g.permission == *permission
        });

        if allowed {
            RbacCheckResult::Allowed
        } else {
            self.metrics.denials.fetch_add(1, Ordering::Relaxed);
            RbacCheckResult::Denied {
                scope,
                permission: permission.to_string(),
                resource: resource.to_owned(),
            }
        }
    }

    /// List grants for a role.
    pub fn grants_for_role(&self, role_id: u64) -> Vec<RbacGrant> {
        self.grants.read().iter()
            .filter(|g| g.role_id == role_id)
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — TLS Certificate Manager with Hot-Reload
// ═══════════════════════════════════════════════════════════════════════════

/// TLS link type — which connection this cert covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsLinkType {
    ClientToGateway,
    GatewayToDataNode,
    DataNodeToReplica,
    DataNodeToController,
}

impl fmt::Display for TlsLinkType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientToGateway => write!(f, "CLIENT→GATEWAY"),
            Self::GatewayToDataNode => write!(f, "GATEWAY→DATA"),
            Self::DataNodeToReplica => write!(f, "DATA→REPLICA"),
            Self::DataNodeToController => write!(f, "DATA→CONTROLLER"),
        }
    }
}

/// Certificate record.
#[derive(Debug, Clone)]
pub struct CertificateRecord {
    pub link_type: TlsLinkType,
    pub cert_path: String,
    pub key_path: String,
    pub ca_path: Option<String>,
    pub fingerprint: String,
    pub not_before: u64,
    pub not_after: u64,
    pub loaded_at: u64,
    pub is_active: bool,
}

/// TLS certificate manager — manages certificates for all link types.
pub struct CertificateManager {
    certs: RwLock<HashMap<TlsLinkType, CertificateRecord>>,
    rotation_history: Mutex<VecDeque<CertRotationEvent>>,
    pub tls_required: AtomicBool,
    pub metrics: CertMetrics,
}

/// Certificate rotation event.
#[derive(Debug, Clone)]
pub struct CertRotationEvent {
    pub link_type: TlsLinkType,
    pub old_fingerprint: String,
    pub new_fingerprint: String,
    pub rotated_at: u64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Default)]
pub struct CertMetrics {
    pub rotations_attempted: AtomicU64,
    pub rotations_succeeded: AtomicU64,
    pub rotations_failed: AtomicU64,
    pub certs_loaded: AtomicU64,
    pub expired_certs_detected: AtomicU64,
}

impl CertificateManager {
    pub fn new(tls_required: bool) -> Self {
        Self {
            certs: RwLock::new(HashMap::new()),
            rotation_history: Mutex::new(VecDeque::with_capacity(100)),
            tls_required: AtomicBool::new(tls_required),
            metrics: CertMetrics::default(),
        }
    }

    /// Load a certificate for a link type.
    pub fn load_cert(&self, record: CertificateRecord) {
        self.metrics.certs_loaded.fetch_add(1, Ordering::Relaxed);
        self.certs.write().insert(record.link_type, record);
    }

    /// Rotate a certificate (hot-reload). Returns success.
    pub fn rotate_cert(
        &self,
        link_type: TlsLinkType,
        new_cert_path: &str,
        new_key_path: &str,
        new_fingerprint: &str,
        not_before: u64,
        not_after: u64,
    ) -> bool {
        self.metrics.rotations_attempted.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let old_fingerprint = {
            let certs = self.certs.read();
            certs.get(&link_type)
                .map(|c| c.fingerprint.clone())
                .unwrap_or_default()
        };

        let new_record = CertificateRecord {
            link_type,
            cert_path: new_cert_path.to_owned(),
            key_path: new_key_path.to_owned(),
            ca_path: None,
            fingerprint: new_fingerprint.to_owned(),
            not_before,
            not_after,
            loaded_at: now,
            is_active: true,
        };

        self.certs.write().insert(link_type, new_record);
        self.metrics.rotations_succeeded.fetch_add(1, Ordering::Relaxed);

        let event = CertRotationEvent {
            link_type,
            old_fingerprint,
            new_fingerprint: new_fingerprint.to_owned(),
            rotated_at: now,
            success: true,
            error: None,
        };
        let mut hist = self.rotation_history.lock();
        if hist.len() >= 100 {
            hist.pop_front();
        }
        hist.push_back(event);
        true
    }

    /// Check for expiring certificates. Returns certs expiring within `within_secs`.
    pub fn expiring_certs(&self, within_secs: u64) -> Vec<(TlsLinkType, u64)> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let threshold = now.saturating_add(within_secs);
        let certs = self.certs.read();
        certs.iter()
            .filter(|(_, c)| c.not_after < threshold && c.is_active)
            .map(|(lt, c)| (*lt, c.not_after))
            .collect()
    }

    /// Get certificate for a link type.
    pub fn get_cert(&self, link_type: TlsLinkType) -> Option<CertificateRecord> {
        self.certs.read().get(&link_type).cloned()
    }

    /// Get rotation history.
    pub fn rotation_history(&self, limit: usize) -> Vec<CertRotationEvent> {
        self.rotation_history.lock().iter().rev().take(limit).cloned().collect()
    }

    /// Check all links have valid certs.
    pub fn all_links_covered(&self) -> Vec<TlsLinkType> {
        let required = vec![
            TlsLinkType::ClientToGateway,
            TlsLinkType::GatewayToDataNode,
            TlsLinkType::DataNodeToReplica,
            TlsLinkType::DataNodeToController,
        ];
        let certs = self.certs.read();
        required.into_iter()
            .filter(|lt| !certs.contains_key(lt))
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Enterprise Backup Orchestrator
// ═══════════════════════════════════════════════════════════════════════════

/// Backup storage target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupTarget {
    Local { path: String },
    S3 { bucket: String, prefix: String, region: String },
    Oss { bucket: String, prefix: String, endpoint: String },
}

impl fmt::Display for BackupTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local { path } => write!(f, "local:{path}"),
            Self::S3 { bucket, prefix, region } =>
                write!(f, "s3://{bucket}/{prefix} ({region})"),
            Self::Oss { bucket, prefix, endpoint } =>
                write!(f, "oss://{bucket}/{prefix} ({endpoint})"),
        }
    }
}

/// Enterprise backup type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterpriseBackupType {
    Full,
    Incremental,
    WalArchive,
}

impl fmt::Display for EnterpriseBackupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "FULL"),
            Self::Incremental => write!(f, "INCREMENTAL"),
            Self::WalArchive => write!(f, "WAL_ARCHIVE"),
        }
    }
}

/// Backup job status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupJobStatus {
    Scheduled,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

impl fmt::Display for BackupJobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scheduled => write!(f, "SCHEDULED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed(e) => write!(f, "FAILED: {e}"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Backup job record.
#[derive(Debug, Clone)]
pub struct BackupJob {
    pub job_id: u64,
    pub backup_type: EnterpriseBackupType,
    pub target: BackupTarget,
    pub status: BackupJobStatus,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub bytes_written: u64,
    pub tables_backed_up: u32,
    pub wal_start_lsn: u64,
    pub wal_end_lsn: u64,
    pub checksum: u32,
    pub initiated_by: String,
}

/// Restore job record.
#[derive(Debug, Clone)]
pub struct RestoreJob {
    pub job_id: u64,
    pub source: BackupTarget,
    pub backup_id: u64,
    pub restore_type: RestoreType,
    pub status: BackupJobStatus,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub bytes_read: u64,
    pub tables_restored: u32,
    pub target_lsn: Option<u64>,
    pub target_timestamp: Option<u64>,
}

/// Restore target type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreType {
    /// Restore to latest.
    Latest,
    /// Restore to a specific LSN.
    ToLsn,
    /// Restore to a specific timestamp (PITR).
    ToTimestamp,
    /// Restore to a new cluster.
    NewCluster,
}

impl fmt::Display for RestoreType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => write!(f, "LATEST"),
            Self::ToLsn => write!(f, "TO_LSN"),
            Self::ToTimestamp => write!(f, "TO_TIMESTAMP"),
            Self::NewCluster => write!(f, "NEW_CLUSTER"),
        }
    }
}

/// Enterprise backup orchestrator.
pub struct BackupOrchestrator {
    backup_jobs: RwLock<Vec<BackupJob>>,
    restore_jobs: RwLock<Vec<RestoreJob>>,
    next_job_id: AtomicU64,
    pub retention_days: u32,
    pub metrics: BackupOrchestratorMetrics,
}

#[derive(Debug, Default)]
pub struct BackupOrchestratorMetrics {
    pub backups_started: AtomicU64,
    pub backups_completed: AtomicU64,
    pub backups_failed: AtomicU64,
    pub restores_started: AtomicU64,
    pub restores_completed: AtomicU64,
    pub total_bytes_backed_up: AtomicU64,
}

impl BackupOrchestrator {
    pub fn new(retention_days: u32) -> Self {
        Self {
            backup_jobs: RwLock::new(Vec::new()),
            restore_jobs: RwLock::new(Vec::new()),
            next_job_id: AtomicU64::new(1),
            retention_days,
            metrics: BackupOrchestratorMetrics::default(),
        }
    }

    /// Schedule a backup job.
    pub fn schedule_backup(
        &self,
        backup_type: EnterpriseBackupType,
        target: BackupTarget,
        initiated_by: &str,
    ) -> u64 {
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let job = BackupJob {
            job_id,
            backup_type,
            target,
            status: BackupJobStatus::Scheduled,
            started_at: None,
            completed_at: None,
            bytes_written: 0,
            tables_backed_up: 0,
            wal_start_lsn: 0,
            wal_end_lsn: 0,
            checksum: 0,
            initiated_by: initiated_by.to_owned(),
        };
        self.backup_jobs.write().push(job);
        self.metrics.backups_started.fetch_add(1, Ordering::Relaxed);
        job_id
    }

    /// Start a backup job (transition from Scheduled to Running).
    pub fn start_backup(&self, job_id: u64, wal_start_lsn: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut jobs = self.backup_jobs.write();
        if let Some(job) = jobs.iter_mut().find(|j| j.job_id == job_id) {
            job.status = BackupJobStatus::Running;
            job.started_at = Some(now);
            job.wal_start_lsn = wal_start_lsn;
            true
        } else {
            false
        }
    }

    /// Complete a backup job.
    pub fn complete_backup(
        &self,
        job_id: u64,
        bytes_written: u64,
        tables: u32,
        wal_end_lsn: u64,
        checksum: u32,
    ) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut jobs = self.backup_jobs.write();
        if let Some(job) = jobs.iter_mut().find(|j| j.job_id == job_id) {
            job.status = BackupJobStatus::Completed;
            job.completed_at = Some(now);
            job.bytes_written = bytes_written;
            job.tables_backed_up = tables;
            job.wal_end_lsn = wal_end_lsn;
            job.checksum = checksum;
            self.metrics.backups_completed.fetch_add(1, Ordering::Relaxed);
            self.metrics.total_bytes_backed_up.fetch_add(bytes_written, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Fail a backup job.
    pub fn fail_backup(&self, job_id: u64, error: &str) -> bool {
        let mut jobs = self.backup_jobs.write();
        if let Some(job) = jobs.iter_mut().find(|j| j.job_id == job_id) {
            job.status = BackupJobStatus::Failed(error.to_owned());
            self.metrics.backups_failed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Schedule a restore job.
    pub fn schedule_restore(
        &self,
        source: BackupTarget,
        backup_id: u64,
        restore_type: RestoreType,
        target_lsn: Option<u64>,
        target_timestamp: Option<u64>,
    ) -> u64 {
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let job = RestoreJob {
            job_id,
            source,
            backup_id,
            restore_type,
            status: BackupJobStatus::Scheduled,
            started_at: None,
            completed_at: None,
            bytes_read: 0,
            tables_restored: 0,
            target_lsn,
            target_timestamp,
        };
        self.restore_jobs.write().push(job);
        self.metrics.restores_started.fetch_add(1, Ordering::Relaxed);
        job_id
    }

    /// Complete a restore job.
    pub fn complete_restore(&self, job_id: u64, bytes_read: u64, tables: u32) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut jobs = self.restore_jobs.write();
        if let Some(job) = jobs.iter_mut().find(|j| j.job_id == job_id) {
            job.status = BackupJobStatus::Completed;
            job.completed_at = Some(now);
            job.bytes_read = bytes_read;
            job.tables_restored = tables;
            self.metrics.restores_completed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get backup history.
    pub fn backup_history(&self, limit: usize) -> Vec<BackupJob> {
        self.backup_jobs.read().iter().rev().take(limit).cloned().collect()
    }

    /// Get latest completed full backup.
    pub fn latest_full_backup(&self) -> Option<BackupJob> {
        self.backup_jobs.read().iter().rev()
            .find(|j| j.backup_type == EnterpriseBackupType::Full
                && j.status == BackupJobStatus::Completed)
            .cloned()
    }

    /// Get a backup job by ID.
    pub fn get_backup_job(&self, job_id: u64) -> Option<BackupJob> {
        self.backup_jobs.read().iter().find(|j| j.job_id == job_id).cloned()
    }

    /// Get a restore job by ID.
    pub fn get_restore_job(&self, job_id: u64) -> Option<RestoreJob> {
        self.restore_jobs.read().iter().find(|j| j.job_id == job_id).cloned()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Enterprise Audit Log (tamper-proof, SIEM-ready)
// ═══════════════════════════════════════════════════════════════════════════

/// Enterprise audit event category.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditCategory {
    Authentication,
    Authorization,
    DataAccess,
    SchemaChange,
    PrivilegeChange,
    OpsOperation,
    TopologyChange,
    SecurityEvent,
    BackupRestore,
    ConfigChange,
}

impl fmt::Display for AuditCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authentication => write!(f, "AUTHN"),
            Self::Authorization => write!(f, "AUTHZ"),
            Self::DataAccess => write!(f, "DATA_ACCESS"),
            Self::SchemaChange => write!(f, "SCHEMA_CHANGE"),
            Self::PrivilegeChange => write!(f, "PRIVILEGE_CHANGE"),
            Self::OpsOperation => write!(f, "OPS"),
            Self::TopologyChange => write!(f, "TOPOLOGY"),
            Self::SecurityEvent => write!(f, "SECURITY"),
            Self::BackupRestore => write!(f, "BACKUP_RESTORE"),
            Self::ConfigChange => write!(f, "CONFIG_CHANGE"),
        }
    }
}

/// Severity level for audit events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuditSeverity {
    Info,
    Warning,
    Critical,
}

impl fmt::Display for AuditSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "INFO"),
            Self::Warning => write!(f, "WARN"),
            Self::Critical => write!(f, "CRIT"),
        }
    }
}

/// Enterprise audit event.
#[derive(Debug, Clone)]
pub struct EnterpriseAuditEvent {
    pub id: u64,
    pub timestamp: u64,
    pub category: AuditCategory,
    pub severity: AuditSeverity,
    pub actor: String,
    pub source_ip: String,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub details: String,
    /// HMAC of the event for tamper detection.
    pub integrity_hash: String,
}

/// Enterprise audit log — tamper-proof, exportable.
pub struct EnterpriseAuditLog {
    events: Mutex<VecDeque<EnterpriseAuditEvent>>,
    capacity: usize,
    next_id: AtomicU64,
    /// Simple HMAC key for integrity (in production, use proper HMAC).
    hmac_key: String,
    /// Chain hash: each event's hash includes the previous event's hash.
    last_hash: Mutex<String>,
    pub metrics: EnterpriseAuditMetrics,
}

#[derive(Debug, Default)]
pub struct EnterpriseAuditMetrics {
    pub events_recorded: AtomicU64,
    pub events_exported: AtomicU64,
    pub integrity_checks: AtomicU64,
    pub integrity_failures: AtomicU64,
}

impl EnterpriseAuditLog {
    pub fn new(capacity: usize, hmac_key: &str) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            next_id: AtomicU64::new(1),
            hmac_key: hmac_key.to_owned(),
            last_hash: Mutex::new("genesis".to_owned()),
            metrics: EnterpriseAuditMetrics::default(),
        }
    }

    /// Compute a simple hash for integrity.
    fn compute_hash(&self, event_str: &str, prev_hash: &str) -> String {
        // Simple hash: djb2(key + prev_hash + event_str)
        let input = format!("{}{}{}", self.hmac_key, prev_hash, event_str);
        let mut h: u64 = 5381;
        for b in input.bytes() {
            h = h.wrapping_mul(33).wrapping_add(u64::from(b));
        }
        format!("{h:016x}")
    }

    /// Record an audit event.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        category: AuditCategory,
        severity: AuditSeverity,
        actor: &str,
        source_ip: &str,
        action: &str,
        resource: &str,
        outcome: &str,
        details: &str,
    ) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let event_str = format!(
            "{id}|{timestamp}|{category}|{actor}|{action}|{resource}|{outcome}|{details}"
        );
        // Hold both locks to guarantee hash-chain order matches event order.
        let mut events = self.events.lock();
        let hash = {
            let mut last = self.last_hash.lock();
            let h = self.compute_hash(&event_str, &last);
            *last = h.clone();
            h
        };

        let event = EnterpriseAuditEvent {
            id,
            timestamp,
            category,
            severity,
            actor: actor.to_owned(),
            source_ip: source_ip.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
            outcome: outcome.to_owned(),
            details: details.to_owned(),
            integrity_hash: hash,
        };

        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
        self.metrics.events_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Verify integrity of the audit chain.
    pub fn verify_integrity(&self) -> bool {
        self.metrics.integrity_checks.fetch_add(1, Ordering::Relaxed);
        let events = self.events.lock();
        let mut prev_hash = "genesis".to_owned();
        for event in events.iter() {
            let event_str = format!(
                "{}|{}|{}|{}|{}|{}|{}|{}",
                event.id, event.timestamp, event.category, event.actor,
                event.action, event.resource, event.outcome, event.details
            );
            let expected = self.compute_hash(&event_str, &prev_hash);
            if event.integrity_hash != expected {
                self.metrics.integrity_failures.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            prev_hash = event.integrity_hash.clone();
        }
        true
    }

    /// Export events in JSON-lines format (SIEM-ready).
    pub fn export_jsonl(&self, since_id: u64) -> Vec<String> {
        let events = self.events.lock();
        let result: Vec<String> = events.iter()
            .filter(|e| e.id >= since_id)
            .map(|e| {
                format!(
                    r#"{{"id":{},"ts":{},"cat":"{}","sev":"{}","actor":"{}","ip":"{}","action":"{}","resource":"{}","outcome":"{}","details":"{}","hash":"{}"}}"#,
                    e.id, e.timestamp, e.category, e.severity,
                    e.actor, e.source_ip, e.action, e.resource,
                    e.outcome, e.details, e.integrity_hash
                )
            })
            .collect();
        self.metrics.events_exported.fetch_add(result.len() as u64, Ordering::Relaxed);
        result
    }

    /// Get recent events.
    pub fn recent(&self, count: usize) -> Vec<EnterpriseAuditEvent> {
        self.events.lock().iter().rev().take(count).cloned().collect()
    }

    /// Total events recorded.
    pub fn total(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed) - 1
    }

    /// Query events by category.
    pub fn query_by_category(&self, category: &AuditCategory, limit: usize) -> Vec<EnterpriseAuditEvent> {
        self.events.lock().iter().rev()
            .filter(|e| e.category == *category)
            .take(limit)
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- AuthN Tests --

    #[test]
    fn test_create_user_and_authenticate() {
        let mgr = AuthnManager::new(5, 300);
        let uid = mgr.create_user("alice", "hash_abc");
        let result = mgr.authenticate(&AuthnRequest {
            username: "alice".into(),
            credential_type: CredentialType::Password,
            credential: "hash_abc".into(),
            source_ip: "127.0.0.1".into(),
        });
        assert!(matches!(result, AuthnResult::Success { .. }));
    }

    #[test]
    fn test_auth_failure() {
        let mgr = AuthnManager::new(5, 300);
        mgr.create_user("bob", "correct");
        let result = mgr.authenticate(&AuthnRequest {
            username: "bob".into(),
            credential_type: CredentialType::Password,
            credential: "wrong".into(),
            source_ip: "127.0.0.1".into(),
        });
        assert!(matches!(result, AuthnResult::Failed { .. }));
    }

    #[test]
    fn test_auth_lockout() {
        let mgr = AuthnManager::new(3, 300);
        mgr.create_user("eve", "secret");
        let bad = AuthnRequest {
            username: "eve".into(),
            credential_type: CredentialType::Password,
            credential: "wrong".into(),
            source_ip: "1.2.3.4".into(),
        };
        for _ in 0..3 {
            mgr.authenticate(&bad);
        }
        let result = mgr.authenticate(&bad);
        assert!(matches!(result, AuthnResult::Locked { .. }));
    }

    #[test]
    fn test_token_auth() {
        let mgr = AuthnManager::new(5, 300);
        let uid = mgr.create_user("svc", "pw");
        mgr.add_token(uid, "token_hash_123", None);
        let result = mgr.authenticate(&AuthnRequest {
            username: "svc".into(),
            credential_type: CredentialType::Token,
            credential: "token_hash_123".into(),
            source_ip: "10.0.0.1".into(),
        });
        assert!(matches!(result, AuthnResult::Success { .. }));
    }

    #[test]
    fn test_mtls_auth() {
        let mgr = AuthnManager::new(5, 300);
        let uid = mgr.create_user("node1", "pw");
        mgr.add_mtls_cert(uid, "sha256:abcdef");
        let result = mgr.authenticate(&AuthnRequest {
            username: "node1".into(),
            credential_type: CredentialType::MtlsCertificate,
            credential: "sha256:abcdef".into(),
            source_ip: "10.0.0.2".into(),
        });
        assert!(matches!(result, AuthnResult::Success { .. }));
    }

    #[test]
    fn test_credential_revocation() {
        let mgr = AuthnManager::new(5, 300);
        let uid = mgr.create_user("alice", "hash1");
        mgr.revoke_credential(uid, CredentialType::Password, "hash1");
        let result = mgr.authenticate(&AuthnRequest {
            username: "alice".into(),
            credential_type: CredentialType::Password,
            credential: "hash1".into(),
            source_ip: "127.0.0.1".into(),
        });
        assert!(matches!(result, AuthnResult::Failed { .. }));
    }

    #[test]
    fn test_user_not_found() {
        let mgr = AuthnManager::new(5, 300);
        let result = mgr.authenticate(&AuthnRequest {
            username: "nobody".into(),
            credential_type: CredentialType::Password,
            credential: "x".into(),
            source_ip: "127.0.0.1".into(),
        });
        assert!(matches!(result, AuthnResult::Failed { reason } if reason == "user not found"));
    }

    #[test]
    fn test_disable_user() {
        let mgr = AuthnManager::new(5, 300);
        let uid = mgr.create_user("alice", "pw");
        mgr.disable_user(uid);
        let result = mgr.authenticate(&AuthnRequest {
            username: "alice".into(),
            credential_type: CredentialType::Password,
            credential: "pw".into(),
            source_ip: "127.0.0.1".into(),
        });
        assert!(matches!(result, AuthnResult::Failed { reason } if reason == "account disabled"));
    }

    // -- RBAC Tests --

    #[test]
    fn test_rbac_grant_and_check() {
        let rbac = EnterpriseRbac::new();
        rbac.grant(1, RbacScope::Table, "orders", EnterprisePermission::Select, "admin");
        let mut roles = HashSet::new();
        roles.insert(1u64);
        let result = rbac.check(1, &roles, RbacScope::Table, "orders", &EnterprisePermission::Select);
        assert_eq!(result, RbacCheckResult::Allowed);
    }

    #[test]
    fn test_rbac_deny() {
        let rbac = EnterpriseRbac::new();
        let mut roles = HashSet::new();
        roles.insert(1u64);
        let result = rbac.check(1, &roles, RbacScope::Table, "orders", &EnterprisePermission::Delete);
        assert!(matches!(result, RbacCheckResult::Denied { .. }));
    }

    #[test]
    fn test_rbac_superuser_bypass() {
        let rbac = EnterpriseRbac::new();
        let mut roles = HashSet::new();
        roles.insert(0u64); // superuser
        let result = rbac.check(0, &roles, RbacScope::Cluster, "any", &EnterprisePermission::ManageCluster);
        assert_eq!(result, RbacCheckResult::Allowed);
    }

    #[test]
    fn test_rbac_wildcard_resource() {
        let rbac = EnterpriseRbac::new();
        rbac.grant(5, RbacScope::Database, "*", EnterprisePermission::Connect, "admin");
        let mut roles = HashSet::new();
        roles.insert(5u64);
        let result = rbac.check(5, &roles, RbacScope::Database, "mydb", &EnterprisePermission::Connect);
        assert_eq!(result, RbacCheckResult::Allowed);
    }

    #[test]
    fn test_rbac_revoke() {
        let rbac = EnterpriseRbac::new();
        rbac.grant(1, RbacScope::Table, "t1", EnterprisePermission::Insert, "admin");
        rbac.revoke(1, RbacScope::Table, "t1", &EnterprisePermission::Insert);
        let mut roles = HashSet::new();
        roles.insert(1u64);
        let result = rbac.check(1, &roles, RbacScope::Table, "t1", &EnterprisePermission::Insert);
        assert!(matches!(result, RbacCheckResult::Denied { .. }));
    }

    // -- TLS Cert Manager Tests --

    #[test]
    fn test_cert_load_and_get() {
        let mgr = CertificateManager::new(true);
        mgr.load_cert(CertificateRecord {
            link_type: TlsLinkType::ClientToGateway,
            cert_path: "/certs/server.pem".into(),
            key_path: "/certs/server.key".into(),
            ca_path: Some("/certs/ca.pem".into()),
            fingerprint: "sha256:aaa".into(),
            not_before: 1000,
            not_after: 2000,
            loaded_at: 1500,
            is_active: true,
        });
        let cert = mgr.get_cert(TlsLinkType::ClientToGateway).unwrap();
        assert_eq!(cert.fingerprint, "sha256:aaa");
    }

    #[test]
    fn test_cert_rotation() {
        let mgr = CertificateManager::new(true);
        mgr.load_cert(CertificateRecord {
            link_type: TlsLinkType::ClientToGateway,
            cert_path: "/old.pem".into(),
            key_path: "/old.key".into(),
            ca_path: None,
            fingerprint: "old".into(),
            not_before: 1000,
            not_after: 2000,
            loaded_at: 1500,
            is_active: true,
        });
        let ok = mgr.rotate_cert(
            TlsLinkType::ClientToGateway,
            "/new.pem",
            "/new.key",
            "new_fp",
            2000,
            3000,
        );
        assert!(ok);
        let cert = mgr.get_cert(TlsLinkType::ClientToGateway).unwrap();
        assert_eq!(cert.fingerprint, "new_fp");
        assert_eq!(mgr.rotation_history(1).len(), 1);
    }

    #[test]
    fn test_missing_link_detection() {
        let mgr = CertificateManager::new(true);
        mgr.load_cert(CertificateRecord {
            link_type: TlsLinkType::ClientToGateway,
            cert_path: "a".into(),
            key_path: "b".into(),
            ca_path: None,
            fingerprint: "f".into(),
            not_before: 0,
            not_after: u64::MAX,
            loaded_at: 0,
            is_active: true,
        });
        let missing = mgr.all_links_covered();
        assert_eq!(missing.len(), 3); // 3 of 4 links uncovered
    }

    // -- Backup Orchestrator Tests --

    #[test]
    fn test_backup_lifecycle() {
        let orch = BackupOrchestrator::new(30);
        let jid = orch.schedule_backup(
            EnterpriseBackupType::Full,
            BackupTarget::Local { path: "/backup".into() },
            "admin",
        );
        assert!(orch.start_backup(jid, 1000));
        assert!(orch.complete_backup(jid, 1_000_000, 10, 2000, 0xABCD));
        let job = orch.get_backup_job(jid).unwrap();
        assert_eq!(job.status, BackupJobStatus::Completed);
        assert_eq!(job.bytes_written, 1_000_000);
    }

    #[test]
    fn test_incremental_backup() {
        let orch = BackupOrchestrator::new(30);
        let jid = orch.schedule_backup(
            EnterpriseBackupType::Incremental,
            BackupTarget::S3 {
                bucket: "my-bucket".into(),
                prefix: "backups/".into(),
                region: "us-east-1".into(),
            },
            "scheduler",
        );
        orch.start_backup(jid, 5000);
        orch.complete_backup(jid, 50_000, 3, 6000, 0x1234);
        let job = orch.get_backup_job(jid).unwrap();
        assert_eq!(job.backup_type, EnterpriseBackupType::Incremental);
    }

    #[test]
    fn test_restore_pitr() {
        let orch = BackupOrchestrator::new(30);
        let rid = orch.schedule_restore(
            BackupTarget::Local { path: "/backup".into() },
            1,
            RestoreType::ToTimestamp,
            None,
            Some(1718000000),
        );
        orch.complete_restore(rid, 500_000, 8);
        let job = orch.get_restore_job(rid).unwrap();
        assert_eq!(job.status, BackupJobStatus::Completed);
        assert_eq!(job.restore_type, RestoreType::ToTimestamp);
    }

    #[test]
    fn test_backup_failure() {
        let orch = BackupOrchestrator::new(30);
        let jid = orch.schedule_backup(
            EnterpriseBackupType::Full,
            BackupTarget::Local { path: "/bad".into() },
            "admin",
        );
        orch.start_backup(jid, 0);
        orch.fail_backup(jid, "disk full");
        let job = orch.get_backup_job(jid).unwrap();
        assert!(matches!(job.status, BackupJobStatus::Failed(_)));
    }

    // -- Enterprise Audit Log Tests --

    #[test]
    fn test_audit_record_and_query() {
        let log = EnterpriseAuditLog::new(1000, "secret-key");
        log.record(
            AuditCategory::Authentication,
            AuditSeverity::Info,
            "alice", "10.0.0.1",
            "LOGIN", "system", "SUCCESS", "password auth",
        );
        log.record(
            AuditCategory::Authentication,
            AuditSeverity::Warning,
            "eve", "10.0.0.2",
            "LOGIN", "system", "FAILED", "wrong password",
        );
        assert_eq!(log.total(), 2);
        let auth_events = log.query_by_category(&AuditCategory::Authentication, 10);
        assert_eq!(auth_events.len(), 2);
    }

    #[test]
    fn test_audit_integrity_chain() {
        let log = EnterpriseAuditLog::new(1000, "key123");
        for i in 0..10 {
            log.record(
                AuditCategory::DataAccess,
                AuditSeverity::Info,
                &format!("user{}", i), "10.0.0.1",
                "SELECT", "table_a", "OK", "",
            );
        }
        assert!(log.verify_integrity());
    }

    #[test]
    fn test_audit_tamper_detection() {
        let log = EnterpriseAuditLog::new(1000, "key456");
        log.record(
            AuditCategory::SchemaChange,
            AuditSeverity::Info,
            "admin", "10.0.0.1",
            "CREATE TABLE", "users", "OK", "",
        );
        // Tamper with the event
        {
            let mut events = log.events.lock();
            if let Some(e) = events.front_mut() {
                e.action = "DROP TABLE".into(); // tampered!
            }
        }
        assert!(!log.verify_integrity());
    }

    #[test]
    fn test_audit_export_jsonl() {
        let log = EnterpriseAuditLog::new(1000, "export-key");
        log.record(
            AuditCategory::OpsOperation,
            AuditSeverity::Info,
            "ops", "10.0.0.1",
            "DRAIN", "node-3", "OK", "",
        );
        let lines = log.export_jsonl(1);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("DRAIN"));
        assert!(lines[0].contains("\"cat\":\"OPS\""));
    }

    #[test]
    fn test_audit_severity_ordering() {
        assert!(AuditSeverity::Info < AuditSeverity::Warning);
        assert!(AuditSeverity::Warning < AuditSeverity::Critical);
    }

    // -- Concurrent Tests --

    #[test]
    fn test_authn_concurrent() {
        use std::sync::Arc;
        let mgr = Arc::new(AuthnManager::new(100, 300));
        for i in 0..10u64 {
            mgr.create_user(&format!("u{}", i), &format!("pw{}", i));
        }
        let mut handles = Vec::new();
        for t in 0..4 {
            let mgr_c = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                for i in 0..250u64 {
                    let u = (t * 250 + i) % 10;
                    mgr_c.authenticate(&AuthnRequest {
                        username: format!("u{}", u),
                        credential_type: CredentialType::Password,
                        credential: format!("pw{}", u),
                        source_ip: "127.0.0.1".into(),
                    });
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(mgr.metrics.auth_attempts.load(Ordering::Relaxed), 1000);
        assert_eq!(mgr.metrics.auth_successes.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_audit_log_concurrent() {
        use std::sync::Arc;
        let log = Arc::new(EnterpriseAuditLog::new(5000, "key"));
        let mut handles = Vec::new();
        for t in 0..4 {
            let log_c = Arc::clone(&log);
            handles.push(std::thread::spawn(move || {
                for i in 0..250 {
                    log_c.record(
                        AuditCategory::DataAccess,
                        AuditSeverity::Info,
                        &format!("t{}", t), "10.0.0.1",
                        "SELECT", &format!("table_{}", i), "OK", "",
                    );
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(log.total(), 1000);
    }
}
