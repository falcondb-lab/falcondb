//! P2-2: Role-Based Access Control (RBAC) and audit model.
//!
//! Provides the foundational types for enterprise security:
//! - Roles with inheritance
//! - Object-level privileges (table, schema, function)
//! - Audit event recording

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

use crate::tenant::TenantId;
use crate::types::TableId;

// ── Role Model ──

/// Unique identifier for a database role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoleId(pub u64);

/// Reserved role ID for the superuser.
pub const SUPERUSER_ROLE_ID: RoleId = RoleId(0);

impl fmt::Display for RoleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "role:{}", self.0)
    }
}

/// A database role (user or group).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
    /// Tenant this role belongs to (SYSTEM_TENANT_ID for global roles).
    pub tenant_id: TenantId,
    /// Whether this role can log in (user vs. group role).
    pub can_login: bool,
    /// Whether this role is a superuser (bypasses all privilege checks).
    pub is_superuser: bool,
    /// Whether this role can create databases/schemas.
    pub can_create_db: bool,
    /// Whether this role can create other roles.
    pub can_create_role: bool,
    /// Roles this role inherits from (for GRANT role TO role).
    pub member_of: HashSet<RoleId>,
    /// Password hash (bcrypt or SCRAM-SHA-256). None = no password auth.
    pub password_hash: Option<String>,
}

impl Role {
    /// Create a new basic user role.
    pub fn new_user(id: RoleId, name: String, tenant_id: TenantId) -> Self {
        Self {
            id,
            name,
            tenant_id,
            can_login: true,
            is_superuser: false,
            can_create_db: false,
            can_create_role: false,
            member_of: HashSet::new(),
            password_hash: None,
        }
    }

    /// Create the built-in superuser role.
    pub fn superuser() -> Self {
        Self {
            id: SUPERUSER_ROLE_ID,
            name: "falcon".into(),
            tenant_id: crate::tenant::SYSTEM_TENANT_ID,
            can_login: true,
            is_superuser: true,
            can_create_db: true,
            can_create_role: true,
            member_of: HashSet::new(),
            password_hash: None,
        }
    }

    /// Check if this role has a given role in its inheritance chain.
    /// Note: full transitive closure requires the RoleCatalog; this only checks direct membership.
    pub fn is_member_of(&self, role_id: RoleId) -> bool {
        self.member_of.contains(&role_id)
    }
}

// ── Privilege Model ──

/// Types of database objects that can have privileges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectType {
    Table,
    Schema,
    Function,
    Sequence,
    Database,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table => write!(f, "TABLE"),
            Self::Schema => write!(f, "SCHEMA"),
            Self::Function => write!(f, "FUNCTION"),
            Self::Sequence => write!(f, "SEQUENCE"),
            Self::Database => write!(f, "DATABASE"),
        }
    }
}

/// Specific privilege types (PG-compatible subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Create,
    Connect,
    Usage,
    Execute,
    /// All privileges for the object type.
    All,
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select => write!(f, "SELECT"),
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
            Self::Truncate => write!(f, "TRUNCATE"),
            Self::References => write!(f, "REFERENCES"),
            Self::Trigger => write!(f, "TRIGGER"),
            Self::Create => write!(f, "CREATE"),
            Self::Connect => write!(f, "CONNECT"),
            Self::Usage => write!(f, "USAGE"),
            Self::Execute => write!(f, "EXECUTE"),
            Self::All => write!(f, "ALL"),
        }
    }
}

/// Identifies a specific database object for privilege checks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub object_type: ObjectType,
    /// For tables: TableId. For schemas: schema name hash. For functions: function OID.
    pub object_id: u64,
    /// Human-readable name for audit/error messages.
    pub object_name: String,
}

/// A single grant entry: "role X has privilege Y on object Z".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantEntry {
    pub grantee: RoleId,
    pub privilege: Privilege,
    pub object: ObjectRef,
    /// Who granted this privilege.
    pub grantor: RoleId,
    /// Whether the grantee can grant this privilege to others (WITH GRANT OPTION).
    pub with_grant_option: bool,
}

/// Result of a privilege check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivilegeCheckResult {
    /// Access granted.
    Allowed,
    /// Access denied — includes the missing privilege for error reporting.
    Denied {
        role: RoleId,
        privilege: Privilege,
        object: String,
    },
}

impl PrivilegeCheckResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

// ── Audit Model ──

/// Types of auditable events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    /// User login (successful or failed).
    Login,
    /// User logout / disconnect.
    Logout,
    /// DDL statement (CREATE, ALTER, DROP).
    Ddl,
    /// Privilege change (GRANT, REVOKE).
    PrivilegeChange,
    /// Role change (CREATE ROLE, ALTER ROLE, DROP ROLE).
    RoleChange,
    /// Configuration change (SET, ALTER SYSTEM).
    ConfigChange,
    /// Authentication failure.
    AuthFailure,
    /// Sensitive data access (if configured).
    DataAccess,
}

impl fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Login => write!(f, "LOGIN"),
            Self::Logout => write!(f, "LOGOUT"),
            Self::Ddl => write!(f, "DDL"),
            Self::PrivilegeChange => write!(f, "PRIVILEGE_CHANGE"),
            Self::RoleChange => write!(f, "ROLE_CHANGE"),
            Self::ConfigChange => write!(f, "CONFIG_CHANGE"),
            Self::AuthFailure => write!(f, "AUTH_FAILURE"),
            Self::DataAccess => write!(f, "DATA_ACCESS"),
        }
    }
}

/// A single audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Monotonic event ID.
    pub event_id: u64,
    /// Wall-clock timestamp (unix millis).
    pub timestamp_ms: u64,
    /// Event type.
    pub event_type: AuditEventType,
    /// Tenant context.
    pub tenant_id: TenantId,
    /// Role that performed the action.
    pub role_id: RoleId,
    /// Role name (denormalized for readability).
    pub role_name: String,
    /// Session ID / connection ID.
    pub session_id: i32,
    /// Source IP address (if available).
    pub source_ip: Option<String>,
    /// Human-readable description of the event.
    pub detail: String,
    /// SQL statement that triggered the event (truncated if too long).
    pub sql: Option<String>,
    /// Whether the operation succeeded.
    pub success: bool,
}

// ── Transaction Priority (P2-3) ──

/// Transaction priority level for SLA-based scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TxnPriority {
    /// Background tasks (GC, analytics, maintenance). Lowest priority.
    Background = 0,
    /// Normal OLTP transactions. Default priority.
    Normal = 1,
    /// High-priority transactions (critical business logic). Preempts Normal.
    High = 2,
    /// System-internal transactions (schema changes, replication). Highest priority.
    System = 3,
}

impl Default for TxnPriority {
    fn default() -> Self {
        Self::Normal
    }
}

impl fmt::Display for TxnPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Background => write!(f, "background"),
            Self::Normal => write!(f, "normal"),
            Self::High => write!(f, "high"),
            Self::System => write!(f, "system"),
        }
    }
}

impl TxnPriority {
    /// Parse from string (case-insensitive).
    pub fn from_str_ci(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "background" | "bg" | "low" => Some(Self::Background),
            "normal" | "default" => Some(Self::Normal),
            "high" | "critical" => Some(Self::High),
            "system" => Some(Self::System),
            _ => None,
        }
    }

    /// Whether this priority can preempt the given priority.
    pub fn can_preempt(self, other: Self) -> bool {
        (self as u8) > (other as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::SYSTEM_TENANT_ID;

    // ── Role tests ──

    #[test]
    fn test_superuser_role() {
        let su = Role::superuser();
        assert_eq!(su.id, SUPERUSER_ROLE_ID);
        assert!(su.is_superuser);
        assert!(su.can_login);
        assert!(su.can_create_db);
        assert!(su.can_create_role);
    }

    #[test]
    fn test_new_user_role() {
        let r = Role::new_user(RoleId(5), "alice".into(), TenantId(1));
        assert_eq!(r.id, RoleId(5));
        assert!(r.can_login);
        assert!(!r.is_superuser);
        assert!(!r.can_create_db);
        assert!(r.member_of.is_empty());
    }

    #[test]
    fn test_role_membership() {
        let mut r = Role::new_user(RoleId(5), "alice".into(), SYSTEM_TENANT_ID);
        let admin_role = RoleId(10);
        r.member_of.insert(admin_role);
        assert!(r.is_member_of(admin_role));
        assert!(!r.is_member_of(RoleId(99)));
    }

    // ── Privilege tests ──

    #[test]
    fn test_privilege_check_result() {
        assert!(PrivilegeCheckResult::Allowed.is_allowed());
        let denied = PrivilegeCheckResult::Denied {
            role: RoleId(1),
            privilege: Privilege::Select,
            object: "users".into(),
        };
        assert!(!denied.is_allowed());
    }

    #[test]
    fn test_object_type_display() {
        assert_eq!(ObjectType::Table.to_string(), "TABLE");
        assert_eq!(ObjectType::Schema.to_string(), "SCHEMA");
    }

    #[test]
    fn test_privilege_display() {
        assert_eq!(Privilege::Select.to_string(), "SELECT");
        assert_eq!(Privilege::All.to_string(), "ALL");
    }

    // ── Audit tests ──

    #[test]
    fn test_audit_event_type_display() {
        assert_eq!(AuditEventType::Login.to_string(), "LOGIN");
        assert_eq!(AuditEventType::Ddl.to_string(), "DDL");
        assert_eq!(AuditEventType::PrivilegeChange.to_string(), "PRIVILEGE_CHANGE");
    }

    // ── Priority tests ──

    #[test]
    fn test_txn_priority_ordering() {
        assert!(TxnPriority::High > TxnPriority::Normal);
        assert!(TxnPriority::Normal > TxnPriority::Background);
        assert!(TxnPriority::System > TxnPriority::High);
    }

    #[test]
    fn test_txn_priority_preemption() {
        assert!(TxnPriority::High.can_preempt(TxnPriority::Normal));
        assert!(TxnPriority::System.can_preempt(TxnPriority::High));
        assert!(!TxnPriority::Normal.can_preempt(TxnPriority::High));
        assert!(!TxnPriority::Background.can_preempt(TxnPriority::Normal));
    }

    #[test]
    fn test_txn_priority_parse() {
        assert_eq!(TxnPriority::from_str_ci("high"), Some(TxnPriority::High));
        assert_eq!(TxnPriority::from_str_ci("NORMAL"), Some(TxnPriority::Normal));
        assert_eq!(TxnPriority::from_str_ci("bg"), Some(TxnPriority::Background));
        assert_eq!(TxnPriority::from_str_ci("critical"), Some(TxnPriority::High));
        assert_eq!(TxnPriority::from_str_ci("invalid"), None);
    }

    #[test]
    fn test_txn_priority_default() {
        assert_eq!(TxnPriority::default(), TxnPriority::Normal);
    }
}
