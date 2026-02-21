//! P2-2: Role-Based Access Control (RBAC) and audit model.
//!
//! Provides the foundational types for enterprise security:
//! - Roles with inheritance
//! - Object-level privileges (table, schema, function)
//! - Audit event recording

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::tenant::TenantId;

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

// ── Role Catalog (v1.6) ──

/// In-memory catalog of roles with transitive inheritance resolution.
#[derive(Debug, Clone, Default)]
pub struct RoleCatalog {
    roles: HashMap<RoleId, Role>,
}

impl RoleCatalog {
    pub fn new() -> Self {
        let mut catalog = Self { roles: HashMap::new() };
        catalog.add_role(Role::superuser());
        catalog
    }

    /// Add a role to the catalog.
    pub fn add_role(&mut self, role: Role) {
        self.roles.insert(role.id, role);
    }

    /// Remove a role from the catalog.
    pub fn remove_role(&mut self, role_id: RoleId) -> Option<Role> {
        // Also remove this role from all member_of sets
        for r in self.roles.values_mut() {
            r.member_of.remove(&role_id);
        }
        self.roles.remove(&role_id)
    }

    /// Get a role by ID.
    pub fn get_role(&self, role_id: RoleId) -> Option<&Role> {
        self.roles.get(&role_id)
    }

    /// Get a role by name (case-insensitive).
    pub fn get_role_by_name(&self, name: &str) -> Option<&Role> {
        let lower = name.to_lowercase();
        self.roles.values().find(|r| r.name.to_lowercase() == lower)
    }

    /// Grant role membership: `member` becomes a member of `group`.
    pub fn grant_role(&mut self, member: RoleId, group: RoleId) -> Result<(), String> {
        if member == group {
            return Err("cannot grant a role to itself".into());
        }
        // Check for circular inheritance
        if self.is_member_of_transitive(group, member) {
            return Err(format!("circular role inheritance: {} is already a member of {}", group.0, member.0));
        }
        if let Some(role) = self.roles.get_mut(&member) {
            role.member_of.insert(group);
            Ok(())
        } else {
            Err(format!("role {} not found", member.0))
        }
    }

    /// Revoke role membership.
    pub fn revoke_role(&mut self, member: RoleId, group: RoleId) -> Result<(), String> {
        if let Some(role) = self.roles.get_mut(&member) {
            role.member_of.remove(&group);
            Ok(())
        } else {
            Err(format!("role {} not found", member.0))
        }
    }

    /// Check if `role_id` is a member of `target` (transitively).
    pub fn is_member_of_transitive(&self, role_id: RoleId, target: RoleId) -> bool {
        if role_id == target {
            return true;
        }
        let mut visited = HashSet::new();
        self.is_member_of_recursive(role_id, target, &mut visited)
    }

    fn is_member_of_recursive(
        &self,
        role_id: RoleId,
        target: RoleId,
        visited: &mut HashSet<RoleId>,
    ) -> bool {
        if !visited.insert(role_id) {
            return false; // cycle protection
        }
        if let Some(role) = self.roles.get(&role_id) {
            for &parent in &role.member_of {
                if parent == target {
                    return true;
                }
                if self.is_member_of_recursive(parent, target, visited) {
                    return true;
                }
            }
        }
        false
    }

    /// Get all roles that `role_id` is a member of (transitive closure).
    pub fn effective_roles(&self, role_id: RoleId) -> HashSet<RoleId> {
        let mut result = HashSet::new();
        result.insert(role_id);
        self.collect_roles(role_id, &mut result);
        result
    }

    fn collect_roles(&self, role_id: RoleId, result: &mut HashSet<RoleId>) {
        if let Some(role) = self.roles.get(&role_id) {
            for &parent in &role.member_of {
                if result.insert(parent) {
                    self.collect_roles(parent, result);
                }
            }
        }
    }

    /// List all roles.
    pub fn list_roles(&self) -> Vec<&Role> {
        self.roles.values().collect()
    }

    /// Number of roles in the catalog.
    pub fn role_count(&self) -> usize {
        self.roles.len()
    }
}

// ── Privilege Manager (v1.6) ──

/// Manages object-level privileges (GRANT/REVOKE).
/// Supports schema-level default privileges.
#[derive(Debug, Clone, Default)]
pub struct PrivilegeManager {
    /// All active grants.
    grants: Vec<GrantEntry>,
    /// Default privileges for new objects in a schema.
    /// Key: (grantor RoleId, schema name), Value: list of default grants.
    schema_defaults: HashMap<(RoleId, String), Vec<DefaultPrivilege>>,
}

/// A default privilege entry: automatically applied to new objects in a schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultPrivilege {
    pub grantee: RoleId,
    pub object_type: ObjectType,
    pub privilege: Privilege,
}

impl PrivilegeManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Grant a privilege on an object to a role.
    pub fn grant(
        &mut self,
        grantee: RoleId,
        privilege: Privilege,
        object: ObjectRef,
        grantor: RoleId,
        with_grant_option: bool,
    ) {
        // Check for duplicate
        let exists = self.grants.iter().any(|g| {
            g.grantee == grantee && g.privilege == privilege && g.object == object
        });
        if !exists {
            self.grants.push(GrantEntry {
                grantee,
                privilege,
                object,
                grantor,
                with_grant_option,
            });
        }
    }

    /// Revoke a privilege on an object from a role.
    pub fn revoke(
        &mut self,
        grantee: RoleId,
        privilege: Privilege,
        object: &ObjectRef,
    ) {
        self.grants.retain(|g| {
            !(g.grantee == grantee && g.privilege == privilege && g.object == *object)
        });
    }

    /// Revoke all privileges on an object (used when dropping the object).
    pub fn revoke_all_on_object(&mut self, object: &ObjectRef) {
        self.grants.retain(|g| g.object != *object);
    }

    /// Check if a role (or any of its effective roles) has a privilege on an object.
    pub fn check_privilege(
        &self,
        effective_roles: &HashSet<RoleId>,
        privilege: Privilege,
        object: &ObjectRef,
    ) -> PrivilegeCheckResult {
        // Superuser check should be done by the caller before calling this.
        for grant in &self.grants {
            if !effective_roles.contains(&grant.grantee) {
                continue;
            }
            if grant.object != *object {
                continue;
            }
            if grant.privilege == privilege || grant.privilege == Privilege::All {
                return PrivilegeCheckResult::Allowed;
            }
        }
        // Check schema-level grants (if object is a table, check schema Usage)
        PrivilegeCheckResult::Denied {
            role: *effective_roles.iter().next().unwrap_or(&RoleId(0)),
            privilege,
            object: object.object_name.clone(),
        }
    }

    /// Add a default privilege for new objects in a schema.
    pub fn add_schema_default(
        &mut self,
        grantor: RoleId,
        schema: &str,
        default: DefaultPrivilege,
    ) {
        self.schema_defaults
            .entry((grantor, schema.to_string()))
            .or_default()
            .push(default);
    }

    /// Get default privileges for a schema (to apply when creating new objects).
    pub fn schema_defaults(&self, grantor: RoleId, schema: &str) -> Vec<&DefaultPrivilege> {
        self.schema_defaults
            .get(&(grantor, schema.to_string()))
            .map(|v| v.iter().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    /// List all grants for a specific role.
    pub fn grants_for_role(&self, role_id: RoleId) -> Vec<&GrantEntry> {
        self.grants.iter().filter(|g| g.grantee == role_id).collect()
    }

    /// List all grants on a specific object.
    pub fn grants_on_object(&self, object: &ObjectRef) -> Vec<&GrantEntry> {
        self.grants.iter().filter(|g| g.object == *object).collect()
    }

    /// Total number of active grants.
    pub fn grant_count(&self) -> usize {
        self.grants.len()
    }
}

// ── Transaction Priority (P2-3) ──

/// Transaction priority level for SLA-based scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum TxnPriority {
    /// Background tasks (GC, analytics, maintenance). Lowest priority.
    Background = 0,
    /// Normal OLTP transactions. Default priority.
    #[default]
    Normal = 1,
    /// High-priority transactions (critical business logic). Preempts Normal.
    High = 2,
    /// System-internal transactions (schema changes, replication). Highest priority.
    System = 3,
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

    // ── RoleCatalog tests ──

    #[test]
    fn test_role_catalog_new_has_superuser() {
        let catalog = RoleCatalog::new();
        assert_eq!(catalog.role_count(), 1);
        let su = catalog.get_role(SUPERUSER_ROLE_ID).unwrap();
        assert!(su.is_superuser);
    }

    #[test]
    fn test_role_catalog_add_and_get() {
        let mut catalog = RoleCatalog::new();
        let alice = Role::new_user(RoleId(1), "alice".into(), SYSTEM_TENANT_ID);
        catalog.add_role(alice);
        assert_eq!(catalog.role_count(), 2);
        assert!(catalog.get_role(RoleId(1)).is_some());
        assert!(catalog.get_role_by_name("Alice").is_some());
    }

    #[test]
    fn test_role_catalog_remove() {
        let mut catalog = RoleCatalog::new();
        catalog.add_role(Role::new_user(RoleId(1), "alice".into(), SYSTEM_TENANT_ID));
        let removed = catalog.remove_role(RoleId(1));
        assert!(removed.is_some());
        assert_eq!(catalog.role_count(), 1);
    }

    #[test]
    fn test_role_catalog_grant_membership() {
        let mut catalog = RoleCatalog::new();
        let admin = Role::new_user(RoleId(10), "admin".into(), SYSTEM_TENANT_ID);
        let alice = Role::new_user(RoleId(1), "alice".into(), SYSTEM_TENANT_ID);
        catalog.add_role(admin);
        catalog.add_role(alice);
        catalog.grant_role(RoleId(1), RoleId(10)).unwrap();
        assert!(catalog.is_member_of_transitive(RoleId(1), RoleId(10)));
    }

    #[test]
    fn test_role_catalog_transitive_inheritance() {
        let mut catalog = RoleCatalog::new();
        catalog.add_role(Role::new_user(RoleId(1), "alice".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(RoleId(2), "editors".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(RoleId(3), "admins".into(), SYSTEM_TENANT_ID));
        // alice -> editors -> admins
        catalog.grant_role(RoleId(1), RoleId(2)).unwrap();
        catalog.grant_role(RoleId(2), RoleId(3)).unwrap();
        assert!(catalog.is_member_of_transitive(RoleId(1), RoleId(3)));
        assert!(!catalog.is_member_of_transitive(RoleId(3), RoleId(1)));
    }

    #[test]
    fn test_role_catalog_effective_roles() {
        let mut catalog = RoleCatalog::new();
        catalog.add_role(Role::new_user(RoleId(1), "alice".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(RoleId(2), "editors".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(RoleId(3), "admins".into(), SYSTEM_TENANT_ID));
        catalog.grant_role(RoleId(1), RoleId(2)).unwrap();
        catalog.grant_role(RoleId(2), RoleId(3)).unwrap();
        let effective = catalog.effective_roles(RoleId(1));
        assert!(effective.contains(&RoleId(1)));
        assert!(effective.contains(&RoleId(2)));
        assert!(effective.contains(&RoleId(3)));
        assert_eq!(effective.len(), 3);
    }

    #[test]
    fn test_role_catalog_circular_inheritance_rejected() {
        let mut catalog = RoleCatalog::new();
        catalog.add_role(Role::new_user(RoleId(1), "a".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(RoleId(2), "b".into(), SYSTEM_TENANT_ID));
        catalog.grant_role(RoleId(1), RoleId(2)).unwrap();
        let result = catalog.grant_role(RoleId(2), RoleId(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_role_catalog_self_grant_rejected() {
        let mut catalog = RoleCatalog::new();
        catalog.add_role(Role::new_user(RoleId(1), "a".into(), SYSTEM_TENANT_ID));
        let result = catalog.grant_role(RoleId(1), RoleId(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_role_catalog_revoke_membership() {
        let mut catalog = RoleCatalog::new();
        catalog.add_role(Role::new_user(RoleId(1), "alice".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(RoleId(2), "editors".into(), SYSTEM_TENANT_ID));
        catalog.grant_role(RoleId(1), RoleId(2)).unwrap();
        assert!(catalog.is_member_of_transitive(RoleId(1), RoleId(2)));
        catalog.revoke_role(RoleId(1), RoleId(2)).unwrap();
        assert!(!catalog.is_member_of_transitive(RoleId(1), RoleId(2)));
    }

    // ── PrivilegeManager tests ──

    #[test]
    fn test_privilege_manager_grant_and_check() {
        let mut pm = PrivilegeManager::new();
        let obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        };
        pm.grant(RoleId(1), Privilege::Select, obj.clone(), RoleId(0), false);
        assert_eq!(pm.grant_count(), 1);

        let roles: HashSet<RoleId> = [RoleId(1)].into_iter().collect();
        assert!(pm.check_privilege(&roles, Privilege::Select, &obj).is_allowed());
        assert!(!pm.check_privilege(&roles, Privilege::Insert, &obj).is_allowed());
    }

    #[test]
    fn test_privilege_manager_all_grants_everything() {
        let mut pm = PrivilegeManager::new();
        let obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        };
        pm.grant(RoleId(1), Privilege::All, obj.clone(), RoleId(0), false);

        let roles: HashSet<RoleId> = [RoleId(1)].into_iter().collect();
        assert!(pm.check_privilege(&roles, Privilege::Select, &obj).is_allowed());
        assert!(pm.check_privilege(&roles, Privilege::Insert, &obj).is_allowed());
        assert!(pm.check_privilege(&roles, Privilege::Delete, &obj).is_allowed());
    }

    #[test]
    fn test_privilege_manager_revoke() {
        let mut pm = PrivilegeManager::new();
        let obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        };
        pm.grant(RoleId(1), Privilege::Select, obj.clone(), RoleId(0), false);
        pm.revoke(RoleId(1), Privilege::Select, &obj);
        assert_eq!(pm.grant_count(), 0);
    }

    #[test]
    fn test_privilege_manager_inherited_role_check() {
        let mut pm = PrivilegeManager::new();
        let obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        };
        // Grant to editors role (RoleId(2))
        pm.grant(RoleId(2), Privilege::Select, obj.clone(), RoleId(0), false);

        // Alice (RoleId(1)) is a member of editors — effective roles include both
        let effective: HashSet<RoleId> = [RoleId(1), RoleId(2)].into_iter().collect();
        assert!(pm.check_privilege(&effective, Privilege::Select, &obj).is_allowed());
    }

    #[test]
    fn test_privilege_manager_schema_defaults() {
        let mut pm = PrivilegeManager::new();
        pm.add_schema_default(RoleId(0), "public", DefaultPrivilege {
            grantee: RoleId(1),
            object_type: ObjectType::Table,
            privilege: Privilege::Select,
        });
        let defaults = pm.schema_defaults(RoleId(0), "public");
        assert_eq!(defaults.len(), 1);
        assert_eq!(defaults[0].privilege, Privilege::Select);
    }

    #[test]
    fn test_privilege_manager_revoke_all_on_object() {
        let mut pm = PrivilegeManager::new();
        let obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        };
        pm.grant(RoleId(1), Privilege::Select, obj.clone(), RoleId(0), false);
        pm.grant(RoleId(2), Privilege::Insert, obj.clone(), RoleId(0), false);
        assert_eq!(pm.grant_count(), 2);
        pm.revoke_all_on_object(&obj);
        assert_eq!(pm.grant_count(), 0);
    }

    #[test]
    fn test_privilege_manager_no_duplicate_grants() {
        let mut pm = PrivilegeManager::new();
        let obj = ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        };
        pm.grant(RoleId(1), Privilege::Select, obj.clone(), RoleId(0), false);
        pm.grant(RoleId(1), Privilege::Select, obj.clone(), RoleId(0), false);
        assert_eq!(pm.grant_count(), 1);
    }
}
