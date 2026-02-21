//! P2-2b: Role catalog — manages roles, grants, and privilege enforcement.
//!
//! Provides the runtime engine for RBAC:
//! - Role CRUD (create, alter, drop)
//! - Grant/revoke privileges on objects
//! - Transitive role inheritance (role membership closure)
//! - Privilege check with superuser bypass

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use falcon_common::security::{
    GrantEntry, ObjectRef, Privilege, PrivilegeCheckResult,
    Role, RoleId, SUPERUSER_ROLE_ID,
};
use falcon_common::tenant::{TenantId, SYSTEM_TENANT_ID};

/// Thread-safe role catalog. Uses RwLock for read-heavy workloads.
pub struct RoleCatalog {
    roles: RwLock<HashMap<RoleId, Role>>,
    grants: RwLock<Vec<GrantEntry>>,
    next_role_id: AtomicU64,
}

impl RoleCatalog {
    pub fn new() -> Self {
        let mut roles = HashMap::new();
        roles.insert(SUPERUSER_ROLE_ID, Role::superuser());
        Self {
            roles: RwLock::new(roles),
            grants: RwLock::new(Vec::new()),
            next_role_id: AtomicU64::new(1),
        }
    }

    /// Allocate a new unique role ID.
    pub fn alloc_role_id(&self) -> RoleId {
        RoleId(self.next_role_id.fetch_add(1, Ordering::Relaxed))
    }

    // ── Role CRUD ──

    /// Create a new role. Returns false if name already exists within the tenant.
    pub fn create_role(&self, role: Role) -> bool {
        let mut roles = self.roles.write();
        // Check for duplicate name within same tenant
        let dup = roles.values().any(|r| r.name == role.name && r.tenant_id == role.tenant_id);
        if dup {
            return false;
        }
        roles.insert(role.id, role);
        true
    }

    /// Drop a role by ID. Returns the removed role, or None.
    /// Also removes all grants involving this role (as grantee or grantor).
    pub fn drop_role(&self, role_id: RoleId) -> Option<Role> {
        let removed = self.roles.write().remove(&role_id);
        if removed.is_some() {
            let mut grants = self.grants.write();
            grants.retain(|g| g.grantee != role_id && g.grantor != role_id);
            // Remove this role from other roles' member_of sets
            let mut roles = self.roles.write();
            for r in roles.values_mut() {
                r.member_of.remove(&role_id);
            }
        }
        removed
    }

    /// Get a role by ID.
    pub fn get_role(&self, role_id: RoleId) -> Option<Role> {
        self.roles.read().get(&role_id).cloned()
    }

    /// Get a role by name within a tenant.
    pub fn get_role_by_name(&self, name: &str, tenant_id: TenantId) -> Option<Role> {
        self.roles.read().values()
            .find(|r| r.name == name && (r.tenant_id == tenant_id || r.tenant_id == SYSTEM_TENANT_ID))
            .cloned()
    }

    /// List all roles for a tenant.
    pub fn list_roles(&self, tenant_id: TenantId) -> Vec<Role> {
        self.roles.read().values()
            .filter(|r| r.tenant_id == tenant_id || r.tenant_id == SYSTEM_TENANT_ID)
            .cloned()
            .collect()
    }

    /// Grant a role to another role (role membership / inheritance).
    pub fn grant_role(&self, member_role: RoleId, parent_role: RoleId) -> bool {
        let mut roles = self.roles.write();
        if let Some(role) = roles.get_mut(&member_role) {
            role.member_of.insert(parent_role);
            true
        } else {
            false
        }
    }

    /// Revoke role membership.
    pub fn revoke_role(&self, member_role: RoleId, parent_role: RoleId) -> bool {
        let mut roles = self.roles.write();
        if let Some(role) = roles.get_mut(&member_role) {
            role.member_of.remove(&parent_role);
            true
        } else {
            false
        }
    }

    // ── Grant/Revoke Privileges ──

    /// Grant a privilege on an object to a role.
    pub fn grant_privilege(&self, entry: GrantEntry) {
        let mut grants = self.grants.write();
        // Avoid duplicate grants
        let exists = grants.iter().any(|g|
            g.grantee == entry.grantee
            && g.privilege == entry.privilege
            && g.object == entry.object
        );
        if !exists {
            grants.push(entry);
        }
    }

    /// Revoke a specific privilege.
    pub fn revoke_privilege(&self, grantee: RoleId, privilege: Privilege, object: &ObjectRef) {
        let mut grants = self.grants.write();
        grants.retain(|g| !(g.grantee == grantee && g.privilege == privilege && g.object == *object));
    }

    /// Revoke all privileges on an object for a role.
    pub fn revoke_all_on_object(&self, grantee: RoleId, object: &ObjectRef) {
        let mut grants = self.grants.write();
        grants.retain(|g| !(g.grantee == grantee && g.object == *object));
    }

    // ── Privilege Check ──

    /// Check if a role has a specific privilege on an object.
    /// Checks direct grants, then transitive role inheritance, then superuser bypass.
    pub fn check_privilege(
        &self,
        role_id: RoleId,
        privilege: Privilege,
        object: &ObjectRef,
    ) -> PrivilegeCheckResult {
        let roles = self.roles.read();

        // Superuser bypass
        if let Some(role) = roles.get(&role_id) {
            if role.is_superuser {
                return PrivilegeCheckResult::Allowed;
            }
        }

        // Compute the transitive closure of role memberships
        let effective_roles = self.effective_roles_inner(&roles, role_id);

        let grants = self.grants.read();

        // Check if any effective role has the required privilege (or ALL)
        for grant in grants.iter() {
            if grant.object == *object
                && effective_roles.contains(&grant.grantee)
                && (grant.privilege == privilege || grant.privilege == Privilege::All)
            {
                return PrivilegeCheckResult::Allowed;
            }
        }

        PrivilegeCheckResult::Denied {
            role: role_id,
            privilege,
            object: object.object_name.clone(),
        }
    }

    /// Compute the set of effective roles (role_id + all roles it inherits from, transitively).
    pub fn effective_roles(&self, role_id: RoleId) -> HashSet<RoleId> {
        let roles = self.roles.read();
        self.effective_roles_inner(&roles, role_id)
    }

    fn effective_roles_inner(&self, roles: &HashMap<RoleId, Role>, role_id: RoleId) -> HashSet<RoleId> {
        let mut result = HashSet::new();
        let mut stack = vec![role_id];
        while let Some(rid) = stack.pop() {
            if result.insert(rid) {
                if let Some(role) = roles.get(&rid) {
                    for &parent in &role.member_of {
                        if !result.contains(&parent) {
                            stack.push(parent);
                        }
                    }
                }
            }
        }
        result
    }

    /// List all grants for a specific role (including inherited).
    pub fn effective_grants(&self, role_id: RoleId) -> Vec<GrantEntry> {
        let effective = self.effective_roles(role_id);
        let grants = self.grants.read();
        grants.iter()
            .filter(|g| effective.contains(&g.grantee))
            .cloned()
            .collect()
    }

    /// Number of roles in the catalog.
    pub fn role_count(&self) -> usize {
        self.roles.read().len()
    }

    /// Number of grant entries.
    pub fn grant_count(&self) -> usize {
        self.grants.read().len()
    }
}

impl Default for RoleCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::security::{ObjectRef, ObjectType, Privilege, RoleId, Role};
    use falcon_common::tenant::TenantId;

    fn test_object() -> ObjectRef {
        ObjectRef {
            object_type: ObjectType::Table,
            object_id: 1,
            object_name: "users".into(),
        }
    }

    #[test]
    fn test_superuser_pre_registered() {
        let cat = RoleCatalog::new();
        assert_eq!(cat.role_count(), 1);
        let su = cat.get_role(SUPERUSER_ROLE_ID).unwrap();
        assert!(su.is_superuser);
    }

    #[test]
    fn test_create_and_get_role() {
        let cat = RoleCatalog::new();
        let id = cat.alloc_role_id();
        let role = Role::new_user(id, "alice".into(), TenantId(1));
        assert!(cat.create_role(role));
        assert_eq!(cat.role_count(), 2);

        let fetched = cat.get_role(id).unwrap();
        assert_eq!(fetched.name, "alice");
    }

    #[test]
    fn test_duplicate_role_name_rejected() {
        let cat = RoleCatalog::new();
        let id1 = cat.alloc_role_id();
        let id2 = cat.alloc_role_id();
        assert!(cat.create_role(Role::new_user(id1, "alice".into(), TenantId(1))));
        assert!(!cat.create_role(Role::new_user(id2, "alice".into(), TenantId(1))));
    }

    #[test]
    fn test_same_name_different_tenants_ok() {
        let cat = RoleCatalog::new();
        let id1 = cat.alloc_role_id();
        let id2 = cat.alloc_role_id();
        assert!(cat.create_role(Role::new_user(id1, "alice".into(), TenantId(1))));
        assert!(cat.create_role(Role::new_user(id2, "alice".into(), TenantId(2))));
    }

    #[test]
    fn test_drop_role_removes_grants() {
        let cat = RoleCatalog::new();
        let id = cat.alloc_role_id();
        cat.create_role(Role::new_user(id, "bob".into(), TenantId(1)));
        cat.grant_privilege(GrantEntry {
            grantee: id,
            privilege: Privilege::Select,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });
        assert_eq!(cat.grant_count(), 1);

        cat.drop_role(id);
        assert_eq!(cat.grant_count(), 0);
    }

    #[test]
    fn test_superuser_bypass() {
        let cat = RoleCatalog::new();
        let result = cat.check_privilege(SUPERUSER_ROLE_ID, Privilege::Delete, &test_object());
        assert!(result.is_allowed());
    }

    #[test]
    fn test_direct_grant_check() {
        let cat = RoleCatalog::new();
        let id = cat.alloc_role_id();
        cat.create_role(Role::new_user(id, "alice".into(), TenantId(1)));

        // No grant — denied
        let result = cat.check_privilege(id, Privilege::Select, &test_object());
        assert!(!result.is_allowed());

        // Grant SELECT — allowed
        cat.grant_privilege(GrantEntry {
            grantee: id,
            privilege: Privilege::Select,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });
        let result = cat.check_privilege(id, Privilege::Select, &test_object());
        assert!(result.is_allowed());

        // INSERT still denied
        let result = cat.check_privilege(id, Privilege::Insert, &test_object());
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_all_privilege_grants_everything() {
        let cat = RoleCatalog::new();
        let id = cat.alloc_role_id();
        cat.create_role(Role::new_user(id, "admin".into(), TenantId(1)));
        cat.grant_privilege(GrantEntry {
            grantee: id,
            privilege: Privilege::All,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });

        assert!(cat.check_privilege(id, Privilege::Select, &test_object()).is_allowed());
        assert!(cat.check_privilege(id, Privilege::Insert, &test_object()).is_allowed());
        assert!(cat.check_privilege(id, Privilege::Delete, &test_object()).is_allowed());
    }

    #[test]
    fn test_transitive_role_inheritance() {
        let cat = RoleCatalog::new();
        let reader_id = cat.alloc_role_id();
        let editor_id = cat.alloc_role_id();
        let alice_id = cat.alloc_role_id();

        cat.create_role(Role::new_user(reader_id, "reader".into(), TenantId(1)));
        cat.create_role(Role::new_user(editor_id, "editor".into(), TenantId(1)));
        cat.create_role(Role::new_user(alice_id, "alice".into(), TenantId(1)));

        // reader has SELECT on users
        cat.grant_privilege(GrantEntry {
            grantee: reader_id,
            privilege: Privilege::Select,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });

        // editor inherits from reader, also has INSERT
        cat.grant_role(editor_id, reader_id);
        cat.grant_privilege(GrantEntry {
            grantee: editor_id,
            privilege: Privilege::Insert,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });

        // alice inherits from editor
        cat.grant_role(alice_id, editor_id);

        // alice should have SELECT (via editor → reader) and INSERT (via editor)
        assert!(cat.check_privilege(alice_id, Privilege::Select, &test_object()).is_allowed());
        assert!(cat.check_privilege(alice_id, Privilege::Insert, &test_object()).is_allowed());
        assert!(!cat.check_privilege(alice_id, Privilege::Delete, &test_object()).is_allowed());

        // effective roles should be {alice, editor, reader}
        let effective = cat.effective_roles(alice_id);
        assert_eq!(effective.len(), 3);
        assert!(effective.contains(&alice_id));
        assert!(effective.contains(&editor_id));
        assert!(effective.contains(&reader_id));
    }

    #[test]
    fn test_revoke_privilege() {
        let cat = RoleCatalog::new();
        let id = cat.alloc_role_id();
        cat.create_role(Role::new_user(id, "bob".into(), TenantId(1)));
        cat.grant_privilege(GrantEntry {
            grantee: id,
            privilege: Privilege::Select,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });

        assert!(cat.check_privilege(id, Privilege::Select, &test_object()).is_allowed());

        cat.revoke_privilege(id, Privilege::Select, &test_object());
        assert!(!cat.check_privilege(id, Privilege::Select, &test_object()).is_allowed());
    }

    #[test]
    fn test_revoke_role_membership() {
        let cat = RoleCatalog::new();
        let reader_id = cat.alloc_role_id();
        let alice_id = cat.alloc_role_id();
        cat.create_role(Role::new_user(reader_id, "reader".into(), TenantId(1)));
        cat.create_role(Role::new_user(alice_id, "alice".into(), TenantId(1)));

        cat.grant_privilege(GrantEntry {
            grantee: reader_id,
            privilege: Privilege::Select,
            object: test_object(),
            grantor: SUPERUSER_ROLE_ID,
            with_grant_option: false,
        });

        cat.grant_role(alice_id, reader_id);
        assert!(cat.check_privilege(alice_id, Privilege::Select, &test_object()).is_allowed());

        cat.revoke_role(alice_id, reader_id);
        assert!(!cat.check_privilege(alice_id, Privilege::Select, &test_object()).is_allowed());
    }

    #[test]
    fn test_get_role_by_name() {
        let cat = RoleCatalog::new();
        let id = cat.alloc_role_id();
        cat.create_role(Role::new_user(id, "charlie".into(), TenantId(1)));

        let found = cat.get_role_by_name("charlie", TenantId(1));
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, id);

        // Not found in different tenant
        let not_found = cat.get_role_by_name("charlie", TenantId(99));
        assert!(not_found.is_none());
    }
}
