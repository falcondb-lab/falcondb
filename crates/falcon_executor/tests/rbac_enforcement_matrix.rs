//! RBAC Full-Path Enforcement Test Matrix
//!
//! Proves that privilege checks are enforced across ALL execution paths:
//!
//! Layer 1 — Executor Internal Path:
//!   RBAC-E1: SELECT denied without SELECT privilege
//!   RBAC-E2: INSERT denied without INSERT privilege
//!   RBAC-E3: UPDATE denied without UPDATE privilege
//!   RBAC-E4: DELETE denied without DELETE privilege
//!   RBAC-E5: Superuser bypasses all checks
//!   RBAC-E6: Inherited role grants access
//!   RBAC-E7: Revoked privilege immediately denies
//!   RBAC-E8: GRANT ALL covers SELECT/INSERT/UPDATE/DELETE
//!   RBAC-E9: Cross-object isolation (grant on A does not allow B)
//!   RBAC-E10: No RBAC configured = all allowed (backward compat)
//!
//! Layer 2 — Storage Role Catalog:
//!   RBAC-S1: Superuser bypass at catalog level
//!   RBAC-S2: Transitive inheritance resolves correctly
//!   RBAC-S3: Revoke role membership immediately removes access
//!   RBAC-S4: Drop role removes all associated grants
//!   RBAC-S5: Duplicate role names rejected within tenant
//!
//! Layer 3 — Common Security Model:
//!   RBAC-C1: PrivilegeManager check_privilege with effective roles
//!   RBAC-C2: RoleCatalog circular inheritance detection
//!   RBAC-C3: Schema default privileges applied correctly

use std::sync::Arc;

use falcon_common::security::*;
use falcon_common::tenant::{TenantId, SYSTEM_TENANT_ID};
use falcon_common::types::*;
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::datum::{Datum, OwnedRow};

use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;
use falcon_executor::Executor;

fn test_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "rbac_test".into(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "name".into(),
                data_type: DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        ..Default::default()
    }
}

fn setup_executor_with_rbac(
    role_id: RoleId,
) -> (
    Executor,
    Arc<std::sync::RwLock<RoleCatalog>>,
    Arc<std::sync::RwLock<PrivilegeManager>>,
    Arc<StorageEngine>,
    Arc<TxnManager>,
) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    storage.create_table(test_schema()).unwrap();

    let role_catalog = Arc::new(std::sync::RwLock::new(RoleCatalog::new()));
    let privilege_mgr = Arc::new(std::sync::RwLock::new(PrivilegeManager::new()));

    let mut executor = Executor::new(storage.clone(), txn_mgr.clone());
    executor.set_rbac(role_catalog.clone(), privilege_mgr.clone(), role_id);

    (executor, role_catalog, privilege_mgr, storage, txn_mgr)
}

fn table_obj(name: &str) -> ObjectRef {
    ObjectRef {
        object_type: ObjectType::Table,
        object_id: {
            let mut h: u64 = 5381;
            for b in name.bytes() {
                h = h.wrapping_mul(33).wrapping_add(b as u64);
            }
            h
        },
        object_name: name.to_string(),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Layer 1 — Executor Internal Path
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn rbac_e1_select_denied_without_privilege() {
    let alice_id = RoleId(100);
    let (executor, role_catalog, _pm, storage, txn_mgr) = setup_executor_with_rbac(alice_id);

    // Register alice in catalog
    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(alice_id, "alice".into(), SYSTEM_TENANT_ID));
    }

    // Insert a row as superuser first
    {
        let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("test".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();
        txn_mgr.commit(txn.txn_id).unwrap();
    }

    // Alice has no grants — check_privilege should deny SELECT
    let result = executor.check_privilege_public(
        Privilege::Select,
        ObjectType::Table,
        "rbac_test",
    );
    assert!(result.is_err(), "RBAC-E1: SELECT should be denied without privilege");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("permission denied"), "RBAC-E1: error should mention permission denied");
}

#[test]
fn rbac_e2_insert_denied_without_privilege() {
    let alice_id = RoleId(101);
    let (executor, role_catalog, _pm, _storage, _txn_mgr) = setup_executor_with_rbac(alice_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(alice_id, "alice".into(), SYSTEM_TENANT_ID));
    }

    let result = executor.check_privilege_public(
        Privilege::Insert,
        ObjectType::Table,
        "rbac_test",
    );
    assert!(result.is_err(), "RBAC-E2: INSERT should be denied");
}

#[test]
fn rbac_e3_update_denied_without_privilege() {
    let alice_id = RoleId(102);
    let (executor, role_catalog, _pm, _storage, _txn_mgr) = setup_executor_with_rbac(alice_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(alice_id, "alice".into(), SYSTEM_TENANT_ID));
    }

    let result = executor.check_privilege_public(
        Privilege::Update,
        ObjectType::Table,
        "rbac_test",
    );
    assert!(result.is_err(), "RBAC-E3: UPDATE should be denied");
}

#[test]
fn rbac_e4_delete_denied_without_privilege() {
    let alice_id = RoleId(103);
    let (executor, role_catalog, _pm, _storage, _txn_mgr) = setup_executor_with_rbac(alice_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(alice_id, "alice".into(), SYSTEM_TENANT_ID));
    }

    let result = executor.check_privilege_public(
        Privilege::Delete,
        ObjectType::Table,
        "rbac_test",
    );
    assert!(result.is_err(), "RBAC-E4: DELETE should be denied");
}

#[test]
fn rbac_e5_superuser_bypasses_all() {
    let (executor, _role_catalog, _pm, _storage, _txn_mgr) =
        setup_executor_with_rbac(SUPERUSER_ROLE_ID);

    // Superuser should pass all privilege checks without any grants
    for privilege in &[Privilege::Select, Privilege::Insert, Privilege::Update, Privilege::Delete] {
        let result = executor.check_privilege_public(*privilege, ObjectType::Table, "rbac_test");
        assert!(result.is_ok(), "RBAC-E5: superuser should bypass {:?} check", privilege);
    }
}

#[test]
fn rbac_e6_inherited_role_grants_access() {
    let alice_id = RoleId(110);
    let editor_id = RoleId(111);
    let (executor, role_catalog, privilege_mgr, _storage, _txn_mgr) = setup_executor_with_rbac(alice_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(alice_id, "alice".into(), SYSTEM_TENANT_ID));
        catalog.add_role(Role::new_user(editor_id, "editor".into(), SYSTEM_TENANT_ID));
        catalog.grant_role(alice_id, editor_id).unwrap();
    }

    // Grant SELECT to editor role
    {
        let mut pm = privilege_mgr.write().unwrap();
        pm.grant(
            editor_id,
            Privilege::Select,
            table_obj("rbac_test"),
            SUPERUSER_ROLE_ID,
            false,
        );
    }

    // Alice should have SELECT via inherited editor role
    let result = executor.check_privilege_public(Privilege::Select, ObjectType::Table, "rbac_test");
    assert!(result.is_ok(), "RBAC-E6: alice should have SELECT via editor inheritance");
}

#[test]
fn rbac_e7_revoked_privilege_immediately_denies() {
    let bob_id = RoleId(120);
    let (executor, role_catalog, privilege_mgr, _storage, _txn_mgr) = setup_executor_with_rbac(bob_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(bob_id, "bob".into(), SYSTEM_TENANT_ID));
    }

    let obj = table_obj("rbac_test");

    // Grant SELECT
    {
        let mut pm = privilege_mgr.write().unwrap();
        pm.grant(bob_id, Privilege::Select, obj.clone(), SUPERUSER_ROLE_ID, false);
    }
    assert!(
        executor.check_privilege_public(Privilege::Select, ObjectType::Table, "rbac_test").is_ok(),
        "RBAC-E7: should be allowed after grant"
    );

    // Revoke SELECT
    {
        let mut pm = privilege_mgr.write().unwrap();
        pm.revoke(bob_id, Privilege::Select, &obj);
    }
    assert!(
        executor.check_privilege_public(Privilege::Select, ObjectType::Table, "rbac_test").is_err(),
        "RBAC-E7: should be denied after revoke"
    );
}

#[test]
fn rbac_e8_grant_all_covers_everything() {
    let admin_id = RoleId(130);
    let (executor, role_catalog, privilege_mgr, _storage, _txn_mgr) = setup_executor_with_rbac(admin_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(admin_id, "admin".into(), SYSTEM_TENANT_ID));
    }

    {
        let mut pm = privilege_mgr.write().unwrap();
        pm.grant(
            admin_id,
            Privilege::All,
            table_obj("rbac_test"),
            SUPERUSER_ROLE_ID,
            false,
        );
    }

    for privilege in &[Privilege::Select, Privilege::Insert, Privilege::Update, Privilege::Delete] {
        let result = executor.check_privilege_public(*privilege, ObjectType::Table, "rbac_test");
        assert!(result.is_ok(), "RBAC-E8: ALL should cover {:?}", privilege);
    }
}

#[test]
fn rbac_e9_cross_object_isolation() {
    let charlie_id = RoleId(140);
    let (executor, role_catalog, privilege_mgr, _storage, _txn_mgr) = setup_executor_with_rbac(charlie_id);

    {
        let mut catalog = role_catalog.write().unwrap();
        catalog.add_role(Role::new_user(charlie_id, "charlie".into(), SYSTEM_TENANT_ID));
    }

    // Grant SELECT on table_a, but NOT on rbac_test
    {
        let mut pm = privilege_mgr.write().unwrap();
        pm.grant(
            charlie_id,
            Privilege::Select,
            table_obj("table_a"),
            SUPERUSER_ROLE_ID,
            false,
        );
    }

    // Should be denied on rbac_test
    assert!(
        executor.check_privilege_public(Privilege::Select, ObjectType::Table, "rbac_test").is_err(),
        "RBAC-E9: grant on table_a should not allow access to rbac_test"
    );
}

#[test]
fn rbac_e10_no_rbac_configured_allows_all() {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    storage.create_table(test_schema()).unwrap();

    // Executor without RBAC configured
    let executor = Executor::new(storage, txn_mgr);

    for privilege in &[Privilege::Select, Privilege::Insert, Privilege::Update, Privilege::Delete] {
        let result = executor.check_privilege_public(*privilege, ObjectType::Table, "rbac_test");
        assert!(result.is_ok(), "RBAC-E10: no RBAC configured should allow {:?}", privilege);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Layer 2 — Storage Role Catalog
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn rbac_s1_superuser_bypass_at_catalog_level() {
    let cat = falcon_storage::role_catalog::RoleCatalog::new();
    let obj = falcon_common::security::ObjectRef {
        object_type: ObjectType::Table,
        object_id: 1,
        object_name: "secret_table".into(),
    };
    // No grants at all — superuser should still be allowed
    let result = cat.check_privilege(SUPERUSER_ROLE_ID, Privilege::Delete, &obj);
    assert!(result.is_allowed(), "RBAC-S1: superuser should bypass at catalog level");
}

#[test]
fn rbac_s2_transitive_inheritance_resolves() {
    let cat = falcon_storage::role_catalog::RoleCatalog::new();
    let reader_id = cat.alloc_role_id();
    let editor_id = cat.alloc_role_id();
    let alice_id = cat.alloc_role_id();

    cat.create_role(Role::new_user(reader_id, "reader".into(), TenantId(1)));
    cat.create_role(Role::new_user(editor_id, "editor".into(), TenantId(1)));
    cat.create_role(Role::new_user(alice_id, "alice".into(), TenantId(1)));

    let obj = falcon_common::security::ObjectRef {
        object_type: ObjectType::Table,
        object_id: 1,
        object_name: "data".into(),
    };

    cat.grant_privilege(GrantEntry {
        grantee: reader_id,
        privilege: Privilege::Select,
        object: obj.clone(),
        grantor: SUPERUSER_ROLE_ID,
        with_grant_option: false,
    });

    // editor inherits reader
    cat.grant_role(editor_id, reader_id);
    // alice inherits editor
    cat.grant_role(alice_id, editor_id);

    // alice -> editor -> reader (has SELECT)
    let result = cat.check_privilege(alice_id, Privilege::Select, &obj);
    assert!(result.is_allowed(), "RBAC-S2: transitive inheritance should grant SELECT");

    // DELETE still denied
    assert!(!cat.check_privilege(alice_id, Privilege::Delete, &obj).is_allowed());
}

#[test]
fn rbac_s3_revoke_membership_removes_access() {
    let cat = falcon_storage::role_catalog::RoleCatalog::new();
    let reader_id = cat.alloc_role_id();
    let alice_id = cat.alloc_role_id();

    cat.create_role(Role::new_user(reader_id, "reader".into(), TenantId(1)));
    cat.create_role(Role::new_user(alice_id, "alice".into(), TenantId(1)));

    let obj = falcon_common::security::ObjectRef {
        object_type: ObjectType::Table,
        object_id: 1,
        object_name: "data".into(),
    };

    cat.grant_privilege(GrantEntry {
        grantee: reader_id,
        privilege: Privilege::Select,
        object: obj.clone(),
        grantor: SUPERUSER_ROLE_ID,
        with_grant_option: false,
    });

    cat.grant_role(alice_id, reader_id);
    assert!(cat.check_privilege(alice_id, Privilege::Select, &obj).is_allowed());

    cat.revoke_role(alice_id, reader_id);
    assert!(!cat.check_privilege(alice_id, Privilege::Select, &obj).is_allowed(),
        "RBAC-S3: revoked membership should immediately deny");
}

#[test]
fn rbac_s4_drop_role_removes_grants() {
    let cat = falcon_storage::role_catalog::RoleCatalog::new();
    let bob_id = cat.alloc_role_id();
    cat.create_role(Role::new_user(bob_id, "bob".into(), TenantId(1)));

    let obj = falcon_common::security::ObjectRef {
        object_type: ObjectType::Table,
        object_id: 1,
        object_name: "data".into(),
    };
    cat.grant_privilege(GrantEntry {
        grantee: bob_id,
        privilege: Privilege::Select,
        object: obj.clone(),
        grantor: SUPERUSER_ROLE_ID,
        with_grant_option: false,
    });
    assert_eq!(cat.grant_count(), 1);

    cat.drop_role(bob_id);
    assert_eq!(cat.grant_count(), 0, "RBAC-S4: dropping role should remove all grants");
}

#[test]
fn rbac_s5_duplicate_role_name_rejected() {
    let cat = falcon_storage::role_catalog::RoleCatalog::new();
    let id1 = cat.alloc_role_id();
    let id2 = cat.alloc_role_id();
    assert!(cat.create_role(Role::new_user(id1, "alice".into(), TenantId(1))));
    assert!(!cat.create_role(Role::new_user(id2, "alice".into(), TenantId(1))),
        "RBAC-S5: duplicate role name should be rejected");
}

// ═══════════════════════════════════════════════════════════════════════════
// Layer 3 — Common Security Model
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn rbac_c1_privilege_manager_with_effective_roles() {
    let mut pm = PrivilegeManager::new();
    let editor_id = RoleId(200);
    let alice_id = RoleId(201);

    let obj = ObjectRef {
        object_type: ObjectType::Table,
        object_id: 1,
        object_name: "users".into(),
    };

    pm.grant(editor_id, Privilege::Select, obj.clone(), SUPERUSER_ROLE_ID, false);

    // Alice's effective roles include editor
    let effective: std::collections::HashSet<RoleId> =
        [alice_id, editor_id].into_iter().collect();

    assert!(pm.check_privilege(&effective, Privilege::Select, &obj).is_allowed(),
        "RBAC-C1: effective roles should include inherited grants");
    assert!(!pm.check_privilege(&effective, Privilege::Delete, &obj).is_allowed(),
        "RBAC-C1: non-granted privilege should be denied");
}

#[test]
fn rbac_c2_circular_inheritance_detected() {
    let mut catalog = falcon_common::security::RoleCatalog::new();
    catalog.add_role(Role::new_user(RoleId(1), "a".into(), SYSTEM_TENANT_ID));
    catalog.add_role(Role::new_user(RoleId(2), "b".into(), SYSTEM_TENANT_ID));
    catalog.add_role(Role::new_user(RoleId(3), "c".into(), SYSTEM_TENANT_ID));

    catalog.grant_role(RoleId(1), RoleId(2)).unwrap();
    catalog.grant_role(RoleId(2), RoleId(3)).unwrap();

    // Creating 3 -> 1 would form a cycle
    let result = catalog.grant_role(RoleId(3), RoleId(1));
    assert!(result.is_err(), "RBAC-C2: circular inheritance should be detected and rejected");
}

#[test]
fn rbac_c3_schema_default_privileges() {
    let mut pm = PrivilegeManager::new();

    pm.add_schema_default(SUPERUSER_ROLE_ID, "public", DefaultPrivilege {
        grantee: RoleId(300),
        object_type: ObjectType::Table,
        privilege: Privilege::Select,
    });

    pm.add_schema_default(SUPERUSER_ROLE_ID, "public", DefaultPrivilege {
        grantee: RoleId(300),
        object_type: ObjectType::Table,
        privilege: Privilege::Insert,
    });

    let defaults = pm.schema_defaults(SUPERUSER_ROLE_ID, "public");
    assert_eq!(defaults.len(), 2, "RBAC-C3: should have 2 default privileges");
    assert_eq!(defaults[0].privilege, Privilege::Select);
    assert_eq!(defaults[1].privilege, Privilege::Insert);

    // Different schema should be empty
    let other = pm.schema_defaults(SUPERUSER_ROLE_ID, "private");
    assert!(other.is_empty(), "RBAC-C3: different schema should have no defaults");
}
