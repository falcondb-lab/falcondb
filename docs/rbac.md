# FalconDB RBAC Guide

## Overview
FalconDB v1.1.0 implements Role-Based Access Control (RBAC) at three scope
levels: Cluster, Database, and Table. The system enforces least-privilege
by default — all operations are denied unless explicitly granted.

## Scope Hierarchy

### Cluster-Level Permissions
| Permission | Description |
|-----------|-------------|
| `ManageCluster` | Administrative operations (drain, upgrade, rebalance) |
| `CreateDatabase` | Create new databases |
| `ManageUsers` | Create/alter/drop users and roles |
| `ViewAuditLog` | Read audit log entries |
| `ManageBackups` | Schedule/cancel/restore backups |

### Database-Level Permissions
| Permission | Description |
|-----------|-------------|
| `Connect` | Connect to a database |
| `CreateTable` | Create tables within the database |
| `DropTable` | Drop tables within the database |

### Table-Level Permissions
| Permission | Description |
|-----------|-------------|
| `Select` | Read rows |
| `Insert` | Insert rows |
| `Update` | Update rows |
| `Delete` | Delete rows |
| `Truncate` | Truncate table |

## Permission Resolution

1. **Superuser check**: If role is superuser, allow immediately
2. **Effective roles**: Compute transitive closure (role + all inherited roles)
3. **Grant search**: Check if any effective role has a matching grant for (scope, resource, permission)
4. **Wildcard**: Resource `*` matches any resource at that scope
5. **Default deny**: If no grant found, deny with structured error

## Usage Examples

### Grant SELECT on a table
```rust
rbac.grant(role_id, RbacScope::Table, "orders", EnterprisePermission::Select, "admin");
```

### Grant database-wide access
```rust
rbac.grant(role_id, RbacScope::Database, "*", EnterprisePermission::Connect, "admin");
```

### Check permission
```rust
let effective_roles = compute_effective_roles(role_id); // includes inherited
let result = rbac.check(role_id, &effective_roles, RbacScope::Table, "orders", &EnterprisePermission::Select);
match result {
    RbacCheckResult::Allowed => { /* proceed */ },
    RbacCheckResult::Denied { scope, permission, resource } => {
        // Log and reject — does not leak topology
    }
}
```

### Revoke a permission
```rust
rbac.revoke(role_id, RbacScope::Table, "orders", &EnterprisePermission::Select);
```

## Integration with Existing RBAC
FalconDB v1.1.0 `EnterpriseRbac` extends the existing `RoleCatalog` and
`PrivilegeManager` (from `falcon_common::security` and `falcon_storage::role_catalog`)
with enterprise-scoped permissions. The existing PG-compatible privilege model
continues to work for SQL-level operations.

## Security Properties
- **Deny by default**: No implicit grants
- **No topology leakage**: Denied responses include only permission/resource info
- **Superuser bypass**: Role ID 0 bypasses all checks (for emergency access)
- **Audit**: All grant/revoke and check operations can be recorded in the audit log
- **Metrics**: `checks`, `grants_count`, `denials` tracked for observability
