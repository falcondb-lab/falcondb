# FalconDB Security Model

## Overview
FalconDB v1.1.0 implements enterprise-grade security across authentication,
authorization, encryption, and audit. Every capability can be explained to
an enterprise architect.

## Authentication (AuthN)

### Credential Types
| Type | Storage | Verification | Use Case |
|------|---------|-------------|----------|
| Password | SCRAM-SHA-256 hash | Challenge-response | Interactive users |
| Token | SHA-256 hash | Direct comparison | Service accounts, CI/CD |
| mTLS | Cert fingerprint | Certificate validation | Node-to-node, automation |

### Account Lifecycle
1. **Create**: `AuthnManager::create_user(username, password_hash)` → user_id
2. **Add credentials**: `add_token()`, `add_mtls_cert()`
3. **Authenticate**: `authenticate(AuthnRequest)` → Success/Failed/Locked/Expired
4. **Rotate**: Add new credential, verify, revoke old
5. **Disable**: `disable_user()` — immediate lockout

### Lockout Policy
- Configurable `lockout_threshold` (default: 5 failures)
- Configurable `lockout_duration_secs` (default: 300s)
- Lockout clears on expiry, not on success during lockout
- Metrics: `auth_attempts`, `auth_successes`, `auth_failures`, `auth_lockouts`

### Security Guarantees
- Failed auth never leaks topology information
- Credential hashes never returned in API responses
- Expired credentials rejected before password comparison

## Authorization (AuthZ / RBAC)

### Scope Hierarchy
```
Cluster (ManageCluster, CreateDatabase, ManageUsers, ViewAuditLog, ManageBackups)
  └── Database (Connect, CreateTable, DropTable)
        └── Table (Select, Insert, Update, Delete, Truncate)
```

### Permission Resolution
1. Check superuser flag → allow all
2. Compute effective roles (direct + inherited)
3. Check grants for any effective role matching scope + resource + permission
4. Wildcard resource (`*`) matches any resource at that scope
5. Deny by default

### Built-in Roles
| Role | ID | Scope | Notes |
|------|----|-------|-------|
| falcon (superuser) | 0 | Cluster | Bypasses all checks |

### Grant Examples
```
GRANT SELECT ON TABLE orders TO role_id=10
GRANT CONNECT ON DATABASE analytics TO role_id=100
GRANT MANAGE_CLUSTER ON CLUSTER * TO role_id=50
```

## Full-Chain TLS

### Link Coverage
| Link | Direction | Required |
|------|-----------|----------|
| Client → Gateway | Inbound | Configurable |
| Gateway → Data Node | Internal | Configurable |
| Data Node → Replica | Internal | Configurable |
| Data Node → Controller | Internal | Configurable |

### Certificate Hot-Reload
1. Load new cert via `CertificateManager::rotate_cert()`
2. Old connections continue on old cert until close
3. New connections use new cert immediately
4. Rotation history tracked for audit
5. Zero restart, zero global disconnect

### Expiry Monitoring
- `expiring_certs(within_secs)` returns certs nearing expiration
- Integrate with capacity alerts for proactive rotation

## Audit Log

### Event Categories
| Category | Examples |
|----------|---------|
| AUTHN | Login success/failure, lockout |
| AUTHZ | Permission denied, privilege check |
| DATA_ACCESS | SELECT, INSERT on sensitive tables |
| SCHEMA_CHANGE | CREATE/ALTER/DROP TABLE |
| PRIVILEGE_CHANGE | GRANT, REVOKE |
| OPS | Drain, upgrade, rebalance |
| TOPOLOGY | Leader change, node join/leave |
| SECURITY | Cert rotation, policy change |
| BACKUP_RESTORE | Backup start/complete, restore |
| CONFIG_CHANGE | Dynamic config updates |

### Tamper-Proof Chain
- Each event's hash = HMAC(key, prev_hash + event_data)
- Chain starts from "genesis" hash
- `verify_integrity()` validates the entire chain
- Any modification to past events breaks the chain

### SIEM Integration
- `export_jsonl(since_id)` returns JSON-lines format
- Each line: `{"id":N,"ts":T,"cat":"...","sev":"...","actor":"...","ip":"...","action":"...","resource":"...","outcome":"...","details":"...","hash":"..."}`
- Compatible with Splunk, Elasticsearch, Datadog
