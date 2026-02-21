# FalconDB Security Guide

## Overview

FalconDB provides defense-in-depth security across five layers:

1. **Authentication** — Trust, Password, MD5, SCRAM-SHA-256
2. **Authorization** — Role-based access control (RBAC) with privilege inheritance
3. **Transport Security** — TLS/mTLS with configurable minimum version
4. **Runtime Protection** — SQL firewall, auth rate limiting, password policy
5. **Audit** — Immutable audit log for login, DDL, privilege changes

---

## Authentication

### Supported Methods

| Method | Config Value | Security Level | Description |
|--------|-------------|----------------|-------------|
| Trust | `trust` | None | No authentication (development only) |
| Password | `password` | Low | Cleartext password (PG auth type 3) |
| MD5 | `md5` | Medium | MD5 hashed password (PG auth type 5) |
| SCRAM-SHA-256 | `scram-sha-256` | High | Channel-binding capable (PG 10+) |

### Configuration

```toml
[server.auth]
method = "scram-sha-256"    # Recommended for production
username = "falcon"
password = "${FALCON_DB_PASSWORD}"  # Use env var, never hardcode
```

### Auth Rate Limiting (Brute-Force Protection)

The `AuthRateLimiter` tracks failed authentication attempts per source IP and enforces lockout after repeated failures.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_failures` | 5 | Max failed attempts before lockout |
| `lockout_duration` | 300s (5 min) | Duration of lockout after threshold |
| `failure_window` | 600s (10 min) | Window for counting failures |
| `per_ip` | true | Track failures per IP (vs global) |

**Behavior:**
- After `max_failures` within `failure_window`, the source IP is locked out for `lockout_duration`
- Successful authentication clears the failure counter
- Lockout expiry automatically resets the counter
- Observable via `SHOW falcon.security_audit` (auth.* rows)

---

## Authorization (RBAC)

### Role Model

| Role Type | Properties |
|-----------|------------|
| Superuser | Bypasses all privilege checks, can execute dangerous statements |
| User | `can_login`, subject to privilege checks |
| Group | Role membership for transitive privilege inheritance |

### Privileges

| Privilege | Applies To | Description |
|-----------|-----------|-------------|
| `SELECT` | Table, View | Read data |
| `INSERT` | Table | Insert rows |
| `UPDATE` | Table | Update rows |
| `DELETE` | Table | Delete rows |
| `CREATE` | Schema, Database | Create objects |
| `DROP` | Table, Schema | Drop objects |
| `ALTER` | Table, Schema | Alter objects |
| `EXECUTE` | Function | Execute functions |
| `USAGE` | Schema | Access schema |
| `ALL` | Any | All privileges |

### Privilege Resolution

1. Check direct grants on the object for the role
2. Check transitive role memberships (group inheritance)
3. Superuser bypass (always allowed)

```sql
-- Grant examples
GRANT SELECT ON users TO analyst_role;
GRANT ALL ON SCHEMA public TO admin_role;
GRANT analyst_role TO alice;  -- Role membership
```

---

## Password Policy

The `PasswordPolicy` enforces password complexity and lifecycle requirements.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_length` | 8 | Minimum password length |
| `require_uppercase` | true | At least one uppercase letter |
| `require_lowercase` | true | At least one lowercase letter |
| `require_digit` | true | At least one digit |
| `require_special` | false | At least one special character |
| `max_age_days` | 90 | Password expiry (0 = no expiry) |
| `history_count` | 3 | Previous passwords remembered |

**Validation errors** return specific reasons (e.g., "Password must be at least 8 characters").

---

## SQL Firewall

The `SqlFirewall` provides runtime SQL statement inspection to detect and block malicious queries.

### Protection Layers

| Layer | Default | Description |
|-------|---------|-------------|
| **Injection Detection** | Enabled | Detects tautology, UNION, comment, time-based injection |
| **Dangerous Statement Blocking** | Enabled | Blocks DROP DATABASE, TRUNCATE, ALTER SYSTEM for non-superusers |
| **Statement Stacking** | Enabled | Blocks multiple statements separated by `;` |
| **Length Limit** | 1 MB | Maximum SQL statement length |
| **Custom Patterns** | Empty | User-defined blocked patterns |

### Injection Patterns Detected

| Pattern | Example | Detection |
|---------|---------|-----------|
| Tautology | `OR 1=1`, `OR '1'='1'`, `OR TRUE` | Always-true conditions |
| UNION injection | `UNION SELECT ... WHERE ...` | UNION with WHERE clause |
| Comment truncation | `WHERE name='admin' -- AND pw='x'` | `--` after WHERE |
| Time-based blind | `pg_sleep(10)`, `BENCHMARK(...)` | Sleep/delay functions |
| Statement stacking | `SELECT 1; DROP TABLE users` | Multiple statements |

### Superuser Bypass

Superusers bypass dangerous statement blocking (they need DROP DATABASE, etc.) but **not** injection detection. Injection patterns are always flagged regardless of privilege level.

---

## Transport Security (TLS)

### Configuration

```toml
[security.tls]
enabled = true
cert_path = "/etc/falcon/server.crt"
key_path = "/etc/falcon/server.key"
ca_cert_path = "/etc/falcon/ca.crt"    # For mTLS
require_client_cert = false             # Set true for mTLS
min_version = "1.2"                     # Minimum TLS version
```

### Recommendations

| Environment | TLS | mTLS | Min Version |
|-------------|-----|------|-------------|
| Development | Optional | No | 1.2 |
| Staging | Required | Optional | 1.2 |
| Production | Required | Recommended | 1.3 |

---

## Encryption at Rest

### Configuration

```toml
[security.encryption]
enabled = true
algorithm = "AES-256-GCM"
key_id = "falcon-master-key-1"
encrypt_wal = true
encrypt_snapshots = true
```

### KMS Integration

| Provider | Config Value | Description |
|----------|-------------|-------------|
| Local | `local` | Local key file (development) |
| AWS KMS | `aws-kms` | AWS Key Management Service |
| GCP KMS | `gcp-kms` | Google Cloud KMS |
| Azure Key Vault | `azure-keyvault` | Azure Key Vault |

---

## IP Allowlist

When enabled, only listed IP addresses can connect.

```sql
-- View current security posture
SHOW falcon.security;
```

---

## Audit Log

### Audited Events

| Event Type | Description |
|------------|-------------|
| `LOGIN` | Successful authentication |
| `LOGOUT` | Session disconnect |
| `AUTH_FAILURE` | Failed authentication attempt |
| `DDL` | CREATE, ALTER, DROP statements |
| `PRIVILEGE_CHANGE` | GRANT, REVOKE operations |
| `ROLE_CHANGE` | CREATE ROLE, ALTER ROLE, DROP ROLE |
| `CONFIG_CHANGE` | SET, ALTER SYSTEM |

### Design

- **Off-hot-path**: `record()` sends to a bounded channel (non-blocking, O(1))
- **Background drain**: Dedicated thread drains channel into ring buffer
- **Backpressure**: When channel full, events are dropped (counter incremented)
- **Capacity**: 4,096 events in ring buffer, 8,192 channel capacity

```sql
-- View recent audit events
SHOW falcon.audit_log;
```

---

## Observability

### SHOW Commands

| Command | Description |
|---------|-------------|
| `SHOW falcon.security` | Security posture (TLS, encryption, IP allowlist) |
| `SHOW falcon.security_audit` | Auth rate limiter, password policy, SQL firewall metrics |
| `SHOW falcon.audit_log` | Recent audit log entries |

### Prometheus Metrics

| Metric | Description |
|--------|-------------|
| `falcon_security_auth_checks` | Total auth rate limit checks |
| `falcon_security_auth_lockouts` | Total lockout events |
| `falcon_security_auth_failures` | Total auth failures recorded |
| `falcon_security_auth_active_lockouts` | Currently active lockouts |
| `falcon_security_password_checks` | Total password policy checks |
| `falcon_security_password_rejections` | Passwords rejected by policy |
| `falcon_security_firewall_checks` | Total SQL firewall checks |
| `falcon_security_firewall_blocked` | Total statements blocked |
| `falcon_security_firewall_injection` | SQL injection patterns detected |
| `falcon_security_firewall_dangerous` | Dangerous statements blocked |
| `falcon_security_firewall_stacking` | Statement stacking blocked |

---

## Security Checklist (Production)

| # | Item | Verify |
|---|------|--------|
| 1 | Auth method is `scram-sha-256` or `md5` (not `trust`) | `falcon.toml` |
| 2 | TLS enabled with min version 1.2+ | `SHOW falcon.security` |
| 3 | Password policy enforced (min 8 chars, complexity) | `SHOW falcon.security_audit` |
| 4 | Auth rate limiting active (max 5 failures) | `SHOW falcon.security_audit` |
| 5 | SQL firewall enabled (injection + dangerous blocking) | `SHOW falcon.security_audit` |
| 6 | Audit log enabled | `SHOW falcon.audit_log` |
| 7 | IP allowlist configured (if applicable) | `SHOW falcon.security` |
| 8 | Encryption at rest enabled (if applicable) | `SHOW falcon.security` |
| 9 | No superuser used for application connections | Role catalog |
| 10 | Passwords not hardcoded in config (use env vars) | `falcon.toml` |
