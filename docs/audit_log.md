# Audit Logging

FalconDB provides audit logging for compliance-sensitive workloads, capturing
authentication events, DDL/DML operations, privilege changes, and
administrative actions.

## Configuration

```toml
[audit]
# Enable audit logging (default: false)
enabled = true

# Audit log destination: "file", "syslog", or "both"
destination = "file"

# File path (when destination includes "file")
log_path = "/var/log/falcondb/audit.log"

# Maximum file size before rotation (bytes); 0 = no limit
max_file_size_bytes = 104857600  # 100 MB

# Number of rotated files to retain
max_retained_files = 30

# Events to capture (list of event classes)
# Available: auth, ddl, dml, role, admin, query, connection
events = ["auth", "ddl", "role", "admin"]

# Minimum severity: "info", "warning", "error"
min_severity = "info"

# Include query text in DML/query events (may contain sensitive data)
include_query_text = false

# Filter by database (empty = all databases)
databases = []

# Filter by user (empty = all users)
users = []
```

## Event Classes

| Class | Events Captured |
|-------|----------------|
| `auth` | Login success/failure, SCRAM handshake, auth rate-limit lockout |
| `ddl` | CREATE, ALTER, DROP for tables, indexes, schemas, databases |
| `dml` | INSERT, UPDATE, DELETE (when enabled — high volume) |
| `role` | CREATE ROLE, ALTER ROLE, GRANT, REVOKE |
| `admin` | Configuration changes, CHECKPOINT, VACUUM, server start/stop |
| `query` | All executed queries (when enabled — very high volume) |
| `connection` | Client connect/disconnect, connection pool events |

## Log Format

Each audit record is a JSON line:

```json
{
  "timestamp": "2025-03-01T08:15:30.123456Z",
  "event_class": "auth",
  "event_type": "login_success",
  "severity": "info",
  "session_id": "a1b2c3d4",
  "user": "falcon",
  "database": "mydb",
  "client_addr": "10.0.1.50:54321",
  "auth_method": "scram-sha-256",
  "details": {},
  "request_id": "req-5678",
  "node_id": 1,
  "duration_us": 1234
}
```

## Syslog Export

When `destination = "syslog"` or `"both"`:

```toml
[audit.syslog]
# Syslog server address
address = "127.0.0.1:514"

# Protocol: "udp" (default), "tcp", or "tcp+tls"
protocol = "tcp"

# Syslog facility (default: "local0")
facility = "local0"

# Syslog tag
tag = "falcondb-audit"

# TLS settings (when protocol = "tcp+tls")
# ca_cert = "/etc/falcondb/certs/syslog-ca.crt"
```

## SIEM Integration

### Splunk

Forward the audit log file via Splunk Universal Forwarder:

```
[monitor:///var/log/falcondb/audit.log]
sourcetype = falcondb:audit
index = security
```

### Elastic / ELK

Use Filebeat with a JSON input:

```yaml
filebeat.inputs:
  - type: log
    paths:
      - /var/log/falcondb/audit.log
    json.keys_under_root: true
    json.add_error_key: true
    fields:
      service: falcondb
output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "falcondb-audit-%{+yyyy.MM.dd}"
```

### Datadog

```yaml
logs:
  - type: file
    path: /var/log/falcondb/audit.log
    service: falcondb
    source: falcondb
    log_processing_rules:
      - type: multi_line
        name: json_logs
        pattern: '^\{'
```

## SQL Interface

Query audit events from within FalconDB:

```sql
-- Recent audit events
SHOW falcon.audit_log;

-- Security audit summary
SHOW falcon.security_audit;
```

## Retention & Compliance

- **File rotation**: Controlled by `max_file_size_bytes` and `max_retained_files`
- **External archival**: Use log shipping (Filebeat, Fluentd, etc.) to move
  rotated files to durable storage (S3, GCS, Azure Blob) before they age out
- **Immutability**: Audit log files are append-only; FalconDB does not modify
  or delete past records
- **Tamper detection**: Each record includes a rolling HMAC chain; enable with
  `[audit] integrity_check = true`

## Kubernetes

In K8s deployments, audit logs are written to stdout/stderr by default and
captured by the container runtime. For file-based audit:

```yaml
# values.yaml
falcondb:
  extraConfig: |
    [audit]
    enabled = true
    destination = "file"
    log_path = "/var/lib/falcondb/audit/audit.log"
    events = ["auth", "ddl", "role", "admin"]

persistence:
  enabled: true
```

Mount a separate PVC for audit logs if long-term on-disk retention is needed.
