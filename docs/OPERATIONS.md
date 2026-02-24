# FalconDB — Operations Guide

## CLI Reference

### Server Commands

| Command | Description |
|---------|-------------|
| `falcon` | Start server in console mode (foreground) |
| `falcon --config <path>` | Start with specific config file |
| `falcon version` | Show version and build information |
| `falcon status` | Show service status, PID, ports |
| `falcon doctor` | Run diagnostic checks |

### Service Commands

| Command | Description |
|---------|-------------|
| `falcon service install` | Register Windows Service |
| `falcon service uninstall` | Remove Windows Service |
| `falcon service start` | Start the service |
| `falcon service stop` | Stop the service (graceful) |
| `falcon service restart` | Stop then start |
| `falcon service status` | Show service state + details |

### Config Commands

| Command | Description |
|---------|-------------|
| `falcon config check` | Check config version compatibility |
| `falcon config migrate` | Migrate config to current schema |
| `falcon --print-default-config` | Dump default config as TOML |

### Data Commands

| Command | Description |
|---------|-------------|
| `falcon purge` | Delete all ProgramData (with confirmation) |
| `falcon purge --yes` | Delete all ProgramData (no prompt) |

---

## Service Management

### Starting and Stopping

```powershell
# Via falcon CLI
falcon service start
falcon service stop
falcon service restart

# Via sc.exe
sc start FalconDB
sc stop FalconDB

# Via services.msc
# Search for "FalconDB Database Server"
```

### Checking Status

```powershell
falcon service status
# Output:
#   Service:  FalconDB
#   State:    RUNNING
#   PID:      1234
#   Config:   C:\ProgramData\FalconDB\conf\falcon.toml
#   Data:     C:\ProgramData\FalconDB\data\
#   Logs:     C:\ProgramData\FalconDB\logs\
#   Ports:
#     PG:     5443
#     Admin:  8080
```

### Recovery Policy

The service is configured with automatic restart on failure:

| Failure # | Action | Delay |
|-----------|--------|-------|
| 1st | Restart | 5 seconds |
| 2nd | Restart | 10 seconds |
| 3rd+ | Restart | 30 seconds |

The failure counter resets after 24 hours of stable operation.

---

## Monitoring

### Health Endpoint

```
GET http://127.0.0.1:8080/health    → 200 OK (healthy) or 503 (unhealthy)
GET http://127.0.0.1:8080/ready     → 200 OK (ready) or 503 (not ready)
GET http://127.0.0.1:8080/status    → JSON status object
```

### Prometheus Metrics

Available at `http://127.0.0.1:9090/metrics` (default).

Key metrics:
- `falcon_connections_active` — Current active connections
- `falcon_queries_total` — Total queries processed
- `falcon_storage_bytes` — Storage utilization
- `falcon_txn_active` — Active transactions

### SQL-Based Monitoring

```sql
SHOW falcon.status;           -- Server status
SHOW falcon.connections;      -- Active connections
SHOW falcon.replica_stats;    -- Replication lag (if replica)
SHOW falcon.security;         -- Security posture
SHOW falcon.audit_log;        -- Recent audit events
```

---

## Logging

### Log Location

| Mode | Location |
|------|----------|
| Console | stderr |
| Service | `C:\ProgramData\FalconDB\logs\falcon.log` |

### Log Rotation

Service mode uses daily rotation. Log files are named:
```
falcon.log              ← current day
falcon.log.2025-02-24   ← previous days
```

### Viewing Logs

```powershell
# Last 100 lines
Get-Content C:\ProgramData\FalconDB\logs\falcon.log -Tail 100

# Follow in real-time
Get-Content C:\ProgramData\FalconDB\logs\falcon.log -Tail 50 -Wait

# Search for errors
Select-String "ERROR" C:\ProgramData\FalconDB\logs\falcon.log
```

### Log Content

Startup log includes:
- Run mode (console / service)
- Config file path
- Port bindings (PG, admin, gRPC)
- Node role (standalone, primary, replica)
- WAL sync mode
- Memory budget

Shutdown log includes:
- Shutdown reason (ctrl_c, sigterm, service_stop, requested)
- Server task teardown order
- Port release confirmation

---

## Diagnostics

### falcon doctor

```powershell
falcon doctor --config C:\ProgramData\FalconDB\conf\falcon.toml
```

Checks:
- Config file exists and is readable
- Ports 5443 and 8080 are available (or already bound by FalconDB)
- Data directory is writable
- Windows Service registration status
- ProgramData directory structure

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Port 5443 in use | Another process | `Get-NetTCPConnection -LocalPort 5443` |
| Service won't start | Config error | Check logs, run `falcon doctor` |
| Permission denied | Wrong service account | Verify ProgramData ACLs |
| Config version mismatch | After upgrade | `falcon config migrate` |
| Data corruption | Unclean shutdown | Check WAL recovery in logs |

---

## SLA Support Information

### Key Operational Queries

| Question | How to Answer |
|----------|---------------|
| Last startup time? | `Get-Content logs\falcon.log \| Select-String "Starting FalconDB" \| Select-Object -Last 1` |
| Last shutdown reason? | `Get-Content logs\falcon.log \| Select-String "shutdown" \| Select-Object -Last 1` |
| Current version? | `falcon version` |
| Data directory size? | `(Get-ChildItem -Recurse C:\ProgramData\FalconDB\data \| Measure-Object Length -Sum).Sum / 1MB` |
| Uptime? | Service start time in `falcon service status` |
| Config version? | `falcon config check` |

### Backup Strategy

```powershell
# Stop service for consistent backup
falcon service stop

# Copy data directory
$ts = Get-Date -Format "yyyyMMdd"
Copy-Item -Recurse "C:\ProgramData\FalconDB\data" "D:\backup\falcon_data_$ts"
Copy-Item "C:\ProgramData\FalconDB\conf\falcon.toml" "D:\backup\falcon.toml.$ts"

# Restart
falcon service start
```

### Capacity Planning

Monitor these metrics for capacity alerts:
- Disk: `C:\ProgramData\FalconDB\data\` growth rate
- Memory: Process working set (Task Manager or `Get-Process falcon`)
- Connections: `falcon_connections_active` vs `max_connections`
- Logs: `C:\ProgramData\FalconDB\logs\` rotation and retention

---

## Directory Reference

```
C:\Program Files\FalconDB\           ← MSI-managed (read-only)
  └── bin\
      └── falcon.exe

C:\ProgramData\FalconDB\             ← Data root (preserved on uninstall)
  ├── conf\
  │   └── falcon.toml                ← Configuration
  ├── data\                          ← Database storage + WAL
  │   ├── falcon.wal                 ← Write-ahead log
  │   └── ...                        ← Table data files
  ├── logs\                          ← Rotating log files
  │   ├── falcon.log                 ← Current log
  │   └── falcon.log.YYYY-MM-DD     ← Historical logs
  ├── certs\                         ← TLS certificates (optional)
  │   ├── server.crt
  │   ├── server.key
  │   └── ca.crt
  └── backup\                        ← Upgrade backups (auto-created)
      └── YYYYMMDD_HHMMSS\
          ├── falcon.exe
          └── falcon.toml
```
