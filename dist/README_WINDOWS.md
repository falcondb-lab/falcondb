# FalconDB for Windows — Quick Start Guide

## What's Inside

```
FalconDB/
  bin/falcon.exe          ← Database server
  conf/falcon.toml        ← Configuration (edit this)
  data/                   ← Database files (WAL, snapshots)
  logs/                   ← Log output directory
  scripts/                ← Service install/uninstall scripts
  README_WINDOWS.md       ← This file
  VERSION                 ← Build version info
```

## Prerequisites

- Windows 10 / Server 2016 or later (x86_64)
- No additional runtime or installer required

## 1. Foreground Mode (Recommended for First Run)

Open PowerShell, navigate to the FalconDB directory, and run:

```powershell
cd C:\Path\To\FalconDB
.\bin\falcon.exe --config .\conf\falcon.toml
```

You should see:

```
FalconDB PG server listening on 0.0.0.0:5443
Health check server listening on 0.0.0.0:8080
```

### Connect with psql

```powershell
psql -h 127.0.0.1 -p 5443 -U falcon
```

### Stop the Server

Press **Ctrl+C** in the terminal. FalconDB will:
1. Stop accepting new connections
2. Drain active connections (up to 30s)
3. Flush the WAL
4. Release all ports (5443, 8080)

## 2. Install as Windows Service

Run PowerShell **as Administrator**:

```powershell
cd C:\Path\To\FalconDB
.\scripts\install_service.ps1
```

This creates the service but does **not** start it.

### Start / Stop / Status

```powershell
sc start FalconDB
sc query FalconDB
sc stop FalconDB
```

### Uninstall the Service

```powershell
.\scripts\uninstall_service.ps1
```

## 3. Configuration

Edit `conf\falcon.toml` to change:

| Setting | Default | Description |
|---------|---------|-------------|
| `server.pg_listen_addr` | `0.0.0.0:5443` | PostgreSQL wire protocol port |
| `server.admin_listen_addr` | `0.0.0.0:8080` | Health check / admin HTTP port |
| `storage.data_dir` | `data` | Data directory (relative to FalconDB root) |
| `storage.wal_enabled` | `true` | Enable WAL for crash recovery |
| `server.max_connections` | `1024` | Max concurrent client connections |
| `server.auth.method` | `trust` | Auth: `trust`, `password`, `md5`, `scram-sha-256` |

## 4. Ports & Firewall

FalconDB listens on the following ports by default:

| Port | Protocol | Purpose |
|------|----------|---------|
| **5443** | TCP | PostgreSQL wire protocol (client connections) |
| **8080** | TCP | Health check / admin HTTP (`/health`, `/ready`, `/status`) |
| **50051** | TCP | gRPC replication (only in Primary/Replica mode) |

If Windows Firewall blocks connections, allow them:

```powershell
# Run as Administrator
netsh advfirewall firewall add rule name="FalconDB PG" dir=in action=allow protocol=tcp localport=5443
netsh advfirewall firewall add rule name="FalconDB Admin" dir=in action=allow protocol=tcp localport=8080
```

## 5. Health Checks

```powershell
# Liveness (always 200 if process is up)
curl http://127.0.0.1:8080/health

# Readiness (200 when ready to serve queries)
curl http://127.0.0.1:8080/ready

# Detailed status (JSON)
curl http://127.0.0.1:8080/status
```

## 6. Data & Logs

| Directory | Purpose |
|-----------|---------|
| `data/` | WAL segments, snapshots, SST files. **Back up regularly.** |
| `logs/` | Log output (when redirected). |

### Capture Logs to File

```powershell
.\bin\falcon.exe --config .\conf\falcon.toml 2> .\logs\falcon.log
```

### Log Verbosity

```powershell
$env:RUST_LOG = "info"     # Default
$env:RUST_LOG = "debug"    # Verbose
$env:RUST_LOG = "warn"     # Quiet
.\bin\falcon.exe --config .\conf\falcon.toml
```

## 7. Troubleshooting

| Symptom | Fix |
|---------|-----|
| "Address already in use" on startup | Another process is using port 5443/8080. Check with `netstat -ano \| findstr 5443` |
| Port still LISTENING after Ctrl+C | Upgrade to latest build (graceful shutdown fix). Verify with `netstat -ano \| findstr 5443` |
| Service won't start | Check Event Viewer > Windows Logs > Application for errors |
| "Access denied" on install_service.ps1 | Run PowerShell as Administrator |
| psql can't connect | Check firewall rules (see Section 4) |

## 8. Version

See the `VERSION` file in the root directory for build information.

---

*FalconDB — PG-Compatible In-Memory OLTP Database*
