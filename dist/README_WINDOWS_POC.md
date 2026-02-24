# FalconDB for Windows — PoC Deployment Guide

This guide answers the 5 questions every Windows admin asks during a PoC.

---

## 1. What Is FalconDB's Running Mode?

FalconDB has two running modes:

| Mode | How | Who |
|------|-----|-----|
| **Console** | `falcon.exe --config conf\falcon.toml` | Developers, debugging |
| **Service** | `falcon service install` + `sc start FalconDB` | Production, PoC |

In **console mode**, logs go to stderr. Press Ctrl+C to stop.

In **service mode**, FalconDB registers with the Windows Service Control Manager (SCM).
Logs go to `C:\ProgramData\FalconDB\logs\falcon.log`. SCM handles start/stop/restart.

---

## 2. How Do I Install, Start, and Stop the Service?

Open PowerShell **as Administrator**:

```powershell
cd C:\Path\To\FalconDB

# Install (creates the service, does NOT start it)
.\bin\falcon.exe service install --config .\conf\falcon.toml

# Start
.\bin\falcon.exe service start
# or: sc start FalconDB

# Check status
.\bin\falcon.exe service status

# Stop
.\bin\falcon.exe service stop
# or: sc stop FalconDB

# Restart
.\bin\falcon.exe service restart

# Uninstall (stops if running, then removes)
.\bin\falcon.exe service uninstall
```

You can also manage the service from `services.msc` (search "FalconDB Database").

### Quick Console Test (No Service)

```powershell
.\bin\falcon.exe --config .\conf\falcon.toml
# Press Ctrl+C to stop
```

---

## 3. Where Are Data and Logs?

### Console Mode (Relative Paths)

| Item | Location |
|------|----------|
| Config | `.\conf\falcon.toml` |
| Data | `.\data\` |
| Logs | stderr (redirect with `2> .\logs\falcon.log`) |

### Service Mode (Fixed ProgramData Paths)

| Item | Location |
|------|----------|
| Config | `C:\ProgramData\FalconDB\conf\falcon.toml` |
| Data | `C:\ProgramData\FalconDB\data\` |
| Logs | `C:\ProgramData\FalconDB\logs\falcon.log` |

The `service install` command copies your config to ProgramData and patches
`data_dir` to the absolute ProgramData path automatically.

### View Service Logs

```powershell
Get-Content C:\ProgramData\FalconDB\logs\falcon.log -Tail 100
# or follow in real-time:
Get-Content C:\ProgramData\FalconDB\logs\falcon.log -Tail 50 -Wait
```

---

## 4. Does It Auto-Restart After a Crash?

**Yes.** The service is registered with failure recovery:

| Failure # | Action | Delay |
|-----------|--------|-------|
| 1st | Restart | 5 seconds |
| 2nd | Restart | 10 seconds |
| 3rd+ | Restart | 30 seconds |

The failure counter resets after 24 hours of stable operation.

You can verify this in `services.msc` → FalconDB Database → Properties → Recovery tab.

---

## 5. How Do I Completely Uninstall After the PoC?

```powershell
# 1. Remove the service
.\bin\falcon.exe service uninstall

# 2. Remove ProgramData (config, data, logs)
Remove-Item -Recurse -Force "$env:ProgramData\FalconDB"

# 3. Remove the FalconDB directory
cd ..
Remove-Item -Recurse -Force "C:\Path\To\FalconDB"

# 4. Remove firewall rules (if you added them)
netsh advfirewall firewall delete rule name="FalconDB PG"
netsh advfirewall firewall delete rule name="FalconDB Admin"
```

After this, FalconDB leaves zero traces on the system.

---

## Ports & Firewall

| Port | Protocol | Purpose |
|------|----------|---------|
| **5443** | TCP | PostgreSQL wire protocol (client connections) |
| **8080** | TCP | Health/admin HTTP (`/health`, `/ready`, `/status`) |
| **50051** | TCP | gRPC replication (Primary/Replica mode only) |

Allow through Windows Firewall:

```powershell
netsh advfirewall firewall add rule name="FalconDB PG" dir=in action=allow protocol=tcp localport=5443
netsh advfirewall firewall add rule name="FalconDB Admin" dir=in action=allow protocol=tcp localport=8080
```

---

## Diagnostics

Run the built-in doctor to check readiness:

```powershell
.\bin\falcon.exe doctor --config .\conf\falcon.toml
```

This checks:
- Config file exists and is valid
- Ports 5443/8080 are available
- Data directory is writable
- Service installation status
- ProgramData directory status

---

## PoC Verification Script

Run the full automated verification (as Administrator):

```powershell
.\scripts\poc_check_windows.ps1
```

This tests console mode, service mode, port release, and file system state.

---

## Connecting to FalconDB

```powershell
# Using psql
psql -h 127.0.0.1 -p 5443 -U falcon

# Using any PostgreSQL client/driver
# Host: 127.0.0.1, Port: 5443, User: falcon, Database: falcon
```

---

*FalconDB v1.0.0-rc.1 — PG-Compatible In-Memory OLTP Database*
