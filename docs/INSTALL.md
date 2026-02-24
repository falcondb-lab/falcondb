# FalconDB — Installation Guide

## System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| OS | Windows Server 2016+ / Windows 10+ (x64) | Windows Server 2022 |
| RAM | 2 GB | 8 GB+ |
| Disk | 1 GB free | SSD, 50 GB+ |
| .NET | Not required (standalone exe) | — |
| Ports | 5443 (PG), 8080 (Admin) | + 50051 (gRPC replication) |

## Installation Methods

### Method 1: MSI Installer (Recommended for Production)

**Interactive install:**

```powershell
msiexec /i FalconDB-1.0.0-x64.msi
```

**Silent install (enterprise deployment):**

```powershell
msiexec /i FalconDB-1.0.0-x64.msi /qn /l*v install.log
```

**Custom install directory:**

```powershell
msiexec /i FalconDB-1.0.0-x64.msi INSTALLDIR="D:\FalconDB"
```

The MSI installer will:
1. Install `falcon.exe` to `C:\Program Files\FalconDB\bin\`
2. Create data directories under `C:\ProgramData\FalconDB\`
3. Deploy default `falcon.toml` to `C:\ProgramData\FalconDB\conf\`
4. Register the **FalconDB** Windows Service (auto-start)
5. Start the service

### Method 2: Green Distribution (ZIP)

For evaluation or development:

```powershell
Expand-Archive FalconDB-windows-x86_64.zip -DestinationPath C:\FalconDB
cd C:\FalconDB

# Console mode (foreground)
.\bin\falcon.exe --config .\conf\falcon.toml

# Or install as service
.\bin\falcon.exe service install --config .\conf\falcon.toml
.\bin\falcon.exe service start
```

## Post-Installation Verification

```powershell
# Check version
& "C:\Program Files\FalconDB\bin\falcon.exe" version

# Check service status
& "C:\Program Files\FalconDB\bin\falcon.exe" service status

# Run diagnostics
& "C:\Program Files\FalconDB\bin\falcon.exe" doctor

# Connect with psql
psql -h 127.0.0.1 -p 5443 -U falcon
```

## Directory Layout

```
C:\Program Files\FalconDB\       ← Read-only program files (MSI managed)
  └── bin\
      └── falcon.exe

C:\ProgramData\FalconDB\         ← Read-write data (preserved on uninstall)
  ├── conf\
  │   └── falcon.toml            ← Configuration
  ├── data\                      ← Database storage + WAL
  ├── logs\                      ← Rotating log files
  └── certs\                     ← TLS certificates (optional)
```

## Firewall Configuration

If clients connect from remote hosts:

```powershell
netsh advfirewall firewall add rule name="FalconDB PG" dir=in action=allow protocol=tcp localport=5443
netsh advfirewall firewall add rule name="FalconDB Admin" dir=in action=allow protocol=tcp localport=8080
```

## Configuration

Edit `C:\ProgramData\FalconDB\conf\falcon.toml` to customize:

- `server.pg_listen_addr` — Client connection address (default: `0.0.0.0:5443`)
- `server.admin_listen_addr` — Health/admin endpoint (default: `0.0.0.0:8080`)
- `storage.data_dir` — Database files location
- `server.max_connections` — Connection limit (default: 1024)
- `server.auth.method` — Authentication (`trust`, `password`, `scram-sha-256`)

After changing configuration, restart the service:

```powershell
falcon service restart
```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Service won't start | `falcon doctor --config ...` |
| Port conflict | `Get-NetTCPConnection -LocalPort 5443` |
| Config error | `falcon config check --config ...` |
| Permission denied | Run as Administrator |
| Logs | `Get-Content C:\ProgramData\FalconDB\logs\falcon.log -Tail 100` |
