# FalconDB MSI Installer — WiX v4

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| .NET SDK | 8.0+ | https://dotnet.microsoft.com/download |
| WiX v4 | 4.0+ | `dotnet tool install --global wix` |
| WiX Extensions | — | Restored automatically via NuGet |
| Rust toolchain | stable | `rustup default stable` |

## Quick Start

```powershell
# 1. Build falcon.exe (release)
cargo build --release --bin falcon

# 2. Stage distribution files
.\scripts\stage_windows_dist.ps1 -Version 1.2.0

# 3. Build MSI
.\packaging\wix\build.ps1 -Version 1.2.0

# Output: dist\windows\FalconDB-1.2.0-x64.msi
```

## Project Structure

```
packaging/wix/
  FalconDB.wixproj      # WiX v4 project file (dotnet build entry point)
  Variables.wxi          # Shared variables: GUIDs, version, paths, service config
  Product.wxs            # MSI core: Package, MajorUpgrade, Feature tree
  Components.wxs         # Program Files file components (bin, docs)
  ProgramData.wxs        # C:\ProgramData\FalconDB\{conf,data,logs,certs}
  Service.wxs            # Windows Service: install, control, failure recovery
  Upgrade.wxs            # Major Upgrade strategy documentation + tracking
  Firewall.wxs           # Optional firewall rules (5443/TCP, 8080/TCP)
  UI.wxs                 # Minimal UI + silent install support
  CustomActions.wxs      # Placeholder (empty — avoid CAs when possible)
  build.ps1              # One-click MSI build script
  sign.ps1               # Authenticode + MSI signing script
  README.md              # This file
```

## Install Layout

### Program Files (read-only, replaced on upgrade, removed on uninstall)

```
C:\Program Files\FalconDB\
  bin\falcon.exe
  VERSION
  LICENSE
  NOTICE
```

### ProgramData (writable, preserved across upgrades AND uninstalls)

```
C:\ProgramData\FalconDB\
  conf\falcon.toml       # NeverOverwrite: first install writes template, upgrades preserve
  data\                   # Database files — NEVER deleted
  logs\                   # Log files — NEVER deleted
  certs\                  # TLS certificates (optional)
```

## Installation

### Interactive

```powershell
msiexec /i FalconDB-1.2.0-x64.msi /l*v install.log
```

### Silent

```powershell
msiexec /i FalconDB-1.2.0-x64.msi /qn /l*v silent.log
```

### Silent with custom options

```powershell
msiexec /i FalconDB-1.2.0-x64.msi /qn /l*v silent.log ^
    INSTALLFOLDER="D:\FalconDB" ^
    ADD_FIREWALL_RULES=0
```

## Upgrade (A → B)

```powershell
# Simply install the new version — MajorUpgrade handles everything:
#   1. Stop FalconDB service
#   2. Remove old Program Files
#   3. Install new Program Files
#   4. Preserve ProgramData (conf, data, logs)
#   5. Start FalconDB service
msiexec /i FalconDB-1.3.0-x64.msi /l*v upgrade.log
```

## Uninstall

```powershell
msiexec /x FalconDB-1.2.0-x64.msi /l*v uninstall.log
```

**After uninstall:**
- ✅ `C:\Program Files\FalconDB\` — **removed**
- ✅ `C:\ProgramData\FalconDB\` — **preserved** (data, config, logs)
- ✅ FalconDB service — **removed**
- ✅ Firewall rules — **removed**

## Windows Service

```powershell
sc query FalconDB          # Check status
sc start FalconDB          # Start
sc stop FalconDB           # Stop
Get-Service FalconDB       # PowerShell alternative
```

**Service details:**
- Name: `FalconDB`
- Display: `FalconDB Database Server`
- Start type: Automatic
- Account: LocalSystem
- Arguments: `--mode service --config "C:\ProgramData\FalconDB\conf\falcon.toml"`
- Recovery: Restart on failure (3 attempts, reset after 24h)

## Signing (Optional)

```powershell
# With PFX certificate
.\packaging\wix\sign.ps1 `
    -MsiPath dist\windows\FalconDB-1.2.0-x64.msi `
    -PfxPath certs\codesign.pfx `
    -PfxPassword $env:PFX_PASSWORD

# With certificate store thumbprint
.\packaging\wix\sign.ps1 `
    -MsiPath dist\windows\FalconDB-1.2.0-x64.msi `
    -Thumbprint "AABBCCDD..."
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| UpgradeCode is fixed forever | Enables MajorUpgrade detection across all versions |
| ProgramData components are `Permanent="yes"` | Data/config survive uninstall |
| Config uses `NeverOverwrite="yes"` | User edits preserved on upgrade |
| Service managed by MSI (not scripts) | Atomic install/rollback; no orphaned services |
| Minimal CustomActions | Fewer failure points; better rollback |
| Firewall rules are optional Feature | Enterprise networks may have their own policies |

## Acceptance Matrix

### A) Install / Uninstall

| Step | Command | Verify |
|------|---------|--------|
| Install | `msiexec /i FalconDB.msi /l*v install.log` | `dir "C:\Program Files\FalconDB"` |
| Service running | — | `sc query FalconDB` → RUNNING |
| PG port open | — | `Test-NetConnection -ComputerName localhost -Port 5443` |
| HTTP port open | — | `Invoke-WebRequest http://localhost:8080/health` |
| Uninstall | `msiexec /x FalconDB.msi /l*v uninstall.log` | Program Files removed |
| Data preserved | — | `dir "C:\ProgramData\FalconDB"` → still exists |

### B) Silent Install

| Step | Command |
|------|---------|
| Install | `msiexec /i FalconDB.msi /qn /l*v silent.log` |
| Verify | `sc query FalconDB` → RUNNING |

### C) Upgrade (A → B)

| Step | Verify |
|------|--------|
| Install v1.2.0 | Service running |
| Modify `falcon.toml` | Add a comment line |
| Install v1.3.0 | Service running, `falcon.toml` unchanged |
| Check data dir | Files preserved |
| Check ports | 5443 + 8080 available |

### D) Rollback

| Step | Verify |
|------|--------|
| Rename `falcon.exe` in staging (simulate missing file) | |
| Attempt install | MSI rolls back |
| Check service | Not registered (clean state) |
| Check Program Files | Empty / removed |

## Troubleshooting

### MSI log analysis

```powershell
# Always install with logging:
msiexec /i FalconDB.msi /l*v install.log

# Search for errors:
Select-String "Return value 3" install.log
Select-String "ERROR" install.log
```

### Service won't start

```powershell
# Check Windows Event Log:
Get-EventLog -LogName Application -Source FalconDB -Newest 10

# Check config:
& "C:\Program Files\FalconDB\bin\falcon.exe" --config "C:\ProgramData\FalconDB\conf\falcon.toml" --validate

# Check port conflicts:
netstat -ano | findstr "5443"
netstat -ano | findstr "8080"
```
