# FalconDB — Uninstall Guide

## Uninstall Semantics

FalconDB strictly separates **program files** from **data**:

| What | Location | Removed on Uninstall? |
|------|----------|----------------------|
| Binary (`falcon.exe`) | `C:\Program Files\FalconDB\` | **Yes** |
| Windows Service | Registry | **Yes** |
| Config, Data, Logs | `C:\ProgramData\FalconDB\` | **No** |

This protects production data from accidental removal during uninstall or
re-install cycles.

## Method 1: MSI Uninstall (Recommended)

**Interactive:**

```powershell
# Via Add/Remove Programs (Settings → Apps → FalconDB → Uninstall)
# or via command line:
msiexec /x FalconDB-1.0.0-x64.msi
```

**Silent:**

```powershell
msiexec /x FalconDB-1.0.0-x64.msi /qn /l*v uninstall.log
```

**By Product Code (if MSI file not available):**

```powershell
# Find the product code
Get-WmiObject Win32_Product | Where-Object { $_.Name -eq "FalconDB" } | Select-Object IdentifyingNumber

# Uninstall by code
msiexec /x {PRODUCT-CODE-GUID} /qn
```

The MSI uninstall will:
1. Stop the FalconDB service
2. Remove the service registration
3. Delete program files from `C:\Program Files\FalconDB\`
4. **Preserve** `C:\ProgramData\FalconDB\` (config, data, logs, certs)

## Method 2: CLI Uninstall (Green Distribution)

```powershell
falcon service uninstall
```

This removes the Windows Service registration. You must manually delete the
FalconDB directory afterward.

## Complete Removal (Including Data)

After uninstalling the program, use `falcon purge` to remove all data:

```powershell
# If falcon.exe is still available:
falcon purge

# With confirmation bypass:
falcon purge --yes
```

Or manually:

```powershell
# Remove all FalconDB data, config, logs, certificates
Remove-Item -Recurse -Force "$env:ProgramData\FalconDB"
```

## Cleanup Checklist

After complete removal, verify nothing remains:

```powershell
# 1. Service removed?
sc.exe query FalconDB
# Expected: "The specified service does not exist"

# 2. Program files removed?
Test-Path "C:\Program Files\FalconDB"
# Expected: False

# 3. Data removed? (only if you ran purge)
Test-Path "C:\ProgramData\FalconDB"
# Expected: False

# 4. Firewall rules removed?
netsh advfirewall firewall show rule name="FalconDB PG"
netsh advfirewall firewall show rule name="FalconDB Admin"
# Remove if present:
netsh advfirewall firewall delete rule name="FalconDB PG"
netsh advfirewall firewall delete rule name="FalconDB Admin"

# 5. Ports released?
Get-NetTCPConnection -LocalPort 5443 -ErrorAction SilentlyContinue
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue
# Expected: empty
```

## Re-Installation After Uninstall

If you uninstalled but preserved ProgramData, re-installing will:
- Install fresh program files
- **Reuse** the existing config (`falcon.toml` is not overwritten)
- **Reuse** existing data (WAL, tables, etc.)
- Re-register the service

This enables clean binary upgrades without data loss.
