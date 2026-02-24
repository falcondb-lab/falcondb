# FalconDB — Upgrade Guide

## Upgrade Philosophy

FalconDB upgrades follow these invariants:

1. **Data is never deleted** — upgrades only touch program files
2. **Config is never overwritten** — existing `falcon.toml` is preserved
3. **Rollback is always possible** — automatic on failure, manual backup available
4. **Downtime is minimized** — stop → replace → start (seconds, not minutes)

## Version Compatibility

| From | To | Method | Config Migration |
|------|----|--------|-----------------|
| 1.0.x | 1.0.y | MSI or manual | No |
| 1.0.x | 1.1.x | MSI or manual | Automatic |
| 0.x | 1.x | Manual + migrate | `falcon config migrate` |

## Method 1: MSI Upgrade (Recommended)

The MSI supports **major upgrade** — installing a new MSI automatically removes
the previous version and preserves all data.

```powershell
# Interactive
msiexec /i FalconDB-1.1.0-x64.msi

# Silent with log
msiexec /i FalconDB-1.1.0-x64.msi /qn /l*v upgrade.log
```

The MSI will automatically:
1. Stop the FalconDB service
2. Remove old program files
3. Install new program files
4. Preserve `C:\ProgramData\FalconDB\` (config, data, logs)
5. Start the service with the new binary

### Verify After MSI Upgrade

```powershell
falcon version                    # confirm new version
falcon service status             # confirm service running
falcon config check               # confirm config compatible
```

## Method 2: Automated Script Upgrade

For environments where MSI is not preferred:

```powershell
# Using new MSI
.\scripts\upgrade_falcondb.ps1 -NewMsi ".\FalconDB-1.1.0-x64.msi"

# Using new binary directly
.\scripts\upgrade_falcondb.ps1 -NewExe ".\falcon.exe"
```

The script:
1. Creates a timestamped backup in `C:\ProgramData\FalconDB\backup\`
2. Stops the service
3. Replaces the binary
4. Runs `falcon config migrate` if needed
5. Starts the service
6. Verifies health (PG port listening)
7. **On failure: automatically rolls back to the previous version**

## Method 3: Manual Upgrade

```powershell
# 1. Check current version
falcon version

# 2. Create backup
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$backupDir = "C:\ProgramData\FalconDB\backup\$ts"
New-Item -ItemType Directory -Path $backupDir -Force
Copy-Item "C:\Program Files\FalconDB\bin\falcon.exe" "$backupDir\falcon.exe"
Copy-Item "C:\ProgramData\FalconDB\conf\falcon.toml" "$backupDir\falcon.toml"

# 3. Stop service
falcon service stop
# Wait for ports to be released
Start-Sleep -Seconds 5

# 4. Replace binary
Copy-Item ".\new-falcon.exe" "C:\Program Files\FalconDB\bin\falcon.exe" -Force

# 5. Migrate config (if version changed)
falcon config check --config "C:\ProgramData\FalconDB\conf\falcon.toml"
falcon config migrate --config "C:\ProgramData\FalconDB\conf\falcon.toml"

# 6. Start service
falcon service start

# 7. Verify
falcon version
falcon service status
psql -h 127.0.0.1 -p 5443 -U falcon -c "SELECT 1"
```

## Config Migration

FalconDB configs include a `config_version` field. On upgrade, the config
schema may change. The migration system handles this automatically:

```powershell
# Check if migration is needed
falcon config check --config "C:\ProgramData\FalconDB\conf\falcon.toml"

# Apply migration (creates .bak backup automatically)
falcon config migrate --config "C:\ProgramData\FalconDB\conf\falcon.toml"
```

Migration is:
- **Sequential**: v1→v2→v3 (never skips versions)
- **Non-destructive**: creates `.toml.bak` before modifying
- **Idempotent**: running twice has no additional effect

## Rollback

### Automatic Rollback (Script Upgrade)

If `upgrade_falcondb.ps1` detects the service failed to start after upgrade,
it automatically:
1. Stops the failed service
2. Restores the backed-up binary and config
3. Restarts the service with the old version

### Manual Rollback

```powershell
# 1. Stop service
falcon service stop

# 2. Restore from backup
$backupDir = "C:\ProgramData\FalconDB\backup\<timestamp>"
Copy-Item "$backupDir\falcon.exe" "C:\Program Files\FalconDB\bin\falcon.exe" -Force
Copy-Item "$backupDir\falcon.toml" "C:\ProgramData\FalconDB\conf\falcon.toml" -Force

# 3. Start service
falcon service start
```

### MSI Rollback

If the new MSI fails during installation, Windows Installer automatically
rolls back the transaction — no manual intervention needed.

## Upgrade Log

All upgrades are logged to:
- MSI log: specified via `/l*v` flag
- Service log: `C:\ProgramData\FalconDB\logs\falcon.log`
- Script log: `C:\ProgramData\FalconDB\logs\upgrade_<timestamp>.log`

Check logs after any upgrade:

```powershell
Get-Content C:\ProgramData\FalconDB\logs\falcon.log -Tail 50
```
