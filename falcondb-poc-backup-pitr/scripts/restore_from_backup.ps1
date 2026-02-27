#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Restore From Backup (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir     = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot       = Split-Path -Parent $ScriptDir
$OutputDir     = Join-Path $PocRoot "output"

$DataDir       = Join-Path $PocRoot "pitr_data"
$ManifestFile  = Join-Path $OutputDir "backup_manifest.json"
$RestoreReport = Join-Path $OutputDir "restore_report.txt"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  Restoring From Backup`n" -ForegroundColor Cyan

if (-not (Test-Path $ManifestFile)) { Fail "Manifest not found: $ManifestFile"; exit 1 }
Ok "Manifest found"

$manifest     = Get-Content $ManifestFile | ConvertFrom-Json
$backupPath   = $manifest.backup_path
$backupLabel  = $manifest.backup_label
$backupRows   = $manifest.row_count
$backupBalance= $manifest.total_balance

Info "Backup label: $backupLabel"
Info "Backup path:  $backupPath"

if (-not (Test-Path $backupPath)) { Fail "Backup directory not found: $backupPath"; exit 1 }
Ok "Backup directory exists"

if (Test-Path $DataDir) { Fail "Data directory still exists — disaster not induced?"; exit 1 }
Ok "Confirmed: data directory is gone (disaster verified)"

Info "Restoring base snapshot..."
$sw = [System.Diagnostics.Stopwatch]::StartNew()
Copy-Item -Recurse -Force $backupPath $DataDir
$sw.Stop()
Ok "Base snapshot restored ($($sw.ElapsedMilliseconds)ms)"

$restoredSize = (Get-ChildItem -Recurse $DataDir | Measure-Object -Property Length -Sum).Sum
Ok "Restored data: $restoredSize bytes"

$restoreTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
@"
Restore Report
==============
Date:            $restoreTs
Backup label:    $backupLabel
Backup path:     $backupPath
Restored to:     $DataDir
Duration:        $($sw.ElapsedMilliseconds)ms
Restored size:   $restoredSize bytes
Expected rows:   $backupRows
Expected balance:$backupBalance

Status: BASE_RESTORED
Note:   WAL replay has NOT been performed yet.
"@ | Set-Content $RestoreReport -Encoding UTF8
Ok "Restore report: $RestoreReport"

Write-Host ""
Write-Host "  Base Restore Complete"
Write-Host "  State: T0 (backup time)"
Write-Host "  Next:  replay_wal_until.ps1 -> T1"
Write-Host ""
