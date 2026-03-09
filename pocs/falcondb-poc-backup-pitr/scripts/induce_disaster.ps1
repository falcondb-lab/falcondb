#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Induce Disaster (Windows)
.DESCRIPTION
    Deliberately destroys the live database. Irreversible.
#>

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$DataDir   = Join-Path $PocRoot "pitr_data"
$PidFile   = Join-Path $OutputDir "falcon.pid"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  DISASTER SIMULATION`n" -ForegroundColor Red
Write-Host "  WARNING: This will destroy the live database" -ForegroundColor Red
Write-Host "  Data directory will be WIPED COMPLETELY." -ForegroundColor Red
Write-Host "  The only recovery is from backup + WAL.`n" -ForegroundColor Red

# Stop FalconDB
Info "Stopping FalconDB..."
if (Test-Path $PidFile) {
    $procId = [int](Get-Content $PidFile).Trim()
    try {
        Stop-Process -Id $procId -Force -ErrorAction Stop
        Start-Sleep -Seconds 1
        Ok "FalconDB killed (pid $procId)"
    } catch {
        Info "FalconDB (pid $procId) already stopped"
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

# DESTROY data directory
Info "Destroying data directory..."
if (Test-Path $DataDir) {
    Remove-Item -Recurse -Force $DataDir
    Ok "Data directory DESTROYED: $DataDir"
} else {
    Info "Data directory already gone"
}

if (Test-Path $DataDir) {
    Write-Host "  ERROR: Data directory still exists!" -ForegroundColor Red
    exit 1
}

$disasterTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Add-Content (Join-Path $OutputDir "event_timeline.txt") "$disasterTs | DISASTER | data_dir=$DataDir destroyed" -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "  Database DESTROYED at $disasterTs" -ForegroundColor Red
Write-Host ""
Write-Host "  The ONLY way to recover is:"
Write-Host "    1. Restore from backup  (restore_from_backup.ps1)"
Write-Host "    2. Replay WAL to T1     (replay_wal_until.ps1)"
Write-Host ""
