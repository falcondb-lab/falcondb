#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Cleanup (Windows)
.PARAMETER All
    Also remove data, backups, WAL archive, and output files.
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$pidFile = Join-Path $OutputDir "falcon.pid"
if (Test-Path $pidFile) {
    $procId = [int](Get-Content $pidFile).Trim()
    try {
        Stop-Process -Id $procId -Force -ErrorAction Stop
        Ok "Stopped FalconDB (pid $procId)"
    } catch {
        Info "FalconDB (pid $procId) already stopped"
    }
    Remove-Item $pidFile -Force
}

if ($All) {
    Info "Removing data directory..."
    Remove-Item -Recurse -Force (Join-Path $PocRoot "pitr_data") -ErrorAction SilentlyContinue
    Ok "Data removed"

    Info "Removing backups..."
    Remove-Item -Recurse -Force (Join-Path $PocRoot "backups") -ErrorAction SilentlyContinue
    Ok "Backups removed"

    Info "Removing WAL archive..."
    Remove-Item -Recurse -Force (Join-Path $PocRoot "wal_archive") -ErrorAction SilentlyContinue
    Ok "WAL archive removed"

    Info "Removing output files..."
    Get-ChildItem $OutputDir -Include "*.log","*.txt","*.json","*.csv" -Recurse | Remove-Item -Force
    Ok "Output files removed"
}

Write-Host ""
Ok "Cleanup complete"
