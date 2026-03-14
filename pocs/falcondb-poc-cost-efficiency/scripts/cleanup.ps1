#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #6 — Cost Efficiency: Cleanup (Windows)
.PARAMETER All
    Also remove data directories and output files.
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

# Stop FalconDB
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

# Stop PostgreSQL
$PgData = Join-Path $PocRoot "pg_data_large"
if (Test-Path $PgData) {
    Info "Stopping PostgreSQL..."
    & pg_ctl -D $PgData stop 2>$null
    if ($LASTEXITCODE -eq 0) { Ok "PostgreSQL stopped" } else { Info "PostgreSQL already stopped" }
}

if ($All) {
    Info "Removing FalconDB data..."
    Remove-Item -Recurse -Force ".\cost_data_falcon" -ErrorAction SilentlyContinue
    Ok "FalconDB data removed"

    Info "Removing PostgreSQL data..."
    Remove-Item -Recurse -Force $PgData -ErrorAction SilentlyContinue
    Ok "PostgreSQL data removed"

    Info "Removing output files..."
    Get-ChildItem $OutputDir -Include "*.log","*.json","*.md","*.txt" -Recurse | Remove-Item -Force
    Ok "Output files removed"
}

Write-Host ""
Ok "Cleanup complete"
