#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Cleanup (Windows)
.PARAMETER All
    If specified, also removes data directories and results.
#>
param([switch]$All)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

# ── Stop FalconDB ─────────────────────────────────────────────────────────
$falconPidFile = Join-Path $ResultsDir "raw\falcon\falcon.pid"
if (Test-Path $falconPidFile) {
    $fpid = [int](Get-Content $falconPidFile).Trim()
    try {
        $null = Get-Process -Id $fpid -ErrorAction Stop
        Info "Stopping FalconDB (pid $fpid)..."
        Stop-Process -Id $fpid -Force
        Ok "FalconDB stopped"
    } catch {
        Info "FalconDB (pid $fpid) already stopped"
    }
    Remove-Item $falconPidFile -Force
}

# ── Stop PostgreSQL (only if we started it) ────────────────────────────────
$pgModeFile = Join-Path $ResultsDir "raw\postgres\start_mode.txt"
$pgDataDir  = if ($env:PG_DATADIR) { $env:PG_DATADIR } else { ".\pg_bench_data" }

if ((Test-Path $pgModeFile) -and ((Get-Content $pgModeFile).Trim() -eq "managed")) {
    if (Test-Path $pgDataDir) {
        Info "Stopping managed PostgreSQL..."
        & pg_ctl -D $pgDataDir stop -m fast 2>$null
        Ok "PostgreSQL stopped"
    }
}

# ── Optionally remove data ────────────────────────────────────────────────
if ($All) {
    Info "Removing data directories..."
    Remove-Item ".\falcon_bench_data" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item $pgDataDir -Recurse -Force -ErrorAction SilentlyContinue
    Ok "Data directories removed"

    Info "Removing results..."
    Remove-Item (Join-Path $ResultsDir "raw\falcon") -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $ResultsDir "raw\postgres") -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem (Join-Path $ResultsDir "parsed") -Include "*.json" -Recurse | Remove-Item -Force
    Remove-Item (Join-Path $ResultsDir "report.md") -Force -ErrorAction SilentlyContinue
    Ok "Results removed"
}

Ok "Cleanup complete"
