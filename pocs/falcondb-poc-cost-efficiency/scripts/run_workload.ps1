#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #6 — Cost Efficiency: Run Workload Against Both Databases (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot      = Split-Path -Parent $ScriptDir
$OutputDir    = Join-Path $PocRoot "output"
$WorkloadBin  = Join-Path $PocRoot "workload\target\release\oltp_writer.exe"

$Rate     = if ($env:WORKLOAD_RATE)     { $env:WORKLOAD_RATE }     else { "1000" }
$Duration = if ($env:WORKLOAD_DURATION) { $env:WORKLOAD_DURATION } else { "60" }
$Threads  = if ($env:WORKLOAD_THREADS)  { $env:WORKLOAD_THREADS }  else { "4" }

$HostAddr    = "127.0.0.1"
$PgPort      = if ($env:PG_PORT) { $env:PG_PORT } else { "5432" }
$PgUser      = if ($env:PG_USER) { $env:PG_USER } else { "postgres" }
$FalconPort  = "5433"
$FalconUser  = "falcon"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

if (-not (Test-Path $WorkloadBin)) {
    Info "Building workload generator..."
    Push-Location (Join-Path $PocRoot "workload")
    & cargo build --release 2>&1
    Pop-Location
    if (-not (Test-Path $WorkloadBin)) { Fail "Failed to build workload generator"; exit 1 }
    Ok "Workload generator built"
}

Write-Host "`n  Running Workload: $Rate tx/s x ${Duration}s ($Threads threads)`n" -ForegroundColor Cyan

Write-Host "`n  Phase 1: FalconDB (small footprint)`n" -ForegroundColor Cyan
& $WorkloadBin --host $HostAddr --port $FalconPort --user $FalconUser --db bench `
    --rate $Rate --duration $Duration --threads $Threads `
    --label falcon --output (Join-Path $OutputDir "workload_results_falcon.json")
Ok "FalconDB workload complete"

Info "Cooldown (5s)..."
Start-Sleep -Seconds 5

Write-Host "`n  Phase 2: PostgreSQL (production-sized)`n" -ForegroundColor Cyan
& $WorkloadBin --host $HostAddr --port $PgPort --user $PgUser --db bench `
    --rate $Rate --duration $Duration --threads $Threads `
    --label postgres --output (Join-Path $OutputDir "workload_results_postgres.json")
Ok "PostgreSQL workload complete"

Write-Host ""
Write-Host "  Run .\scripts\generate_cost_report.ps1 for full analysis."
Write-Host ""
