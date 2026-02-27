#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — One-Command Benchmark (Windows)
.DESCRIPTION
    Runs the entire FalconDB vs PostgreSQL comparison end-to-end.
.EXAMPLE
    .\run_benchmark.ps1
    $env:PGBENCH_SCALE=50; $env:PGBENCH_CLIENTS=20; .\run_benchmark.ps1
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $env:FALCON_BIN)      { $env:FALCON_BIN      = "target\release\falcon_server.exe" }
if (-not $env:PGBENCH_SCALE)   { $env:PGBENCH_SCALE   = "10" }
if (-not $env:PGBENCH_CLIENTS) { $env:PGBENCH_CLIENTS = "10" }
if (-not $env:PGBENCH_JOBS)    { $env:PGBENCH_JOBS    = "2" }
if (-not $env:PGBENCH_DURATION){ $env:PGBENCH_DURATION = "60" }
if (-not $env:PGBENCH_MODE)    { $env:PGBENCH_MODE    = "tpcb-like" }

function Banner($msg) { Write-Host "`n  === $msg ===`n" -ForegroundColor Cyan }

try {

Banner "FalconDB vs PostgreSQL - pgbench Comparison"
Write-Host ""
Write-Host "  Scale:       $($env:PGBENCH_SCALE)"
Write-Host "  Clients:     $($env:PGBENCH_CLIENTS)"
Write-Host "  Jobs:        $($env:PGBENCH_JOBS)"
Write-Host "  Duration:    $($env:PGBENCH_DURATION)s per run"
Write-Host "  Mode:        $($env:PGBENCH_MODE)"
Write-Host "  Runs:        1 warm-up + 3 measured (per system)"
Write-Host ""

Banner "Step 1/7: Environment Check"
& (Join-Path $ScriptDir "scripts\check_env.ps1")

Banner "Step 2/7: Starting FalconDB"
& (Join-Path $ScriptDir "scripts\start_falcondb.ps1")

Banner "Step 3/7: Starting PostgreSQL"
& (Join-Path $ScriptDir "scripts\start_postgres.ps1")

Banner "Step 4/7: Initializing pgbench Tables"
& (Join-Path $ScriptDir "scripts\init_pgbench.ps1")

Banner "Step 5/7: Benchmarking FalconDB"
& (Join-Path $ScriptDir "scripts\run_pgbench_falcon.ps1")

Banner "Step 6/7: Benchmarking PostgreSQL"
& (Join-Path $ScriptDir "scripts\run_pgbench_postgres.ps1")

Banner "Step 7/7: Collecting Results"
& (Join-Path $ScriptDir "scripts\collect_results.ps1")

Write-Host ""
Write-Host "  Benchmark complete." -ForegroundColor Green
Write-Host ""
Write-Host "  Report:  $(Join-Path $ScriptDir 'results\report.md')"
Write-Host "  Summary: $(Join-Path $ScriptDir 'results\parsed\summary.json')"
Write-Host "  Raw:     $(Join-Path $ScriptDir 'results\raw')"
Write-Host ""

} finally {
    & (Join-Path $ScriptDir "scripts\cleanup.ps1") -ErrorAction SilentlyContinue
}
