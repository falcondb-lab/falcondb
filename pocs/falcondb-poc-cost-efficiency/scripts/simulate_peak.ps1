#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #6 — Cost Efficiency: Simulate Peak Load (Windows)
.PARAMETER Target
    Which database to hit: "falcon" or "postgres"
#>
param(
    [ValidateSet("falcon","postgres")]
    [string]$Target = "falcon"
)

$ErrorActionPreference = 'Stop'

$ScriptDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot      = Split-Path -Parent $ScriptDir
$OutputDir    = Join-Path $PocRoot "output"
$WorkloadBin  = Join-Path $PocRoot "workload\target\release\oltp_writer.exe"

$HostAddr = "127.0.0.1"
$Threads  = if ($env:WORKLOAD_THREADS) { $env:WORKLOAD_THREADS } else { "4" }

if ($Target -eq "falcon") {
    $Port = "5433"; $User = "falcon"
} else {
    $Port = if ($env:PG_PORT) { $env:PG_PORT } else { "5432" }
    $User = if ($env:PG_USER) { $env:PG_USER } else { "postgres" }
}

$NormalRate = if ($env:WORKLOAD_RATE) { [int]$env:WORKLOAD_RATE } else { 1000 }
$PeakRate   = $NormalRate * 2
$PeakDur    = 30
$NormalDur  = 30

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

if (-not (Test-Path $WorkloadBin)) {
    Info "Building workload generator..."
    Push-Location (Join-Path $PocRoot "workload"); & cargo build --release 2>&1; Pop-Location
}

Write-Host "`n  Peak Simulation: $Target ($NormalRate -> $PeakRate -> $NormalRate tx/s)`n" -ForegroundColor Cyan

Info "Phase 1: Normal load ($NormalRate tx/s x ${NormalDur}s)"
& $WorkloadBin --host $HostAddr --port $Port --user $User --db bench `
    --rate $NormalRate --duration $NormalDur --threads $Threads `
    --label "${Target}_pre_peak" --output (Join-Path $OutputDir "peak_pre_${Target}.json")
Ok "Pre-peak baseline complete"

Write-Host ""
Info "Phase 2: PEAK load ($PeakRate tx/s x ${PeakDur}s)"
Write-Host "  >>> 2x load spike <<<" -ForegroundColor Red
& $WorkloadBin --host $HostAddr --port $Port --user $User --db bench `
    --rate $PeakRate --duration $PeakDur --threads $Threads `
    --label "${Target}_peak" --output (Join-Path $OutputDir "peak_during_${Target}.json")
Ok "Peak phase complete"

Write-Host ""
Info "Phase 3: Recovery - normal load ($NormalRate tx/s x ${NormalDur}s)"
& $WorkloadBin --host $HostAddr --port $Port --user $User --db bench `
    --rate $NormalRate --duration $NormalDur --threads $Threads `
    --label "${Target}_post_peak" --output (Join-Path $OutputDir "peak_post_${Target}.json")
Ok "Post-peak recovery complete"

Write-Host ""
Write-Host "  Compare p95/p99 latency across phases to measure stability."
Write-Host ""
