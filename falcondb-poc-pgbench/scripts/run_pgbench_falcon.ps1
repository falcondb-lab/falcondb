#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Run pgbench Against FalconDB (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"

$FalconHost = if ($env:FALCON_HOST) { $env:FALCON_HOST } else { "127.0.0.1" }
$FalconPort = if ($env:FALCON_PORT) { $env:FALCON_PORT } else { "5433" }
$FalconUser = if ($env:FALCON_USER) { $env:FALCON_USER } else { "falcon" }

$Clients  = if ($env:PGBENCH_CLIENTS)  { [int]$env:PGBENCH_CLIENTS }  else { 10 }
$Jobs     = if ($env:PGBENCH_JOBS)     { [int]$env:PGBENCH_JOBS }     else { 2 }
$Duration = if ($env:PGBENCH_DURATION) { [int]$env:PGBENCH_DURATION } else { 60 }
$Mode     = if ($env:PGBENCH_MODE)     { $env:PGBENCH_MODE }          else { "tpcb-like" }

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }
function Banner($msg) { Write-Host "`n  $msg`n" -ForegroundColor Cyan }

$RawDir = Join-Path $ResultsDir "raw\falcon"
New-Item -ItemType Directory -Path $RawDir -Force | Out-Null

$ModeFlag = switch ($Mode) {
    "read-only"      { "-S" }
    "select-only"    { "-S" }
    "simple-update"  { "-N" }
    default          { "" }
}

Banner "FalconDB pgbench: clients=$Clients, jobs=$Jobs, duration=${Duration}s, mode=$Mode"

# ── Warm-up ───────────────────────────────────────────────────────────────
Info "Warm-up run (15s, not measured)..."
$warmupArgs = @("-h", $FalconHost, "-p", $FalconPort, "-U", $FalconUser, "-c", $Clients, "-j", $Jobs, "-T", "15")
if ($ModeFlag) { $warmupArgs += $ModeFlag }
$warmupArgs += "pgbench"
& pgbench @warmupArgs > (Join-Path $RawDir "warmup.log") 2>&1
Ok "Warm-up complete"

# ── Measured runs ─────────────────────────────────────────────────────────
for ($run = 1; $run -le 3; $run++) {
    Info "Run $run/3 (${Duration}s)..."

    $runArgs = @("-h", $FalconHost, "-p", $FalconPort, "-U", $FalconUser, "-c", $Clients, "-j", $Jobs, "-T", $Duration, "--progress=10")
    if ($ModeFlag) { $runArgs += $ModeFlag }
    $runArgs += "pgbench"

    & pgbench @runArgs > (Join-Path $RawDir "run_${run}.log") 2>&1

    $logContent = Get-Content (Join-Path $RawDir "run_${run}.log") -Raw
    $tpsMatch = [regex]::Match($logContent, 'tps = ([0-9.]+)')
    $latMatch = [regex]::Match($logContent, 'latency average = ([0-9.]+)')
    $tps = if ($tpsMatch.Success) { $tpsMatch.Groups[1].Value } else { "N/A" }
    $lat = if ($latMatch.Success) { $latMatch.Groups[1].Value } else { "N/A" }

    Ok "Run $run`: TPS=$tps, avg latency=$lat ms"
}

# ── Save parameters ──────────────────────────────────────────────────────
$params = @{
    system       = "falcondb"
    host         = $FalconHost
    port         = [int]$FalconPort
    clients      = $Clients
    jobs         = $Jobs
    duration_sec = $Duration
    mode         = $Mode
    runs         = 3
    warmup_sec   = 15
    timestamp    = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
} | ConvertTo-Json -Depth 2

$params | Set-Content (Join-Path $RawDir "parameters.json")
Ok "All FalconDB runs complete. Raw output: $RawDir"
