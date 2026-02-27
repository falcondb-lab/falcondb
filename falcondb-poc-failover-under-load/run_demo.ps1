#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: One-Command Demo (Windows)
.EXAMPLE
    .\run_demo.ps1
    $env:MARKER_COUNT=100000; .\run_demo.ps1
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$OutputDir = Join-Path $ScriptDir "output"

if (-not $env:FALCON_BIN)    { $env:FALCON_BIN    = "target\release\falcon_server.exe" }
if (-not $env:MARKER_COUNT)  { $env:MARKER_COUNT  = "50000" }
if (-not $env:LOAD_RAMP_SEC) { $env:LOAD_RAMP_SEC = "5" }

function Banner($msg) { Write-Host "`n  === $msg ===`n" -ForegroundColor Cyan }

try {

Banner "FalconDB - Failover Under Load (Correctness + Availability)"
Write-Host ""
Write-Host "  This demo proves that FalconDB does not lose acknowledged commits"
Write-Host "  even when the primary crashes during peak write traffic."
Write-Host ""
Write-Host "  Markers to write: $($env:MARKER_COUNT)"
Write-Host "  Load ramp time:   $($env:LOAD_RAMP_SEC)s (before crash)"
Write-Host ""

Banner "Step 1/8: Starting 2-node cluster"
& (Join-Path $ScriptDir "scripts\start_cluster.ps1")

Banner "Step 2/8: Verifying replication"
& (Join-Path $ScriptDir "scripts\wait_cluster_ready.ps1")

Banner "Step 3/8: Starting sustained write load"
& (Join-Path $ScriptDir "scripts\start_load.ps1")

Banner "Step 4/8: Letting load ramp up ($($env:LOAD_RAMP_SEC)s)"
Write-Host "  The writer is actively committing markers to the primary..."
$rampSec = [int]$env:LOAD_RAMP_SEC
for ($i = 1; $i -le $rampSec; $i++) {
    $count = (Get-Content (Join-Path $OutputDir "committed_markers.log") -ErrorAction SilentlyContinue | Where-Object { $_ -match '\d' }).Count
    Write-Host "  [$i/${rampSec}s] $count markers committed"
    Start-Sleep -Seconds 1
}

Banner "Step 5/8: KILLING the primary (DURING ACTIVE WRITES)"
& (Join-Path $ScriptDir "scripts\kill_primary.ps1")

Start-Sleep -Seconds 2

Banner "Step 6/8: Promoting replica to primary"
& (Join-Path $ScriptDir "scripts\promote_replica.ps1")

Banner "Step 7/8: Waiting for writer to reconnect and complete"
$writerPidFile = Join-Path $OutputDir "writer.pid"
if (Test-Path $writerPidFile) {
    $writerPid = [int](Get-Content $writerPidFile).Trim()
    $maxWait = 120
    for ($i = 1; $i -le $maxWait; $i++) {
        try {
            $null = Get-Process -Id $writerPid -ErrorAction Stop
        } catch {
            $finalCount = (Get-Content (Join-Path $OutputDir "committed_markers.log") | Where-Object { $_ -match '\d' }).Count
            Write-Host "  + Writer finished ($finalCount markers committed)" -ForegroundColor Green
            break
        }
        if ($i % 10 -eq 0) {
            $count = (Get-Content (Join-Path $OutputDir "committed_markers.log") -ErrorAction SilentlyContinue | Where-Object { $_ -match '\d' }).Count
            Write-Host "  [$i/${maxWait}s] Writer still running ($count markers)"
        }
        Start-Sleep -Seconds 1
    }
    # Force stop if still running
    try {
        $null = Get-Process -Id $writerPid -ErrorAction Stop
        "" | Set-Content (Join-Path $OutputDir "stop_writer") -NoNewline
        Start-Sleep -Seconds 3
        Stop-Process -Id $writerPid -Force -ErrorAction SilentlyContinue
    } catch {}
}

Banner "Step 8/8: Measuring downtime and verifying integrity"

& (Join-Path $ScriptDir "scripts\monitor_downtime.ps1")
Write-Host ""
& (Join-Path $ScriptDir "scripts\verify_integrity.ps1")
$VerifyExit = $LASTEXITCODE

Write-Host ""
if ($VerifyExit -eq 0) {
    Write-Host "  Demo complete. FalconDB remained correct through the crash." -ForegroundColor Green
} else {
    Write-Host "  Demo complete. Verification FAILED." -ForegroundColor Red
}
Write-Host ""
Write-Host "  Output files: $OutputDir"
Write-Host ""

exit $VerifyExit

} finally {
    & (Join-Path $ScriptDir "scripts\cleanup.ps1") -ErrorAction SilentlyContinue
}
