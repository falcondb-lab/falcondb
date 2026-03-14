#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: Kill Primary (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$PidFile = Join-Path $OutputDir "primary.pid"

if (-not (Test-Path $PidFile)) {
    Write-Host "  x No primary PID file found." -ForegroundColor Red
    exit 1
}

$PrimaryPid = [int](Get-Content $PidFile).Trim()

try {
    $null = Get-Process -Id $PrimaryPid -ErrorAction Stop
} catch {
    Write-Host "  > Primary (pid $PrimaryPid) is not running." -ForegroundColor Yellow
    exit 0
}

$PreKillCount = (Get-Content (Join-Path $OutputDir "committed_markers.log") -ErrorAction SilentlyContinue | Where-Object { $_ -match '\d' }).Count
$KillTS = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

Write-Host ""
Write-Host "  >>> Stop-Process -Force -Id $PrimaryPid <<<" -ForegroundColor Red
Write-Host ""
Write-Host "  > The primary is being killed DURING ACTIVE WRITES." -ForegroundColor Yellow
Write-Host "  > Markers committed before crash: $PreKillCount" -ForegroundColor Yellow
Write-Host "  > No graceful shutdown. No final flush. No signal handler." -ForegroundColor Yellow
Write-Host ""

Stop-Process -Id $PrimaryPid -Force -ErrorAction SilentlyContinue

Write-Host "  + Primary killed at $KillTS" -ForegroundColor Green
$KillTS | Set-Content (Join-Path $OutputDir "kill_timestamp.txt")
"$PreKillCount" | Set-Content (Join-Path $OutputDir "pre_kill_marker_count.txt")
Write-Host ""
