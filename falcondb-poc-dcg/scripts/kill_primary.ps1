#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — Kill Primary (Hard Crash, Windows)
.DESCRIPTION
    Sends Stop-Process -Force to the primary node. This is NOT a graceful shutdown.
    Equivalent to taskkill /F — the process is terminated immediately by the OS.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$PidFile = Join-Path $OutputDir "primary.pid"

if (-not (Test-Path $PidFile)) {
    Write-Host "  x No primary PID file found at $PidFile" -ForegroundColor Red
    Write-Host "  Run scripts\start_cluster.ps1 first."
    exit 1
}

$PrimaryPid = [int](Get-Content $PidFile).Trim()

try {
    $null = Get-Process -Id $PrimaryPid -ErrorAction Stop
} catch {
    Write-Host "  > Primary (pid $PrimaryPid) is not running." -ForegroundColor Yellow
    exit 0
}

$KillTS = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

Write-Host ""
Write-Host "  >>> Stop-Process -Force -Id $PrimaryPid <<<" -ForegroundColor Red
Write-Host ""
Write-Host "  > This simulates a sudden hardware failure or power loss." -ForegroundColor Yellow
Write-Host "  > The primary has NO chance to flush buffers or send final messages." -ForegroundColor Yellow
Write-Host ""

Stop-Process -Id $PrimaryPid -Force -ErrorAction SilentlyContinue

Write-Host "  + Primary killed at $KillTS" -ForegroundColor Green
$KillTS | Set-Content (Join-Path $OutputDir "kill_timestamp.txt")
Write-Host ""
