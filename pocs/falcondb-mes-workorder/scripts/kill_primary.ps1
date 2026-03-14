#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB MES — Kill Primary Node (Windows)
.DESCRIPTION
    Simulates a hard crash using Stop-Process -Force (equivalent to kill -9).
#>

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"
$PidFile   = Join-Path $OutputDir "falcon.pid"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  KILLING FalconDB primary (Hard Crash)`n" -ForegroundColor Red

if (-not (Test-Path $PidFile)) { Fail "PID file not found. Is FalconDB running?"; exit 1 }

$falconPid = [int](Get-Content $PidFile).Trim()

try {
    Get-Process -Id $falconPid -ErrorAction Stop | Out-Null
    $killTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    Info "Sending kill to pid $falconPid..."
    Stop-Process -Id $falconPid -Force
    Start-Sleep -Seconds 1
    Ok "FalconDB primary KILLED at $killTs"
    Add-Content (Join-Path $OutputDir "event_timeline.txt") "$killTs | KILL_PRIMARY | pid=$falconPid"
} catch {
    Info "FalconDB (pid $falconPid) already dead"
}

Remove-Item $PidFile -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "  Primary is DOWN" -ForegroundColor Red
Write-Host ""
Write-Host "  Next: promote_replica.ps1 -> verify_business_state.ps1"
Write-Host ""
