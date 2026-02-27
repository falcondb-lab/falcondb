#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Simulate Failover (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"
$Timeline  = Join-Path $OutputDir "event_timeline.txt"

$Host_       = "127.0.0.1"
$ReplicaPort = 5434
$DbName      = "falcon"
$DbUser      = "falcon"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$PidFile = Join-Path $OutputDir "node1.pid"
if (-not (Test-Path $PidFile)) { Fail "No node1 PID file found."; exit 1 }

$PrimaryPid = [int](Get-Content $PidFile).Trim()
try { $null = Get-Process -Id $PrimaryPid -ErrorAction Stop } catch {
    Info "Primary (pid $PrimaryPid) is not running."
    exit 0
}

Info "Snapshotting metrics before failover..."
try { Invoke-RestMethod -Uri "http://${Host_}:8080/metrics" -TimeoutSec 5 | Set-Content (Join-Path $OutputDir "metrics_before_failover.txt") } catch {}

$KillTS = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

Write-Host ""
Write-Host "  >>> Stop-Process -Force -Id $PrimaryPid <<<" -ForegroundColor Red
Write-Host ""
Info "Watch the Grafana dashboard — metrics will react immediately."

Stop-Process -Id $PrimaryPid -Force -ErrorAction SilentlyContinue
Ok "Primary killed at $KillTS"
Add-Content $Timeline "$KillTS | FAILOVER_KILL | pid=$PrimaryPid"

Start-Sleep -Seconds 2

Info "Promoting replica..."
try { Invoke-RestMethod -Uri "http://${Host_}:8081/promote" -Method Post -TimeoutSec 5 } catch {}
Start-Sleep -Seconds 2

$PromoteOk = $false
for ($attempt = 1; $attempt -le 15; $attempt++) {
    try {
        $r = & psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { $PromoteOk = $true; Ok "Replica promoted and accepting queries (${attempt}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

$PromoteTS = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
Add-Content $Timeline "$PromoteTS | FAILOVER_PROMOTE | success=$PromoteOk"

Info "Snapshotting metrics after failover..."
try { Invoke-RestMethod -Uri "http://${Host_}:8081/metrics" -TimeoutSec 5 | Set-Content (Join-Path $OutputDir "metrics_after_failover.txt") } catch {}

Write-Host ""
Info "What to look for on the dashboard:"
Write-Host "  - falcon_replication_leader_changes incremented"
Write-Host "  - falcon_replication_promote_count incremented"
Write-Host "  - falcon_cluster_health_status may have briefly changed"
Write-Host ""
