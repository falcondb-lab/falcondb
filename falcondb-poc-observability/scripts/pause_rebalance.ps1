#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Pause Rebalance (Windows)
#>

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"
$Timeline  = Join-Path $OutputDir "event_timeline.txt"

$HostAddr  = "127.0.0.1"
$AdminPort = if ($env:ADMIN_PORT) { $env:ADMIN_PORT } else { "8080" }

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Info "Pausing rebalancer on ${HostAddr}:${AdminPort}..."

try {
    $resp = Invoke-WebRequest -Uri "http://${HostAddr}:${AdminPort}/rebalance/pause" -Method Post -TimeoutSec 5
    Ok "Rebalancer paused (HTTP $($resp.StatusCode))"
    Add-Content $Timeline "$ts | REBALANCE_PAUSE | status=ok http=$($resp.StatusCode)"
} catch {
    $code = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
    Info "Pause API returned HTTP $code (endpoint may not be implemented yet)"
    Add-Content $Timeline "$ts | REBALANCE_PAUSE | status=attempted http=$code"
}

Info "Watch the dashboard: falcon_rebalancer_paused should change to 1"
