#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Trigger Rebalance (Windows)
#>

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"
$Timeline  = Join-Path $OutputDir "event_timeline.txt"

$HostAddr  = "127.0.0.1"
$AdminPort = if ($env:ADMIN_PORT) { $env:ADMIN_PORT } else { "8080" }

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

Info "Triggering rebalance on ${HostAddr}:${AdminPort}..."

try {
    $resp = Invoke-WebRequest -Uri "http://${HostAddr}:${AdminPort}/rebalance" -Method Post -TimeoutSec 5
    $code = $resp.StatusCode
    Ok "Rebalance triggered (HTTP $code)"
    Add-Content $Timeline "$ts | REBALANCE_TRIGGER | status=ok http=$code"
} catch {
    $code = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
    Info "Rebalance API returned HTTP $code (endpoint may not be implemented yet)"
    Add-Content $Timeline "$ts | REBALANCE_TRIGGER | status=attempted http=$code"
}

Info "Watch the dashboard: falcon_rebalancer_running should change to 1"

try {
    $metrics = (Invoke-RestMethod -Uri "http://${HostAddr}:${AdminPort}/metrics" -TimeoutSec 5) -split "`n" | Where-Object { $_ -match "^falcon_rebalancer_" }
    Write-Host "`n  Current rebalancer metrics:"
    $metrics | ForEach-Object { Write-Host "    $_" }
    Write-Host ""
} catch {
    Info "Could not fetch metrics snapshot"
}
