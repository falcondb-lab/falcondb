#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Generate OLTP Activity (Windows)
.DESCRIPTION
    Light continuous INSERT/UPDATE traffic. Runs until Ctrl+C.
#>

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$OutputDir  = Join-Path $PocRoot "output"
$Timeline   = Join-Path $OutputDir "event_timeline.txt"

$HostAddr   = "127.0.0.1"
$Port       = if ($env:FALCON_PORT) { $env:FALCON_PORT } else { "5433" }
$DbName     = if ($env:FALCON_DB)   { $env:FALCON_DB }   else { "falcon" }
$DbUser     = if ($env:FALCON_USER) { $env:FALCON_USER } else { "falcon" }
$IntervalMs = if ($env:ACTIVITY_INTERVAL_MS) { [int]$env:ACTIVITY_INTERVAL_MS } else { 200 }

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Add-Content $Timeline "$ts | ACTIVITY_START | interval=${IntervalMs}ms"
Info "Generating activity on ${HostAddr}:${Port}/${DbName} (every ${IntervalMs}ms)"
Info "Press Ctrl+C to stop"

$Counter = 0
$EventTypes = @("order", "payment", "shipment", "refund", "login", "logout", "search", "view")

try {
    while ($true) {
        $Counter++
        $type = $EventTypes | Get-Random

        & psql -h $HostAddr -p $Port -U $DbUser -d $DbName -q -c "INSERT INTO demo_events (event_type, payload) VALUES ('$type', 'event_$Counter');" 2>$null

        if ($Counter % 10 -eq 0) {
            & psql -h $HostAddr -p $Port -U $DbUser -d $DbName -q -c "UPDATE demo_events SET payload = 'updated_$Counter' WHERE id = (SELECT id FROM demo_events ORDER BY RANDOM() LIMIT 1);" 2>$null
        }

        if ($Counter % 20 -eq 0) {
            & psql -h $HostAddr -p $Port -U $DbUser -d $DbName -q -c "SELECT COUNT(*) FROM demo_events WHERE event_type = '$type';" 2>$null
        }

        if ($Counter % 100 -eq 0) {
            Info "[$Counter] events generated"
        }

        Start-Sleep -Milliseconds $IntervalMs
    }
} finally {
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Add-Content $Timeline "$ts | ACTIVITY_STOP | total_events=$Counter"
    Ok "Stopped after $Counter events"
}
