#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Wait for Cluster Ready (Windows)
#>

$ErrorActionPreference = 'Stop'

$Host_       = "127.0.0.1"
$PrimaryPort = 5433
$ReplicaPort = 5434
$DbName      = "falcon"
$DbUser      = "falcon"
$MaxWait     = if ($env:CLUSTER_READY_TIMEOUT) { [int]$env:CLUSTER_READY_TIMEOUT } else { 30 }

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Info "Waiting for cluster readiness (max ${MaxWait}s)..."

$PrimaryOk = $false
for ($i = 1; $i -le $MaxWait; $i++) {
    try {
        $r = & psql -h $Host_ -p $PrimaryPort -U $DbUser -d $DbName -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { $PrimaryOk = $true; Ok "Primary is accepting queries"; break }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $PrimaryOk) { Fail "Primary not ready within ${MaxWait}s"; exit 1 }

$ReplicaOk = $false
for ($i = 1; $i -le $MaxWait; $i++) {
    try {
        $r = & psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { $ReplicaOk = $true; Ok "Replica is accepting queries"; break }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $ReplicaOk) { Fail "Replica not ready within ${MaxWait}s"; exit 1 }

Info "Checking replication (canary write)..."
& psql -h $Host_ -p $PrimaryPort -U $DbUser -d $DbName -c "CREATE TABLE IF NOT EXISTS _canary (id INT PRIMARY KEY); INSERT INTO _canary VALUES (1) ON CONFLICT DO NOTHING;" 2>$null

$CanaryOk = $false
for ($i = 1; $i -le 15; $i++) {
    try {
        $r = & psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName -t -A -c "SELECT id FROM _canary WHERE id = 1;" 2>$null
        if ($r -match "1") { $CanaryOk = $true; Ok "Replication confirmed (canary replicated in ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

& psql -h $Host_ -p $PrimaryPort -U $DbUser -d $DbName -c "DROP TABLE IF EXISTS _canary;" 2>$null

if (-not $CanaryOk) { Fail "Replication not caught up within 15s"; exit 1 }

Ok "Cluster is ready for load"
