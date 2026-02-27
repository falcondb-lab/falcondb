#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #4 — Observability: Start 2-Node Cluster (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir

$FalconBin  = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }
$ConfNode1  = Join-Path $PocRoot "configs\node1.toml"
$ConfNode2  = Join-Path $PocRoot "configs\node2.toml"
$OutputDir  = Join-Path $PocRoot "output"

$Host_      = "127.0.0.1"
$Node1Port  = 5433
$Node2Port  = 5434
$DbName     = "falcon"
$DbUser     = "falcon"

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg)   { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }

if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}
if (-not (Test-Path $FalconBin)) {
    Fail "FalconDB binary not found at '$FalconBin'"
    Write-Host "  Build it: cargo build -p falcon_server --release"
    exit 1
}
Ok "Binary: $FalconBin"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
if (Test-Path ".\obs_data_node1") { Remove-Item -Recurse -Force ".\obs_data_node1" }
if (Test-Path ".\obs_data_node2") { Remove-Item -Recurse -Force ".\obs_data_node2" }

Info "Starting Node 1 (primary) on port $Node1Port..."
$Proc1 = Start-Process -FilePath $FalconBin -ArgumentList "-c",$ConfNode1 `
    -RedirectStandardOutput (Join-Path $OutputDir "node1.log") `
    -RedirectStandardError  (Join-Path $OutputDir "node1_err.log") `
    -PassThru -WindowStyle Hidden
$Proc1.Id | Set-Content (Join-Path $OutputDir "node1.pid")

Info "Starting Node 2 (replica) on port $Node2Port..."
$Proc2 = Start-Process -FilePath $FalconBin -ArgumentList "-c",$ConfNode2 `
    -RedirectStandardOutput (Join-Path $OutputDir "node2.log") `
    -RedirectStandardError  (Join-Path $OutputDir "node2_err.log") `
    -PassThru -WindowStyle Hidden
$Proc2.Id | Set-Content (Join-Path $OutputDir "node2.pid")

function WaitForNode {
    param([int]$Port, [string]$Label, [int]$MaxSec = 30)
    for ($i = 1; $i -le $MaxSec; $i++) {
        try {
            $r = & psql -h $Host_ -p $Port -U $DbUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
            if ($r -match "1") { Ok "$Label ready (${i}s)"; return $true }
        } catch {}
        Start-Sleep -Seconds 1
    }
    Fail "$Label did not start within ${MaxSec}s"
    return $false
}

if (-not (WaitForNode -Port $Node1Port -Label "Node 1 / primary (pid $($Proc1.Id))")) { exit 1 }
if (-not (WaitForNode -Port $Node2Port -Label "Node 2 / replica (pid $($Proc2.Id))")) { exit 1 }

Info "Creating database and demo table..."
& psql -h $Host_ -p $Node1Port -U $DbUser -d "postgres" -c "CREATE DATABASE $DbName;" 2>$null
Start-Sleep -Milliseconds 500
& psql -h $Host_ -p $Node1Port -U $DbUser -d $DbName -c @"
CREATE TABLE IF NOT EXISTS demo_events (
    id         BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload    TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
"@ 2>$null
Ok "Database '$DbName' and table 'demo_events' created"

Start-Sleep -Seconds 2

Write-Host ""
Write-Host "  FalconDB cluster is running"
Write-Host "  Node 1 (primary): psql -h $Host_ -p $Node1Port"
Write-Host "  Node 2 (replica): psql -h $Host_ -p $Node2Port"
Write-Host "  Metrics: http://${Host_}:8080/metrics"
Write-Host "           http://${Host_}:8081/metrics"
Write-Host ""
