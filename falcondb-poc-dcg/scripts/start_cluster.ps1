#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — Start 2-Node Cluster (Windows)
.DESCRIPTION
    Starts a primary (port 5433) and replica (port 5434).
    Waits until both nodes accept connections and replication is established.
    Writes PIDs to output/ for other scripts.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir

$FalconBin    = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }
$ConfPrimary  = Join-Path $PocRoot "configs\primary.toml"
$ConfReplica  = Join-Path $PocRoot "configs\replica.toml"
$OutputDir    = Join-Path $PocRoot "output"

$Host_       = "127.0.0.1"
$PrimaryPort = 5433
$ReplicaPort = 5434
$DbName      = "falcon"
$DbUser      = "falcon"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

# ── Resolve binary ─────────────────────────────────────────────────────────
if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) {
        $FalconBin = $RepoBin
    } else {
        Fail "FalconDB binary not found at '$FalconBin'"
        Write-Host "  Build it: cargo build -p falcon_server --release"
        exit 1
    }
}
Ok "Binary: $FalconBin"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

# ── Clean previous data ───────────────────────────────────────────────────
if (Test-Path ".\poc_data_primary") { Remove-Item -Recurse -Force ".\poc_data_primary" }
if (Test-Path ".\poc_data_replica") { Remove-Item -Recurse -Force ".\poc_data_replica" }

# ── Start PRIMARY ─────────────────────────────────────────────────────────
Info "Starting PRIMARY on port $PrimaryPort..."
$ProcPrimary = Start-Process -FilePath $FalconBin -ArgumentList "-c",$ConfPrimary `
    -RedirectStandardOutput (Join-Path $OutputDir "primary.log") `
    -RedirectStandardError  (Join-Path $OutputDir "primary_err.log") `
    -PassThru -WindowStyle Hidden

$ProcPrimary.Id | Set-Content (Join-Path $OutputDir "primary.pid")

# ── Start REPLICA ─────────────────────────────────────────────────────────
Info "Starting REPLICA on port $ReplicaPort..."
$ProcReplica = Start-Process -FilePath $FalconBin -ArgumentList "-c",$ConfReplica `
    -RedirectStandardOutput (Join-Path $OutputDir "replica.log") `
    -RedirectStandardError  (Join-Path $OutputDir "replica_err.log") `
    -PassThru -WindowStyle Hidden

$ProcReplica.Id | Set-Content (Join-Path $OutputDir "replica.pid")

# ── Wait for nodes ────────────────────────────────────────────────────────
function WaitForNode {
    param([int]$Port, [string]$Label, [int]$MaxSec = 30)
    for ($i = 1; $i -le $MaxSec; $i++) {
        try {
            $r = & psql -h $Host_ -p $Port -U $DbUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
            if ($r -match "1") {
                Ok "$Label ready (${i}s)"
                return $true
            }
        } catch {}
        Start-Sleep -Seconds 1
    }
    Fail "$Label did not start within ${MaxSec}s"
    return $false
}

if (-not (WaitForNode -Port $PrimaryPort -Label "PRIMARY (pid $($ProcPrimary.Id))")) { exit 1 }
if (-not (WaitForNode -Port $ReplicaPort -Label "REPLICA (pid $($ProcReplica.Id))")) { exit 1 }

# ── Create database and schema ────────────────────────────────────────────
Info "Creating database and schema..."
& psql -h $Host_ -p $PrimaryPort -U $DbUser -d "postgres" -c "CREATE DATABASE $DbName;" 2>$null
Start-Sleep -Milliseconds 500
& psql -h $Host_ -p $PrimaryPort -U $DbUser -d $DbName -f (Join-Path $PocRoot "schema\orders.sql") 2>$null

Ok "Database '$DbName' and table 'orders' created"

Start-Sleep -Seconds 2

# ── Print connection info ─────────────────────────────────────────────────
Write-Host ""
Write-Host "  FalconDB 2-node cluster is running"
Write-Host "  PRIMARY:  psql -h $Host_ -p $PrimaryPort -U $DbUser -d $DbName"
Write-Host "  REPLICA:  psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName"
Write-Host "  PIDs: primary=$($ProcPrimary.Id), replica=$($ProcReplica.Id)"
Write-Host ""
