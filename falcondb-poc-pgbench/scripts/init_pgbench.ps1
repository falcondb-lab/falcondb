#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Initialize pgbench Tables (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"

$FalconHost = if ($env:FALCON_HOST) { $env:FALCON_HOST } else { "127.0.0.1" }
$FalconPort = if ($env:FALCON_PORT) { $env:FALCON_PORT } else { "5433" }
$FalconUser = if ($env:FALCON_USER) { $env:FALCON_USER } else { "falcon" }

$PgHost = if ($env:PG_HOST) { $env:PG_HOST } else { "127.0.0.1" }
$PgPort = if ($env:PG_PORT) { $env:PG_PORT } else { "5432" }
$PgUser = if ($env:PG_USER) { $env:PG_USER } else { $env:USERNAME }

$Scale = if ($env:PGBENCH_SCALE) { [int]$env:PGBENCH_SCALE } else { 10 }

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg)   { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }
function Banner($msg) { Write-Host "`n  $msg`n" -ForegroundColor Cyan }

Banner "Initializing pgbench (scale=$Scale)"

$falconRaw = Join-Path $ResultsDir "raw\falcon"
$pgRaw     = Join-Path $ResultsDir "raw\postgres"
$parsedDir = Join-Path $ResultsDir "parsed"
New-Item -ItemType Directory -Path $falconRaw -Force | Out-Null
New-Item -ItemType Directory -Path $pgRaw -Force | Out-Null
New-Item -ItemType Directory -Path $parsedDir -Force | Out-Null

# ── FalconDB ──────────────────────────────────────────────────────────────
Banner "FalconDB (${FalconHost}:${FalconPort})"

& psql -h $FalconHost -p $FalconPort -U $FalconUser -d postgres -c "CREATE DATABASE pgbench;" 2>$null

Info "Running pgbench -i -s $Scale on FalconDB..."
& pgbench -h $FalconHost -p $FalconPort -U $FalconUser -i -s $Scale pgbench > (Join-Path $falconRaw "init.log") 2>&1

$FalconRows = (& psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -t -A -c "SELECT COUNT(*) FROM pgbench_accounts;" 2>$null).Trim()
$ExpectedRows = $Scale * 100000

if ($FalconRows -eq "$ExpectedRows") {
    Ok "FalconDB: $FalconRows accounts (expected $ExpectedRows)"
} else {
    Fail "FalconDB: $FalconRows accounts (expected $ExpectedRows)"
}

# ── PostgreSQL ────────────────────────────────────────────────────────────
Banner "PostgreSQL (${PgHost}:${PgPort})"

& psql -h $PgHost -p $PgPort -U $PgUser -d postgres -c "CREATE DATABASE pgbench;" 2>$null

Info "Running pgbench -i -s $Scale on PostgreSQL..."
& pgbench -h $PgHost -p $PgPort -U $PgUser -i -s $Scale pgbench > (Join-Path $pgRaw "init.log") 2>&1

$PgRows = (& psql -h $PgHost -p $PgPort -U $PgUser -d pgbench -t -A -c "SELECT COUNT(*) FROM pgbench_accounts;" 2>$null).Trim()

if ($PgRows -eq "$ExpectedRows") {
    Ok "PostgreSQL: $PgRows accounts (expected $ExpectedRows)"
} else {
    Fail "PostgreSQL: $PgRows accounts (expected $ExpectedRows)"
}

# ── Save init metadata ────────────────────────────────────────────────────
$meta = @{
    scale         = $Scale
    expected_rows = $ExpectedRows
    falcon_rows   = [int]$FalconRows
    postgres_rows = [int]$PgRows
    timestamp     = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
} | ConvertTo-Json -Depth 2

$meta | Set-Content (Join-Path $parsedDir "init_metadata.json")
Ok "Initialization complete for both systems"
