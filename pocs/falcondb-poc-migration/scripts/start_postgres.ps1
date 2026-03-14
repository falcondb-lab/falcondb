#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Start PostgreSQL (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$PgPort = if ($env:PG_PORT) { $env:PG_PORT } else { "5432" }
$PgUser = if ($env:PG_USER) { $env:PG_USER } else { "postgres" }
$PgDb   = "shop_demo"
$PgData = Join-Path $PocRoot "pg_data"
$HostAddr = "127.0.0.1"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

if (-not (Get-Command pg_ctl -ErrorAction SilentlyContinue)) {
    Fail "pg_ctl not found. Install PostgreSQL."
    exit 1
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Write-Host "`n  Starting PostgreSQL (source database)`n" -ForegroundColor Cyan

$running = $false
try {
    $r = & psql -h $HostAddr -p $PgPort -U $PgUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
    if ($r -match "1") { $running = $true; Ok "PostgreSQL already running on port $PgPort" }
} catch {}

if (-not $running) {
    if (-not (Test-Path $PgData)) {
        Info "Initializing PostgreSQL data directory..."
        & initdb -D $PgData --auth=trust --username=$PgUser > (Join-Path $OutputDir "pg_init.log") 2>&1
        Ok "Data directory initialized: $PgData"
    }

    Info "Starting PostgreSQL on port $PgPort..."
    & pg_ctl -D $PgData -l (Join-Path $OutputDir "postgres.log") -o "-p $PgPort" start | Out-Null

    for ($i = 1; $i -le 20; $i++) {
        try {
            $r = & psql -h $HostAddr -p $PgPort -U $PgUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
            if ($r -match "1") { Ok "PostgreSQL ready on port $PgPort (${i}s)"; break }
        } catch {}
        Start-Sleep -Seconds 1
    }
}

& psql -h $HostAddr -p $PgPort -U $PgUser -d "postgres" -c "CREATE DATABASE $PgDb;" 2>$null
Ok "Database '$PgDb' exists"

Write-Host ""
Write-Host "  PostgreSQL: psql -h $HostAddr -p $PgPort -U $PgUser -d $PgDb"
Write-Host ""
