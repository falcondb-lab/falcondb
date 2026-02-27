#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Initialize Sample App Database on PostgreSQL (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$HostAddr = "127.0.0.1"
$PgPort   = if ($env:PG_PORT) { $env:PG_PORT } else { "5432" }
$PgUser   = if ($env:PG_USER) { $env:PG_USER } else { "postgres" }
$PgDb     = "shop_demo"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Write-Host "`n  Initializing PostgreSQL Source Database`n" -ForegroundColor Cyan

Info "Applying schema..."
& psql -h $HostAddr -p $PgPort -U $PgUser -d $PgDb -f (Join-Path $PocRoot "schema\postgres_schema.sql") > (Join-Path $OutputDir "schema_apply_pg.log") 2>&1
if ($LASTEXITCODE -eq 0) { Ok "Schema applied" } else { Fail "Schema apply failed"; exit 1 }

Info "Loading seed data..."
& psql -h $HostAddr -p $PgPort -U $PgUser -d $PgDb -f (Join-Path $PocRoot "data\sample_seed.sql") > (Join-Path $OutputDir "seed_apply_pg.log") 2>&1
if ($LASTEXITCODE -eq 0) { Ok "Seed data loaded" } else { Fail "Seed data load failed"; exit 1 }

Info "Verifying row counts..."
$counts = & psql -h $HostAddr -p $PgPort -U $PgUser -d $PgDb -t -A -c @"
  SELECT 'customers=' || COUNT(*) FROM customers
  UNION ALL
  SELECT 'orders=' || COUNT(*) FROM orders
  UNION ALL
  SELECT 'order_items=' || COUNT(*) FROM order_items
  UNION ALL
  SELECT 'payments=' || COUNT(*) FROM payments;
"@
$counts -split "`n" | Where-Object { $_.Trim() } | ForEach-Object { Ok $_.Trim() }

Write-Host ""
Ok "PostgreSQL source database ready"
Write-Host ""
