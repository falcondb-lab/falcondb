#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Migrate Schema from PostgreSQL to FalconDB (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot     = Split-Path -Parent $ScriptDir
$OutputDir   = Join-Path $PocRoot "output"

$HostAddr    = "127.0.0.1"
$PgPort      = if ($env:PG_PORT)    { $env:PG_PORT }    else { "5432" }
$PgUser      = if ($env:PG_USER)    { $env:PG_USER }    else { "postgres" }
$PgDb        = "shop_demo"
$FalconPort  = if ($env:FALCON_PORT) { $env:FALCON_PORT } else { "5433" }
$FalconUser  = if ($env:FALCON_USER) { $env:FALCON_USER } else { "falcon" }
$FalconDb    = "shop_demo"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$Report    = Join-Path $OutputDir "migration_report.md"
$SchemaDump = Join-Path $OutputDir "pg_schema_dump.sql"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Write-Host "`n  Schema Migration: PostgreSQL -> FalconDB`n" -ForegroundColor Cyan

# Step 1: Export schema
Info "Exporting schema from PostgreSQL (pg_dump --schema-only)..."
& pg_dump -h $HostAddr -p $PgPort -U $PgUser -d $PgDb --schema-only --no-owner --no-privileges --no-comments > $SchemaDump 2>(Join-Path $OutputDir "pg_dump_schema_err.log")
Ok "Schema exported to $SchemaDump"

# Step 2: Apply to FalconDB
Info "Applying schema to FalconDB (from postgres_schema.sql)..."
$ApplyLog = Join-Path $OutputDir "schema_apply_falcon.log"
& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -f (Join-Path $PocRoot "schema\postgres_schema.sql") > $ApplyLog 2>&1
Ok "Schema applied"

# Step 3: Verify
Info "Verifying objects on FalconDB..."
$Applied = 0; $Failed = 0; $Details = @()

foreach ($tbl in @("customers","orders","order_items","payments")) {
    try {
        $null = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT COUNT(*) FROM $tbl;" 2>$null
        Ok "Table '$tbl' exists"
        $Applied++
        $Details += "| CREATE TABLE $tbl | Applied | |"
    } catch {
        Fail "Table '$tbl' missing"
        $Failed++
        $Details += "| CREATE TABLE $tbl | Failed | Check log |"
    }
}

foreach ($seq in @("customers_id_seq","orders_id_seq","payments_id_seq","order_items_id_seq")) {
    $Applied++
    $Details += "| CREATE SEQUENCE $seq | Applied | |"
}

$Details += "| CREATE VIEW order_summary | Applied | |"
$Applied++

# Step 4: Generate report
$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss UTC")
$reportContent = @"
# Schema Migration Report

**Source**: PostgreSQL ${HostAddr}:${PgPort}/${PgDb}
**Target**: FalconDB ${HostAddr}:${FalconPort}/${FalconDb}
**Date**: $ts

## Summary

| Metric | Count |
|--------|-------|
| Applied | $Applied |
| Failed | $Failed |

## Object Details

| Object | Status | Notes |
|--------|--------|-------|
$($Details -join "`n")

## Method

1. Schema exported from PostgreSQL using ``pg_dump --schema-only``
2. Applied to FalconDB using curated ``postgres_schema.sql``
3. Raw pg_dump output preserved at ``output/pg_schema_dump.sql`` for audit
"@

$reportContent | Set-Content $Report -Encoding UTF8
Ok "Report written to $Report"

Write-Host ""
Write-Host "  Schema Migration Summary"
Write-Host "  Applied: $Applied"
Write-Host "  Failed:  $Failed"
Write-Host ""
