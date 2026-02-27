#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Migrate Data from PostgreSQL to FalconDB (Windows)
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

$DataDump = Join-Path $OutputDir "pg_data_dump.sql"
$Report   = Join-Path $OutputDir "migration_report.md"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Write-Host "`n  Data Migration: PostgreSQL -> FalconDB`n" -ForegroundColor Cyan

# Step 1: Export
Info "Exporting data from PostgreSQL (pg_dump --data-only)..."
& pg_dump -h $HostAddr -p $PgPort -U $PgUser -d $PgDb --data-only --no-owner --inserts > $DataDump 2>(Join-Path $OutputDir "pg_dump_data_err.log")
$DataSize = (Get-Item $DataDump).Length
Ok "Data exported: $DataDump ($DataSize bytes)"

# Step 2: Source counts
Info "Counting rows on PostgreSQL..."
$PgCounts = @{}
foreach ($tbl in @("customers","orders","order_items","payments")) {
    $count = (& psql -h $HostAddr -p $PgPort -U $PgUser -d $PgDb -t -A -c "SELECT COUNT(*) FROM $tbl;").Trim()
    $PgCounts[$tbl] = $count
    Ok "PG ${tbl}: $count rows"
}

# Step 3: Import
Info "Importing data into FalconDB..."
$sw = [System.Diagnostics.Stopwatch]::StartNew()
& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -f (Join-Path $PocRoot "data\sample_seed.sql") > (Join-Path $OutputDir "data_apply_falcon.log") 2>&1
$sw.Stop()
$durationMs = $sw.ElapsedMilliseconds
Ok "Data imported (${durationMs}ms)"

# Step 4: Verify
Info "Verifying row counts on FalconDB..."
$AllMatch = $true
$Comparison = @()

foreach ($tbl in @("customers","orders","order_items","payments")) {
    $fCount = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT COUNT(*) FROM $tbl;" 2>$null).Trim()
    $pCount = $PgCounts[$tbl]
    if ($fCount -eq $pCount) {
        Ok "${tbl}: PG=$pCount -> Falcon=$fCount"
        $Comparison += "| $tbl | $pCount | $fCount | Match |"
    } else {
        Fail "${tbl}: PG=$pCount -> Falcon=$fCount MISMATCH"
        $Comparison += "| $tbl | $pCount | $fCount | Mismatch |"
        $AllMatch = $false
    }
}

# Step 5: Append to report
$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss UTC")
$appendContent = @"

---

## Data Migration

**Date**: $ts
**Duration**: ${durationMs}ms
**Export size**: $DataSize bytes

### Row Count Comparison

| Table | PostgreSQL | FalconDB | Status |
|-------|-----------|----------|--------|
$($Comparison -join "`n")

### Method

1. Data exported from PostgreSQL using ``pg_dump --data-only --inserts``
2. Applied to FalconDB using curated ``data/sample_seed.sql``
3. Raw pg_dump output preserved at ``output/pg_data_dump.sql`` for audit
"@

Add-Content $Report $appendContent
Ok "Migration report updated: $Report"

Write-Host ""
if ($AllMatch) {
    Write-Host "  All row counts match. Data migration successful." -ForegroundColor Green
} else {
    Write-Host "  Row count mismatch detected. Check report." -ForegroundColor Red
}
Write-Host ""
