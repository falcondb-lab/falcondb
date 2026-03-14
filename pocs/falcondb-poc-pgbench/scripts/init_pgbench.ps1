#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Initialize pgbench Tables (Windows)
.DESCRIPTION
    FalconDB uses custom init (no multi-table TRUNCATE, BIGSERIAL PK on history).
    PostgreSQL uses standard pgbench -i.
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
$PgUser = if ($env:PG_USER) { $env:PG_USER } else { "postgres" }

$Scale = if ($env:PGBENCH_SCALE) { [int]$env:PGBENCH_SCALE } else { 1 }

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

$ExpectedRows = $Scale * 100000

# ── FalconDB (custom init — no multi-table TRUNCATE support) ─────────────
Banner "FalconDB (${FalconHost}:${FalconPort})"

& psql -h $FalconHost -p $FalconPort -U $FalconUser -d postgres -c "CREATE DATABASE pgbench;" 2>$null

Info "Dropping old tables..."
@('pgbench_history','pgbench_accounts','pgbench_tellers','pgbench_branches') | ForEach-Object {
    & psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -c "DROP TABLE IF EXISTS $_;" 2>$null
}

Info "Creating tables (BIGSERIAL PK on pgbench_history for FalconDB)..."
$ddl = @"
CREATE TABLE pgbench_branches (bid INT NOT NULL PRIMARY KEY, bbalance INT NOT NULL, filler CHAR(88));
CREATE TABLE pgbench_tellers (tid INT NOT NULL PRIMARY KEY, bid INT NOT NULL, tbalance INT NOT NULL, filler CHAR(84));
CREATE TABLE pgbench_accounts (aid INT NOT NULL PRIMARY KEY, bid INT NOT NULL, abalance INT NOT NULL, filler CHAR(84));
CREATE TABLE pgbench_history (hid BIGSERIAL PRIMARY KEY, tid INT, bid INT, aid INT, delta INT, mtime TIMESTAMP, filler CHAR(22));
"@
& psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -c $ddl 2>&1 | Out-Null

Info "Populating branches ($Scale)..."
$bvals = (1..$Scale | ForEach-Object { "($_, 0, '')" }) -join ","
& psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -c "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES $bvals;" 2>$null

Info "Populating tellers ($($Scale * 10))..."
$tvals = (1..($Scale * 10) | ForEach-Object { "($_, $(([math]::Ceiling($_ / 10))), 0, '')" }) -join ","
& psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -c "INSERT INTO pgbench_tellers (tid, bid, tbalance, filler) VALUES $tvals;" 2>$null

Info "Populating accounts ($ExpectedRows rows, batch=1000)..."
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$totalBatches = $ExpectedRows / 1000
for ($batch = 0; $batch -lt $totalBatches; $batch++) {
    $start = $batch * 1000 + 1
    $end = $start + 999
    $bid = ($batch % $Scale) + 1
    $vals = ($start..$end | ForEach-Object { "($_, $bid, 0, '')" }) -join ","
    & psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -q -c "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES $vals;" 2>$null
    if (($batch + 1) % 20 -eq 0) {
        $pct = [math]::Round(($batch + 1) / $totalBatches * 100)
        Info "  $($batch+1)/$totalBatches batches ($pct%, $($sw.Elapsed.TotalSeconds.ToString('F1'))s)"
    }
}
$sw.Stop()
Info "  Done in $($sw.Elapsed.TotalSeconds.ToString('F1'))s"

$FalconRows = (& psql -h $FalconHost -p $FalconPort -U $FalconUser -d pgbench -t -A -c "SELECT COUNT(*) FROM pgbench_accounts;" 2>$null).Trim()
if ($FalconRows -eq "$ExpectedRows") {
    Ok "FalconDB: $FalconRows accounts (expected $ExpectedRows)"
} else {
    Fail "FalconDB: $FalconRows accounts (expected $ExpectedRows)"
}

"custom" | Set-Content (Join-Path $falconRaw "init.log")

# ── PostgreSQL (standard pgbench -i) ─────────────────────────────────────
Banner "PostgreSQL (${PgHost}:${PgPort})"

& psql -h $PgHost -p $PgPort -U $PgUser -d postgres -c "DROP DATABASE IF EXISTS pgbench;" 2>$null
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
