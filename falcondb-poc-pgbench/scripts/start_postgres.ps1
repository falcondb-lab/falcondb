#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Start PostgreSQL Server (Windows)
.DESCRIPTION
    If PostgreSQL is already running on port 5432, uses it as-is.
    Otherwise initializes a fresh instance with benchmark config.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"

$PgPort    = if ($env:PG_PORT) { $env:PG_PORT } else { "5432" }
$PgUser    = if ($env:PG_USER) { $env:PG_USER } else { $env:USERNAME }
$PgHost    = if ($env:PG_HOST) { $env:PG_HOST } else { "127.0.0.1" }
$PgDataDir = if ($env:PG_DATADIR) { $env:PG_DATADIR } else { ".\pg_bench_data" }
$PgConf    = Join-Path $PocRoot "conf\postgres.bench.conf"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$rawDir = Join-Path $ResultsDir "raw\postgres"
New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

# ── Check if PostgreSQL is already running ─────────────────────────────────
try {
    $r = & pg_isready -h $PgHost -p $PgPort 2>$null
    if ($LASTEXITCODE -eq 0) {
        Ok "PostgreSQL already running on port $PgPort"
        $pgVer = & psql -h $PgHost -p $PgPort -U $PgUser -d postgres -t -A -c "SHOW server_version;" 2>$null
        Ok "Version: $pgVer"
        "existing" | Set-Content (Join-Path $rawDir "start_mode.txt")
        exit 0
    }
} catch {}

# ── Initialize fresh data directory ────────────────────────────────────────
Info "PostgreSQL not running. Initializing fresh instance..."

if (-not (Get-Command initdb -ErrorAction SilentlyContinue)) {
    Fail "initdb not found. Install PostgreSQL server package."
    exit 1
}

if (Test-Path $PgDataDir) { Remove-Item -Recurse -Force $PgDataDir }

& initdb -D $PgDataDir --auth=trust --username=$PgUser > (Join-Path $rawDir "initdb.log") 2>&1

if (Test-Path $PgConf) {
    Get-Content $PgConf | Add-Content (Join-Path $PgDataDir "postgresql.conf")
    Ok "Applied benchmark config"
}

"port = $PgPort" | Add-Content (Join-Path $PgDataDir "postgresql.conf")

# ── Start PostgreSQL ──────────────────────────────────────────────────────
Info "Starting PostgreSQL on port $PgPort..."

& pg_ctl -D $PgDataDir -l (Join-Path $rawDir "server.log") -o "-p $PgPort" start

for ($i = 1; $i -le 30; $i++) {
    try {
        & pg_isready -h $PgHost -p $PgPort 2>$null
        if ($LASTEXITCODE -eq 0) {
            Ok "PostgreSQL ready (${i}s)"
            "managed" | Set-Content (Join-Path $rawDir "start_mode.txt")
            exit 0
        }
    } catch {}
    Start-Sleep -Seconds 1
}

Fail "PostgreSQL did not start within 30s"
exit 1
