#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Smoke Test (Windows)
.DESCRIPTION
    Runs the same workflows against PostgreSQL and FalconDB, compares results.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$AppDir    = Join-Path $PocRoot "app"
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$Results    = Join-Path $OutputDir "smoke_test_results.txt"
$PgJson     = Join-Path $OutputDir "smoke_pg.json"
$FalconJson = Join-Path $OutputDir "smoke_falcon.json"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Write-Host "`n  Smoke Test: Same App, Two Databases`n" -ForegroundColor Cyan

# Find Python
$PythonCmd = "python"
try { & python3 -c "import psycopg2" 2>$null; $PythonCmd = "python3" } catch {
    try { & python -c "import psycopg2" 2>$null } catch {
        Fail "psycopg2 not installed. Run: pip install psycopg2-binary"
        exit 1
    }
}
Ok "Python + psycopg2 available"

# Run against PostgreSQL
Info "Running workflows against PostgreSQL..."
& $PythonCmd (Join-Path $AppDir "demo_app.py") --config (Join-Path $AppDir "app.conf.postgres") --workflow all --json > $PgJson 2>(Join-Path $OutputDir "smoke_pg_err.log")
if ($LASTEXITCODE -eq 0) { Ok "PostgreSQL run complete" } else { Fail "PostgreSQL run failed" }

# Run against FalconDB
Info "Running workflows against FalconDB..."
& $PythonCmd (Join-Path $AppDir "demo_app.py") --config (Join-Path $AppDir "app.conf.falcon") --workflow all --json > $FalconJson 2>(Join-Path $OutputDir "smoke_falcon_err.log")
if ($LASTEXITCODE -eq 0) { Ok "FalconDB run complete" } else { Fail "FalconDB run failed" }

# Compare
Write-Host "`n  Results Comparison`n" -ForegroundColor Cyan

$compareScript = @"
import json, sys
with open(r'$PgJson') as f: pg = json.load(f)
with open(r'$FalconJson') as f: fc = json.load(f)
pg_r = {r['workflow']: r for r in pg.get('results', [])}
fc_r = {r['workflow']: r for r in fc.get('results', [])}
wfs = list(dict.fromkeys(list(pg_r.keys()) + list(fc_r.keys())))
print(f'  {"Workflow":<22} {"PostgreSQL":<14} {"FalconDB":<14} Match')
print('  ' + '-' * 64)
ok = True
for w in wfs:
    p = pg_r.get(w, {}).get('result', 'N/A')
    f2 = fc_r.get(w, {}).get('result', 'N/A')
    m = 'YES' if p == f2 else 'NO'
    if m == 'NO': ok = False
    print(f'  {w:<22} {p:<14} {f2:<14} {m}')
print('  ' + '-' * 64)
print('  All workflows match.' if ok else '  MISMATCH detected.')
if not ok: sys.exit(1)
"@

$output = & $PythonCmd -c $compareScript 2>&1
$output | ForEach-Object { Write-Host $_ }
$output | Set-Content $Results

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss UTC")
Add-Content $Results "`nPostgreSQL JSON: $PgJson"
Add-Content $Results "FalconDB JSON:   $FalconJson"
Add-Content $Results "Date: $ts"

Write-Host ""
Ok "Results saved to $Results"
Write-Host ""
