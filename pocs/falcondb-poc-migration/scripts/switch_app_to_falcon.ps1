#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Switch Application to FalconDB (Windows)
.DESCRIPTION
    Shows what changes — only the connection string. No code changes.
#>

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$AppDir    = Join-Path $PocRoot "app"
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  Switching Application to FalconDB`n" -ForegroundColor Cyan

Info "Before (PostgreSQL config):"
Write-Host ""
Get-Content (Join-Path $AppDir "app.conf.postgres") | ForEach-Object { Write-Host "    $_" }
Write-Host ""

Info "After (FalconDB config):"
Write-Host ""
Get-Content (Join-Path $AppDir "app.conf.falcon") | ForEach-Object { Write-Host "    $_" }
Write-Host ""

Info "Changes:"
Write-Host "    host:     127.0.0.1  ->  127.0.0.1  (same)"
Write-Host "    port:     5432       ->  5433"
Write-Host "    user:     postgres   ->  falcon"
Write-Host "    dbname:   shop_demo  ->  shop_demo  (same)"
Write-Host "    password: (none)     ->  (none)"
Write-Host ""
Write-Host "    Code changes: NONE"
Write-Host "    SQL changes:  NONE"
Write-Host "    Driver:       psycopg2 (unchanged)"
Write-Host ""

Ok "To run against FalconDB, use: --config app.conf.falcon"
Ok "To run against PostgreSQL, use: --config app.conf.postgres"
Write-Host ""
Info "The application is the same script. Only the config file changes."
Write-Host ""

$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Add-Content (Join-Path $OutputDir "event_timeline.txt") "$ts | APP_SWITCH | target=falcon port=5433" -ErrorAction SilentlyContinue
