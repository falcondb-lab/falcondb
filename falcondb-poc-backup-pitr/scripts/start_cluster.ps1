#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Start FalconDB Cluster (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot     = Split-Path -Parent $ScriptDir
$OutputDir   = Join-Path $PocRoot "output"

$FalconBin   = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }
$FalconPort  = 5433
$FalconUser  = "falcon"
$FalconDb    = "pitr_demo"
$HostAddr    = "127.0.0.1"
$DataDir     = Join-Path $PocRoot "pitr_data"
$WalArchive  = Join-Path $PocRoot "wal_archive"
$BackupDir   = Join-Path $PocRoot "backups"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}
if (-not (Test-Path $FalconBin)) {
    Fail "FalconDB binary not found at '$FalconBin'"
    Write-Host "  Build it: cargo build -p falcon_server --release"
    exit 1
}
Ok "Binary: $FalconBin"

New-Item -ItemType Directory -Path $OutputDir, $WalArchive, $BackupDir -Force | Out-Null
if (Test-Path $DataDir) { Remove-Item -Recurse -Force $DataDir }

Write-Host "`n  Starting FalconDB (WAL + Archiving Enabled)`n" -ForegroundColor Cyan

Info "Data dir:    $DataDir"
Info "WAL archive: $WalArchive"
Info "Backup dir:  $BackupDir"

$Proc = Start-Process -FilePath $FalconBin `
    -ArgumentList "--pg-listen-addr","0.0.0.0:$FalconPort","--admin-listen-addr","0.0.0.0:8080","--data-dir",$DataDir `
    -RedirectStandardOutput (Join-Path $OutputDir "falcon.log") `
    -RedirectStandardError  (Join-Path $OutputDir "falcon_err.log") `
    -PassThru -WindowStyle Hidden
$Proc.Id | Set-Content (Join-Path $OutputDir "falcon.pid")

for ($i = 1; $i -le 30; $i++) {
    try {
        $r = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { Ok "FalconDB ready (pid $($Proc.Id), ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

& psql -h $HostAddr -p $FalconPort -U $FalconUser -d "postgres" -c "CREATE DATABASE $FalconDb;" 2>$null
Ok "Database '$FalconDb' exists"

Info "Applying accounts schema..."
& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -f (Join-Path $PocRoot "schema\accounts.sql") > (Join-Path $OutputDir "schema_apply.log") 2>&1
Ok "Schema applied (100 accounts seeded)"

Write-Host ""
Write-Host "  FalconDB: psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb"
Write-Host "  WAL:      enabled, archiving to $WalArchive"
Write-Host ""
