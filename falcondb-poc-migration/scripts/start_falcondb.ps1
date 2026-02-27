#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #5 — Migration: Start FalconDB (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$OutputDir  = Join-Path $PocRoot "output"

$FalconBin  = if ($env:FALCON_BIN)  { $env:FALCON_BIN }  else { "target\release\falcon_server.exe" }
$FalconPort = if ($env:FALCON_PORT) { $env:FALCON_PORT } else { "5433" }
$FalconUser = if ($env:FALCON_USER) { $env:FALCON_USER } else { "falcon" }
$FalconDb   = "shop_demo"
$HostAddr   = "127.0.0.1"

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

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
if (Test-Path ".\mig_data_falcon") { Remove-Item -Recurse -Force ".\mig_data_falcon" }

Write-Host "`n  Starting FalconDB (target database)`n" -ForegroundColor Cyan

Info "Starting FalconDB on port $FalconPort..."
$Proc = Start-Process -FilePath $FalconBin `
    -ArgumentList "--pg-listen-addr","0.0.0.0:$FalconPort","--admin-listen-addr","0.0.0.0:8080","--data-dir","./mig_data_falcon" `
    -RedirectStandardOutput (Join-Path $OutputDir "falcon.log") `
    -RedirectStandardError  (Join-Path $OutputDir "falcon_err.log") `
    -PassThru -WindowStyle Hidden
$Proc.Id | Set-Content (Join-Path $OutputDir "falcon.pid")

for ($i = 1; $i -le 30; $i++) {
    try {
        $r = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { Ok "FalconDB ready on port $FalconPort (pid $($Proc.Id), ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

& psql -h $HostAddr -p $FalconPort -U $FalconUser -d "postgres" -c "CREATE DATABASE $FalconDb;" 2>$null
Ok "Database '$FalconDb' exists"

Write-Host ""
Write-Host "  FalconDB: psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb"
Write-Host ""
