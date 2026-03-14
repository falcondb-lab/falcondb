#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB MES — Start Cluster + Apply Schema + Launch Backend (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot     = Split-Path -Parent $ScriptDir
$OutputDir   = Join-Path $PocRoot "output"

$FalconBin   = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }
$FalconPort  = 5433
$FalconUser  = "falcon"
$FalconDb    = "mes_prod"
$HostAddr    = "127.0.0.1"
$DataDir     = Join-Path $PocRoot "mes_data"
$BackendPort = 8000

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}
if (-not (Test-Path $FalconBin)) { Fail "FalconDB binary not found"; exit 1 }
Ok "Binary: $FalconBin"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
if (Test-Path $DataDir) { Remove-Item -Recurse -Force $DataDir }

Write-Host "`n  Starting FalconDB MES System`n" -ForegroundColor Cyan

# Start FalconDB
Info "Starting FalconDB..."
$FalconProc = Start-Process -FilePath $FalconBin `
    -ArgumentList "--pg-listen-addr","0.0.0.0:$FalconPort","--admin-listen-addr","0.0.0.0:8080","--data-dir",$DataDir `
    -RedirectStandardOutput (Join-Path $OutputDir "falcon.log") `
    -RedirectStandardError  (Join-Path $OutputDir "falcon_err.log") `
    -PassThru -WindowStyle Hidden
$FalconProc.Id | Set-Content (Join-Path $OutputDir "falcon.pid")

for ($i = 1; $i -le 30; $i++) {
    try {
        $r = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d postgres -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { Ok "FalconDB ready (pid $($FalconProc.Id), ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

# Create database and apply schema
& psql -h $HostAddr -p $FalconPort -U $FalconUser -d postgres -c "CREATE DATABASE $FalconDb;" 2>$null
Ok "Database '$FalconDb' ready"

Info "Applying MES schema..."
& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -f (Join-Path $PocRoot "schema\init.sql") > (Join-Path $OutputDir "schema.log") 2>&1
Ok "Schema applied (4 tables)"

# Start backend
Info "Starting MES backend API..."
$env:FALCON_HOST = $HostAddr
$env:FALCON_PORT = $FalconPort
$env:FALCON_DB   = $FalconDb
$BackendProc = Start-Process -FilePath "python" `
    -ArgumentList (Join-Path $PocRoot "backend\app.py") `
    -RedirectStandardOutput (Join-Path $OutputDir "backend.log") `
    -RedirectStandardError  (Join-Path $OutputDir "backend_err.log") `
    -PassThru -WindowStyle Hidden
$BackendProc.Id | Set-Content (Join-Path $OutputDir "backend.pid")

for ($i = 1; $i -le 15; $i++) {
    try {
        $r = Invoke-RestMethod -Uri "http://${HostAddr}:${BackendPort}/api/health" -TimeoutSec 2 -ErrorAction Stop
        if ($r.status -eq "healthy") { Ok "Backend API ready (pid $($BackendProc.Id), ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

Write-Host ""
Write-Host "  FalconDB MES System Started"
Write-Host "  Database:  psql -h $HostAddr -p $FalconPort -d $FalconDb"
Write-Host "  REST API:  http://${HostAddr}:${BackendPort}/docs"
Write-Host ""
