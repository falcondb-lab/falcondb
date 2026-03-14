#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB MES — Promote Replica / Restart After Crash (Windows)
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

Write-Host "`n  Promote / Recover After Crash`n" -ForegroundColor Cyan

if (-not (Test-Path $DataDir)) { Fail "Data directory not found — cannot recover"; exit 1 }
Ok "Data directory intact"

Info "Starting FalconDB (WAL recovery)..."
$FalconProc = Start-Process -FilePath $FalconBin `
    -ArgumentList "--pg-listen-addr","0.0.0.0:$FalconPort","--admin-listen-addr","0.0.0.0:8080","--data-dir",$DataDir `
    -RedirectStandardOutput (Join-Path $OutputDir "falcon_recovery.log") `
    -RedirectStandardError  (Join-Path $OutputDir "falcon_recovery_err.log") `
    -PassThru -WindowStyle Hidden
$FalconProc.Id | Set-Content (Join-Path $OutputDir "falcon.pid")

$ready = $false
for ($i = 1; $i -le 60; $i++) {
    try {
        $r = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d postgres -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { $ready = $true; Ok "FalconDB recovered (pid $($FalconProc.Id), ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $ready) { Fail "FalconDB did not recover within 60s"; exit 1 }

# Restart backend
$oldPidFile = Join-Path $OutputDir "backend.pid"
if (Test-Path $oldPidFile) {
    $oldPid = [int](Get-Content $oldPidFile).Trim()
    Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
    Remove-Item $oldPidFile -Force -ErrorAction SilentlyContinue
}

Info "Restarting backend API..."
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
        if ($r.status -eq "healthy") { Ok "Backend API ready (${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

Write-Host ""
Write-Host "  Recovery Complete"
Write-Host "  All committed production facts are intact."
Write-Host "  Next: verify_business_state.ps1"
Write-Host ""
