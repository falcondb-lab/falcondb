#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #6 — Cost Efficiency: Start FalconDB (Small Footprint) (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$OutputDir  = Join-Path $PocRoot "output"

$FalconBin  = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }
$FalconPort = 5433
$FalconUser = "falcon"
$FalconDb   = "bench"
$HostAddr   = "127.0.0.1"
$Conf       = Join-Path $PocRoot "conf\falcon.small.toml"

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
if (Test-Path ".\cost_data_falcon") { Remove-Item -Recurse -Force ".\cost_data_falcon" }

Write-Host "`n  Starting FalconDB (Small Footprint)`n" -ForegroundColor Cyan
Info "Config: $Conf"
Info "Memory soft limit: 512 MB | hard limit: 768 MB"

$Proc = Start-Process -FilePath $FalconBin -ArgumentList "-c",$Conf `
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

Write-Host ""
Write-Host "  FalconDB (small): psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb"
Write-Host "  Resource envelope: 512 MB soft / 768 MB hard / WAL+fsync ON"
Write-Host ""
