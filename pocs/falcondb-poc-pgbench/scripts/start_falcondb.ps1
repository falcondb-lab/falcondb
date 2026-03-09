#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Start FalconDB Server (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"
$FalconBin  = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon_server.exe" }
$Conf       = Join-Path $PocRoot "conf\falcon.bench.toml"
$Host_      = "127.0.0.1"
$Port       = 5433

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}
if (-not (Test-Path $FalconBin)) {
    Fail "FalconDB binary not found. Build: cargo build -p falcon_server --release"
    exit 1
}

if (Test-Path ".\falcon_bench_data") { Remove-Item -Recurse -Force ".\falcon_bench_data" }

$rawDir = Join-Path $ResultsDir "raw\falcon"
New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

Info "Starting FalconDB on port $Port..."
$proc = Start-Process -FilePath $FalconBin -ArgumentList "-c",$Conf `
    -RedirectStandardOutput (Join-Path $rawDir "server.log") `
    -RedirectStandardError  (Join-Path $rawDir "server_err.log") `
    -PassThru -WindowStyle Hidden

$proc.Id | Set-Content (Join-Path $rawDir "falcon.pid")

for ($i = 1; $i -le 30; $i++) {
    try {
        $r = & psql -h $Host_ -p $Port -U falcon -d postgres -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") {
            Ok "FalconDB ready (pid $($proc.Id), ${i}s)"
            exit 0
        }
    } catch {}
    Start-Sleep -Seconds 1
}

Fail "FalconDB did not start within 30s"
exit 1
