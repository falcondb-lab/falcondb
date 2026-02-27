#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: Start Write Load (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$env:FALCON_HOST    = if ($env:FALCON_HOST)    { $env:FALCON_HOST }    else { "127.0.0.1" }
$env:FALCON_PORT    = if ($env:FALCON_PORT)    { $env:FALCON_PORT }    else { "5433" }
$env:FALCON_DB      = if ($env:FALCON_DB)      { $env:FALCON_DB }      else { "falcon" }
$env:FALCON_USER    = if ($env:FALCON_USER)    { $env:FALCON_USER }    else { "falcon" }
$env:FAILOVER_HOST  = if ($env:FAILOVER_HOST)  { $env:FAILOVER_HOST }  else { "127.0.0.1" }
$env:FAILOVER_PORT  = if ($env:FAILOVER_PORT)  { $env:FAILOVER_PORT }  else { "5434" }
$env:MARKER_COUNT   = if ($env:MARKER_COUNT)   { $env:MARKER_COUNT }   else { "50000" }
$env:OUTPUT_DIR     = $OutputDir
$env:START_MARKER_ID = if ($env:START_MARKER_ID) { $env:START_MARKER_ID } else { "1" }
$env:STOP_FILE      = Join-Path $OutputDir "stop_writer"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
"" | Set-Content (Join-Path $OutputDir "committed_markers.log") -NoNewline
Remove-Item (Join-Path $OutputDir "stop_writer") -Force -ErrorAction SilentlyContinue

$RustBin = Join-Path $PocRoot "workload\target\release\tx_marker_writer.exe"

if (Test-Path $RustBin) {
    Info "Launching Rust workload generator (background)..."
    $writerProc = Start-Process -FilePath $RustBin `
        -RedirectStandardOutput (Join-Path $OutputDir "writer_stdout.log") `
        -RedirectStandardError  (Join-Path $OutputDir "writer_stderr.log") `
        -PassThru -WindowStyle Hidden
} else {
    $py = if (Get-Command python3 -ErrorAction SilentlyContinue) { "python3" } elseif (Get-Command python -ErrorAction SilentlyContinue) { "python" } else { $null }
    if ($py) {
        Info "Launching Python fallback workload generator (background)..."
        $writerProc = Start-Process -FilePath $py -ArgumentList (Join-Path $PocRoot "workload\tx_marker_writer.py") `
            -RedirectStandardOutput (Join-Path $OutputDir "writer_stdout.log") `
            -RedirectStandardError  (Join-Path $OutputDir "writer_stderr.log") `
            -PassThru -WindowStyle Hidden
    } else {
        Write-Host "  x No Rust binary or Python found." -ForegroundColor Red
        exit 1
    }
}

$writerProc.Id | Set-Content (Join-Path $OutputDir "writer.pid")
Ok "Writer started (pid $($writerProc.Id), $($env:MARKER_COUNT) markers)"

Start-Sleep -Seconds 2

try {
    $null = Get-Process -Id $writerProc.Id -ErrorAction Stop
    $earlyCount = (Get-Content (Join-Path $OutputDir "committed_markers.log") | Where-Object { $_ -match '\d' }).Count
    Ok "Writer is running ($earlyCount markers committed so far)"
} catch {
    Write-Host "  x Writer exited unexpectedly. Check writer_stderr.log" -ForegroundColor Red
    exit 1
}
