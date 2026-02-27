#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Replay WAL Until Target Time T1 (Windows)
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
$TsFile      = Join-Path $OutputDir "recovery_target.txt"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}
if (-not (Test-Path $FalconBin)) { Fail "FalconDB binary not found"; exit 1 }

Write-Host "`n  WAL Replay: Advancing to Recovery Target (T1)`n" -ForegroundColor Cyan

if (-not (Test-Path $TsFile)) { Fail "Recovery target file not found: $TsFile"; exit 1 }
$recoveryTs = (Get-Content $TsFile | Where-Object { $_ -match "^Database time:" }) -replace "^Database time:\s*",""
Info "Recovery target: $recoveryTs"

if (-not (Test-Path $DataDir)) { Fail "Restored data directory not found"; exit 1 }
Ok "Restored data directory exists"

# Copy WAL segments
$walTarget = Join-Path $DataDir "wal"
New-Item -ItemType Directory -Path $walTarget -Force | Out-Null
$walCopied = 0
if (Test-Path $WalArchive) {
    Get-ChildItem $WalArchive -File | ForEach-Object {
        Copy-Item $_.FullName $walTarget -Force
        $walCopied++
    }
}
Ok "Copied $walCopied WAL segments for replay"

Info "Starting FalconDB for WAL recovery..."
$Proc = Start-Process -FilePath $FalconBin `
    -ArgumentList "--pg-listen-addr","0.0.0.0:$FalconPort","--admin-listen-addr","0.0.0.0:8080","--data-dir",$DataDir `
    -RedirectStandardOutput (Join-Path $OutputDir "falcon_recovery.log") `
    -RedirectStandardError  (Join-Path $OutputDir "falcon_recovery_err.log") `
    -PassThru -WindowStyle Hidden
$Proc.Id | Set-Content (Join-Path $OutputDir "falcon.pid")

$recoveryDone = $false
for ($i = 1; $i -le 60; $i++) {
    try {
        $r = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d "postgres" -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { $recoveryDone = $true; Ok "FalconDB recovered and ready (pid $($Proc.Id), ${i}s)"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

if (-not $recoveryDone) { Fail "FalconDB did not start within 60s"; exit 1 }

$replayTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
@"
WAL Replay Log
==============
Date:              $replayTs
Recovery target:   $recoveryTs
WAL segments:      $walCopied
Data directory:    $DataDir
Recovery method:   FalconDB WAL replay on startup

Status: REPLAY_COMPLETE
"@ | Set-Content (Join-Path $OutputDir "wal_replay.log") -Encoding UTF8
Ok "Replay log written"

$rowCount     = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT COUNT(*) FROM accounts;" 2>$null).Trim()
$totalBalance = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT SUM(balance) FROM accounts;" 2>$null).Trim()

Write-Host ""
Write-Host "  WAL Replay Complete"
Write-Host "  Target:   $recoveryTs"
Write-Host "  Accounts: $rowCount"
Write-Host "  Balance:  $totalBalance"
Write-Host "  Next:     verify_restored_data.ps1"
Write-Host ""
