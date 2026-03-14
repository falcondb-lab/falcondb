#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — Promote Replica to Primary (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$Host_       = "127.0.0.1"
$ReplicaPort = 5434
$DbName      = "falcon"
$DbUser      = "falcon"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$FalconBin = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon.exe" }
$ConfReplica = Join-Path $PocRoot "configs\replica.toml"

# Resolve binary path
if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}

Info "Promoting replica to primary (restart as standalone)..."

# Stop the replica process
$ReplicaPidFile = Join-Path $OutputDir "replica.pid"
if (Test-Path $ReplicaPidFile) {
    $ReplicaPid = [int](Get-Content $ReplicaPidFile).Trim()
    Stop-Process -Id $ReplicaPid -Force -ErrorAction SilentlyContinue
    Info "Stopped replica (pid $ReplicaPid)"
    Start-Sleep -Seconds 2
}

# Restart as standalone (writable) using the same data directory
$NewProc = Start-Process -FilePath $FalconBin -ArgumentList "-c",$ConfReplica,"--role","standalone" `
    -RedirectStandardOutput (Join-Path $OutputDir "replica_promoted.log") `
    -RedirectStandardError  (Join-Path $OutputDir "replica_promoted_err.log") `
    -PassThru -WindowStyle Hidden

$NewProc.Id | Set-Content $ReplicaPidFile

$PromoteOk = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    try {
        $r = & psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") {
            $PromoteOk = $true
            Ok "Replica promoted and accepting queries (pid $($NewProc.Id), ${attempt}s)"
            break
        }
    } catch {}
    Start-Sleep -Seconds 1
}

if (-not $PromoteOk) {
    Fail "Replica did not become available after promotion"
    exit 1
}

$PromoteTS = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$PromoteTS | Set-Content (Join-Path $OutputDir "promote_timestamp.txt")

Write-Host ""
Write-Host "  New primary available at: psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName"
Write-Host ""
