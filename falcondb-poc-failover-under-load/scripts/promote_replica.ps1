#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: Promote Replica (Windows)
.DESCRIPTION
    Stops the replica process and restarts it as a standalone (writable) node
    using the same data directory. This simulates failover promotion.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$FalconBin   = if ($env:FALCON_BIN) { $env:FALCON_BIN } else { "target\release\falcon.exe" }
$Host_       = "127.0.0.1"
$ReplicaPort = 5434
$DbName      = "falcon"
$DbUser      = "falcon"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

# Resolve binary
if (-not (Test-Path $FalconBin)) {
    $RepoBin = Join-Path (Split-Path -Parent $PocRoot) $FalconBin
    if (Test-Path $RepoBin) { $FalconBin = $RepoBin }
}

# Stop the replica process
$ReplicaPidFile = Join-Path $OutputDir "replica.pid"
if (Test-Path $ReplicaPidFile) {
    $rpid = [int](Get-Content $ReplicaPidFile).Trim()
    Info "Stopping replica (pid $rpid) for role change..."
    Stop-Process -Id $rpid -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Ok "Replica stopped"
}

# Restart as standalone (writable) using the same data directory
Info "Restarting replica as standalone on port $ReplicaPort..."
$ConfReplica = Join-Path $PocRoot "configs\replica.toml"
$Proc = Start-Process -FilePath $FalconBin `
    -ArgumentList "-c",$ConfReplica,"--role","standalone" `
    -RedirectStandardOutput (Join-Path $OutputDir "replica_promoted.log") `
    -RedirectStandardError  (Join-Path $OutputDir "replica_promoted_err.log") `
    -PassThru -WindowStyle Hidden
$Proc.Id | Set-Content $ReplicaPidFile

$PromoteOk = $false
for ($attempt = 1; $attempt -le 20; $attempt++) {
    try {
        $r = & psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") { $PromoteOk = $true; Ok "Replica promoted to standalone and accepting queries (${attempt}s, pid $($Proc.Id))"; break }
    } catch {}
    Start-Sleep -Seconds 1
}

if (-not $PromoteOk) { Fail "Promoted replica did not become available"; exit 1 }

$PromoteTS = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
$PromoteTS | Set-Content (Join-Path $OutputDir "promote_timestamp.txt")

Write-Host ""
Write-Host "  New primary: psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName"
Write-Host ""
