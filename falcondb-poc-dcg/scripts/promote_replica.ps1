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

Info "Promoting replica to primary..."

try { Invoke-RestMethod -Uri "http://${Host_}:8081/promote" -Method Post -ErrorAction SilentlyContinue } catch {}
Start-Sleep -Seconds 2

$PromoteOk = $false
for ($attempt = 1; $attempt -le 20; $attempt++) {
    try {
        $r = & psql -h $Host_ -p $ReplicaPort -U $DbUser -d $DbName -t -A -c "SELECT 1;" 2>$null
        if ($r -match "1") {
            $PromoteOk = $true
            Ok "Replica promoted and accepting queries (${attempt}s)"
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
