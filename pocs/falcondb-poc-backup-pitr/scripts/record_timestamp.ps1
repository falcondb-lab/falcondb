#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Record Recovery Timestamp T1 (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot     = Split-Path -Parent $ScriptDir
$OutputDir   = Join-Path $PocRoot "output"

$HostAddr    = "127.0.0.1"
$FalconPort  = 5433
$FalconUser  = "falcon"
$FalconDb    = "pitr_demo"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$TsFile    = Join-Path $OutputDir "recovery_target.txt"
$StateFile = Join-Path $OutputDir "state_at_t1.csv"

Write-Host "`n  Recording Recovery Target (T1)`n" -ForegroundColor Cyan

$dbTs   = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT NOW();" 2>$null).Trim()
$wallTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

Info "Database timestamp: $dbTs"
Info "Wall-clock:         $wallTs"

@"
Recovery Target (T1)
====================
Database time: $dbTs
Wall-clock:    $wallTs
Target type:   timestamp
"@ | Set-Content $TsFile -Encoding UTF8
Ok "Recovery target written: $TsFile"

Info "Capturing database state at T1..."
$rowCount     = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT COUNT(*) FROM accounts;" 2>$null).Trim()
$totalBalance = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT SUM(balance) FROM accounts;" 2>$null).Trim()

$balances = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT account_id, balance FROM accounts ORDER BY account_id;" 2>$null
$balances -replace '\|',',' | Set-Content $StateFile -Encoding UTF8

Ok "State captured: $rowCount accounts, total balance: $totalBalance"
Ok "Per-account balances: $StateFile"

Write-Host ""
Write-Host "  Recovery Target (T1) Recorded"
Write-Host "  Time:     $dbTs"
Write-Host "  Accounts: $rowCount"
Write-Host "  Balance:  $totalBalance"
Write-Host ""
Write-Host "  Everything after this timestamp will be LOST during recovery."
Write-Host ""
