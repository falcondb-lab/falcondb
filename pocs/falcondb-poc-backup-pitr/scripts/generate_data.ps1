#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Generate Transactional Data (Windows)
.PARAMETER Count
    Number of transactions to generate.
.PARAMETER Phase
    Label for this generation phase (e.g. "pre_backup", "post_backup").
#>
param(
    [int]$Count = 500,
    [string]$Phase = "default"
)

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$OutputDir  = Join-Path $PocRoot "output"

$HostAddr   = "127.0.0.1"
$FalconPort = 5433
$FalconUser = "falcon"
$FalconDb   = "pitr_demo"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$TxLog = Join-Path $OutputDir "txn_log_${Phase}.csv"

Write-Host "`n  Generating Data: $Count transactions (phase: $Phase)`n" -ForegroundColor Cyan

"seq,account_id,delta,balance_after,commit_ts" | Set-Content $TxLog

$committed = 0
$errors = 0

for ($i = 1; $i -le $Count; $i++) {
    $acctId = ($i % 100) + 1
    $delta  = ($i % 50) + 1

    try {
        $result = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c @"
BEGIN;
UPDATE accounts SET balance = balance + $delta, updated_at = NOW() WHERE account_id = $acctId;
SELECT balance, NOW() FROM accounts WHERE account_id = $acctId;
COMMIT;
"@ 2>$null

        if ($result) {
            $parts = ($result -split "`n")[0] -split '\|'
            $balance = $parts[0].Trim()
            $ts = $parts[1].Trim()
            "$i,$acctId,$delta,$balance,$ts" | Add-Content $TxLog
            $committed++
        } else { $errors++ }
    } catch { $errors++ }

    if ($i % 100 -eq 0) {
        Info "Progress: $i/$Count (committed: $committed, errors: $errors)"
    }
}

Ok "Phase '$Phase' complete: $committed committed, $errors errors"
Ok "Transaction log: $TxLog"

$total = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT SUM(balance) FROM accounts;" 2>$null).Trim()
Write-Host ""
Ok "Total balance across all accounts: $total"
"$Phase,$committed,$total" | Add-Content (Join-Path $OutputDir "balance_checkpoints.csv")
Write-Host ""
