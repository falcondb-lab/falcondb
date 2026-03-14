#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #7 — Backup & PITR: Verify Restored Data (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot     = Split-Path -Parent $ScriptDir
$OutputDir   = Join-Path $PocRoot "output"

$HostAddr    = "127.0.0.1"
$FalconPort  = 5433
$FalconUser  = "falcon"
$FalconDb    = "pitr_demo"

$StateFile   = Join-Path $OutputDir "state_at_t1.csv"
$ResultFile  = Join-Path $OutputDir "verification_result.txt"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  Verifying Restored Data`n" -ForegroundColor Cyan

if (-not (Test-Path $StateFile)) { Fail "State snapshot not found: $StateFile"; exit 1 }
Ok "Expected state file: $StateFile"

Info "Querying restored database..."
$RestoredState = Join-Path $OutputDir "state_restored.csv"
$restored = & psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT account_id, balance FROM accounts ORDER BY account_id;" 2>$null
$restored -replace '\|',',' | Set-Content $RestoredState -Encoding UTF8

$restoredTotal = (& psql -h $HostAddr -p $FalconPort -U $FalconUser -d $FalconDb -t -A -c "SELECT SUM(balance) FROM accounts;" 2>$null).Trim()

Info "Comparing per-account balances..."

$pyScript = @"
import sys
expected = {}
with open(r'$StateFile') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split(',')
        if len(parts) >= 2:
            try: expected[int(parts[0])] = int(parts[1])
            except: pass

restored = {}
with open(r'$RestoredState') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split(',')
        if len(parts) >= 2:
            try: restored[int(parts[0])] = int(parts[1])
            except: pass

mismatches = []
missing = []
extra = []
for a, e in sorted(expected.items()):
    if a not in restored: missing.append(a)
    elif restored[a] != e: mismatches.append((a, e, restored[a]))
for a in sorted(restored):
    if a not in expected: extra.append(a)

m = len(expected) - len(mismatches) - len(missing)
print(f'matches={m}')
print(f'mismatches={len(mismatches)}')
print(f'missing={len(missing)}')
print(f'extra={len(extra)}')
for a, e, g in mismatches[:10]:
    print(f'MISMATCH: account {a}: expected={e}, got={g}')
if not mismatches and not missing and not extra:
    print('RESULT=PASS')
else:
    print('RESULT=FAIL')
"@

$PythonCmd = "python"
try { & python3 --version 2>$null; $PythonCmd = "python3" } catch {}

$output = & $PythonCmd -c $pyScript 2>&1
$output | ForEach-Object {
    if ($_ -match "RESULT=PASS") { Ok "RESULT: PASS" }
    elseif ($_ -match "RESULT=FAIL") { Fail "RESULT: FAIL" }
    elseif ($_ -match "^MISMATCH") { Fail $_ }
    else { Info $_ }
}

$finalResult = ($output | Where-Object { $_ -match "^RESULT=" }) -replace "^RESULT=",""

$verifyTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$expectedCount = (Get-Content $StateFile | Where-Object { $_.Trim() }).Count
$restoredCount = (Get-Content $RestoredState | Where-Object { $_.Trim() }).Count

@"
Verification Result
===================
Date:               $verifyTs
Accounts (expected): $expectedCount
Accounts (restored): $restoredCount
Balance (restored):  $restoredTotal

$($output -join "`n")

Result: $finalResult
"@ | Set-Content $ResultFile -Encoding UTF8

if ($finalResult -eq "PASS") {
    Add-Content $ResultFile "`nDatabase restored exactly to target time."
}

Ok "Verification result: $ResultFile"

Write-Host ""
if ($finalResult -eq "PASS") {
    Write-Host "  PASS - Database restored exactly to target time." -ForegroundColor Green
} else {
    Write-Host "  FAIL - Restored data does not match expected state." -ForegroundColor Red
}
Write-Host ""
