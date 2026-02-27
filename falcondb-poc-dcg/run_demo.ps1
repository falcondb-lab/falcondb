#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — One-Command Demo (Windows)
.DESCRIPTION
    Runs the entire Deterministic Commit Guarantee demo end-to-end:
      1. Start a 2-node FalconDB cluster
      2. Write orders to the primary
      3. Kill the primary with Stop-Process -Force
      4. Promote the replica
      5. Verify every committed order survived
      6. Print PASS/FAIL verdict
.EXAMPLE
    .\run_demo.ps1
    $env:ORDER_COUNT=5000; .\run_demo.ps1
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $env:FALCON_BIN)    { $env:FALCON_BIN    = "target\release\falcon_server.exe" }
if (-not $env:ORDER_COUNT)   { $env:ORDER_COUNT   = "1000" }
if (-not $env:REPL_WAIT_SEC) { $env:REPL_WAIT_SEC = "5" }

function Banner($msg) { Write-Host "`n  === $msg ===`n" -ForegroundColor Cyan }

try {

Banner "FalconDB - Deterministic Commit Guarantee (DCG) PoC"
Write-Host ""
Write-Host "  This demo proves that FalconDB never loses a committed transaction,"
Write-Host "  even if the primary server crashes."
Write-Host ""

# ── Step 1: Start cluster ─────────────────────────────────────────────────
Banner "Step 1/6: Starting 2-node cluster"
& (Join-Path $ScriptDir "scripts\start_cluster.ps1")

# ── Step 2: Run workload ──────────────────────────────────────────────────
Banner "Step 2/6: Writing $($env:ORDER_COUNT) orders to primary"
& (Join-Path $ScriptDir "scripts\run_workload.ps1")

# ── Step 3: Wait for replication ──────────────────────────────────────────
Banner "Step 3/6: Waiting $($env:REPL_WAIT_SEC)s for replication"
Start-Sleep -Seconds ([int]$env:REPL_WAIT_SEC)

# ── Step 4: Kill primary ─────────────────────────────────────────────────
Banner "Step 4/6: KILLING the primary (hard crash)"
& (Join-Path $ScriptDir "scripts\kill_primary.ps1")

Start-Sleep -Seconds 2

# ── Step 5: Promote replica ──────────────────────────────────────────────
Banner "Step 5/6: Promoting replica to primary"
& (Join-Path $ScriptDir "scripts\promote_replica.ps1")

# ── Step 6: Verify ────────────────────────────────────────────────────────
Banner "Step 6/6: VERIFICATION"
& (Join-Path $ScriptDir "scripts\verify_results.ps1")
$VerifyExit = $LASTEXITCODE

Write-Host ""
if ($VerifyExit -eq 0) {
    Write-Host "  Demo complete. The Deterministic Commit Guarantee held." -ForegroundColor Green
} else {
    Write-Host "  Demo complete. Verification FAILED." -ForegroundColor Red
}
Write-Host ""
Write-Host "  Output files: $(Join-Path $ScriptDir 'output')"
Write-Host ""

exit $VerifyExit

} finally {
    & (Join-Path $ScriptDir "scripts\cleanup.ps1") -ErrorAction SilentlyContinue
}
