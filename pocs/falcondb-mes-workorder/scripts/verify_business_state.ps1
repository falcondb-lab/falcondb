#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB MES — Verify Business State After Failover (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$OutputDir  = Join-Path $PocRoot "output"
$API        = "http://127.0.0.1:8000"
$Report     = Join-Path $OutputDir "verification_report.txt"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$overallPass = $true

Write-Host "`n  Business State Verification After Failover`n" -ForegroundColor Cyan

# Health check
Info "Checking system health..."
try {
    $health = Invoke-RestMethod -Uri "$API/api/health" -TimeoutSec 5
    if ($health.status -eq "healthy") { Ok "System healthy" } else { Fail "Unhealthy"; $overallPass = $false }
} catch { Fail "System unreachable"; $overallPass = $false }

# Get all work orders
$wos = Invoke-RestMethod -Uri "$API/api/work-orders" -TimeoutSec 10
Ok "Found $($wos.Count) work orders"

$reportLines = @(
    "======================================================"
    "  FalconDB MES - Business State Verification Report"
    "======================================================"
    "  Time: $((Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss UTC'))"
    "  Work Orders: $($wos.Count)"
    "------------------------------------------------------"
    ""
)

foreach ($wo in $wos) {
    $woId = $wo.work_order_id
    Info "Verifying work order #${woId}..."

    $v = Invoke-RestMethod -Uri "$API/api/work-orders/$woId/verify" -TimeoutSec 10

    if ($v.overall -eq "PASS") { Ok "Work order #${woId}: PASS" }
    else { Fail "Work order #${woId}: FAIL"; $overallPass = $false }

    foreach ($inv in $v.invariants) {
        $s = if ($inv.passed) { "PASS" } else { "FAIL" }
        Write-Host "    $s`: $($inv.name) - $($inv.detail)"
    }

    $reportLines += @(
        "  Work Order #$woId"
        "  Planned:   $($v.planned_qty)"
        "  Reported:  $($v.total_reported_qty)"
        "  Completed: $($v.completed_qty)"
        "  Status:    $($v.status)"
        "  Result:    $($v.overall)"
        ""
    )
}

# Failover comparison
$beforeFile = Join-Path $OutputDir "before_kill.json"
if (Test-Path $beforeFile) {
    Write-Host "`n  Failover Comparison`n" -ForegroundColor Cyan

    $before = Get-Content $beforeFile | ConvertFrom-Json
    $bWoId = $before.work_order_id
    $after = Invoke-RestMethod -Uri "$API/api/work-orders/$bWoId/verify" -TimeoutSec 10

    Write-Host "  Work Order #${bWoId}:"
    Write-Host "                 Before Kill    After Recovery"
    Write-Host "  Reported:      $($before.total_reported_qty)              $($after.total_reported_qty)"
    Write-Host "  Completed:     $($before.completed_qty)              $($after.completed_qty)"
    Write-Host "  Status:        $($before.status)       $($after.status)"

    if ($before.completed_qty -eq $after.completed_qty -and
        $before.total_reported_qty -eq $after.total_reported_qty -and
        $before.status -eq $after.status) {
        Ok "Failover verified: data identical before and after crash"
        $reportLines += "  Failover: IDENTICAL"
    } else {
        Fail "Failover: DATA MISMATCH"
        $overallPass = $false
        $reportLines += "  Failover: MISMATCH"
    }
}

# Final result
$reportLines += ""
if ($overallPass) {
    $reportLines += "  Final Result: PASS"
    $reportLines += "  All production facts survived the crash."
    Write-Host ""
    Write-Host "  ================================================" -ForegroundColor Green
    Write-Host "  Final Result: PASS" -ForegroundColor Green
    Write-Host "  All production facts survived the crash." -ForegroundColor Green
    Write-Host "  Confirmed report = never lost." -ForegroundColor Green
    Write-Host "  ================================================" -ForegroundColor Green
} else {
    $reportLines += "  Final Result: FAIL"
    Write-Host ""
    Write-Host "  FAIL - Business state mismatch" -ForegroundColor Red
}

$reportLines | Set-Content $Report -Encoding UTF8
Ok "Full report: $Report"
Write-Host ""
