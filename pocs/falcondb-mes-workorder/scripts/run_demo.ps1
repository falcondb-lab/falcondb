#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB MES — Full Demo: Scenario A + B + C (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$OutputDir  = Join-Path $PocRoot "output"
$API        = "http://127.0.0.1:8000"

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }
function Banner($msg) { Write-Host "`n  $msg`n" -ForegroundColor Cyan }

function ApiPost($url, $body = "{}") {
    Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" -Body $body -TimeoutSec 10
}
function ApiGet($url) {
    Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 10
}

# ═══════════════════════════════════════════════════════════════════════════
Banner "Scenario A: Normal Production"

Info "Creating work order: MOTOR-A100, qty=1000"
$wo = ApiPost "$API/api/work-orders" '{"product_code":"MOTOR-A100","planned_qty":1000,"operations":["cutting","welding","assembly","inspection"]}'
$woId = $wo.work_order_id
Ok "Work order #$woId created"

$ops = ApiGet "$API/api/work-orders/$woId/operations"
$opNames = @("cutting","welding","assembly","inspection")

for ($i = 0; $i -lt $ops.Count; $i++) {
    $opId = $ops[$i].operation_id
    $opName = $opNames[$i]
    ApiPost "$API/api/operations/$opId/start" | Out-Null
    for ($b = 1; $b -le 4; $b++) {
        ApiPost "$API/api/operations/$opId/report" "{`"report_qty`":250,`"reported_by`":`"operator_$b`"}" | Out-Null
    }
    ApiPost "$API/api/operations/$opId/complete" | Out-Null
    Ok "  $opName : DONE (1000 units)"
}

ApiPost "$API/api/work-orders/$woId/complete" | Out-Null
Ok "Work order #${woId}: COMPLETED"

$verify = ApiGet "$API/api/work-orders/$woId/verify"
Write-Host ""
Write-Host "  Scenario A Result"
Write-Host "  Planned:   $($verify.planned_qty)"
Write-Host "  Reported:  $($verify.total_reported_qty)"
Write-Host "  Completed: $($verify.completed_qty)"
Write-Host "  Status:    $($verify.status)"
Write-Host "  Result:    $($verify.overall)" -ForegroundColor $(if ($verify.overall -eq "PASS") {"Green"} else {"Red"})
Write-Host ""

$verify | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $OutputDir "pre_failover_state.json")

# ═══════════════════════════════════════════════════════════════════════════
Banner "Scenario B: Failover During Production"

$wo2 = ApiPost "$API/api/work-orders" '{"product_code":"GEARBOX-B200","planned_qty":500,"operations":["machining","heat_treatment","grinding","qa_check"]}'
$wo2Id = $wo2.work_order_id
Ok "Work order #$wo2Id created (GEARBOX-B200)"

$ops2 = ApiGet "$API/api/work-orders/$wo2Id/operations"

for ($i = 0; $i -lt 2; $i++) {
    $opId = $ops2[$i].operation_id
    ApiPost "$API/api/operations/$opId/start" | Out-Null
    ApiPost "$API/api/operations/$opId/report" '{"report_qty":200,"reported_by":"worker_a"}' | Out-Null
    ApiPost "$API/api/operations/$opId/report" '{"report_qty":50,"reported_by":"worker_b"}' | Out-Null
    ApiPost "$API/api/operations/$opId/complete" | Out-Null
}
Ok "Operations 1-2 completed (250 each)"

$op3Id = $ops2[2].operation_id
ApiPost "$API/api/operations/$op3Id/start" | Out-Null
ApiPost "$API/api/operations/$op3Id/report" '{"report_qty":100,"reported_by":"worker_c"}' | Out-Null
Ok "Operation 3 (grinding): RUNNING, 100 reported"

$before = ApiGet "$API/api/work-orders/$wo2Id/verify"
$before | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $OutputDir "before_kill.json")
Ok "State saved: completed=$($before.completed_qty), reported=$($before.total_reported_qty)"

Write-Host ""
Write-Host "  READY FOR FAILOVER TEST"
Write-Host "  Next: kill_primary.ps1 -> promote_replica.ps1 -> verify_business_state.ps1"
Write-Host ""

# ═══════════════════════════════════════════════════════════════════════════
Banner "Scenario C: Concurrent Reporting"

$wo3 = ApiPost "$API/api/work-orders" '{"product_code":"SHAFT-C300","planned_qty":600,"operations":["turning","polishing"]}'
$wo3Id = $wo3.work_order_id
$ops3 = ApiGet "$API/api/work-orders/$wo3Id/operations"
$firstOp = $ops3[0].operation_id
ApiPost "$API/api/operations/$firstOp/start" | Out-Null

Info "Submitting 10 concurrent reports (60 each)..."
$jobs = 1..10 | ForEach-Object {
    $t = $_
    Start-Job -ScriptBlock {
        param($api, $opId, $t)
        Invoke-RestMethod -Uri "$api/api/operations/$opId/report" -Method Post `
            -ContentType "application/json" `
            -Body "{`"report_qty`":60,`"reported_by`":`"terminal_$t`"}"
    } -ArgumentList $API, $firstOp, $t
}
$jobs | Wait-Job | Out-Null
$jobs | Remove-Job
Ok "All 10 concurrent reports submitted"

$cv = ApiGet "$API/api/work-orders/$wo3Id/verify"
Write-Host ""
Write-Host "  Scenario C Result"
Write-Host "  Expected: 600 (10 x 60)"
Write-Host "  Actual:   $($cv.total_reported_qty)"
Write-Host "  Result:   $($cv.overall)" -ForegroundColor $(if ($cv.overall -eq "PASS") {"Green"} else {"Red"})
Write-Host ""

Banner "Demo Complete"
