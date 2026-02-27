#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB DCG PoC — Verify Results (Windows)
.DESCRIPTION
    Connects to the surviving node and checks that every committed order exists.
    Exit code 0 = PASS, 1 = FAIL.
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

$Host_        = "127.0.0.1"
$SurvivorPort = if ($env:SURVIVOR_PORT) { [int]$env:SURVIVOR_PORT } else { 5434 }
$DbName       = "falcon"
$DbUser       = "falcon"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  x $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

$CommittedFile = Join-Path $OutputDir "committed_orders.log"
$ReportFile    = Join-Path $OutputDir "verification_report.txt"

if (-not (Test-Path $CommittedFile)) {
    Fail "committed_orders.log not found. Run the workload first."
    exit 1
}

# ── Get committed order IDs from log ──────────────────────────────────────
$CommittedIds = Get-Content $CommittedFile | Where-Object { $_ -match '^\d+$' } | Sort-Object { [long]$_ }
$CommittedCount = $CommittedIds.Count

if ($CommittedCount -eq 0) {
    Fail "No committed order IDs found in log."
    exit 1
}

Info "Committed orders (from client log): $CommittedCount"

# ── Get surviving order IDs from database ─────────────────────────────────
$SurvivingFile = Join-Path $OutputDir "surviving_orders.txt"
$surviving = & psql -h $Host_ -p $SurvivorPort -U $DbUser -d $DbName -t -A -c "SELECT order_id FROM orders ORDER BY order_id;" 2>$null
$SurvivingIds = ($surviving -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^\d+$' })
$SurvivingIds | Set-Content $SurvivingFile
$SurvivingCount = $SurvivingIds.Count

Info "Orders on survivor (from database): $SurvivingCount"

# ── Check 1: Missing orders ──────────────────────────────────────────────
$survivingSet = [System.Collections.Generic.HashSet[string]]::new()
foreach ($sid in $SurvivingIds) { [void]$survivingSet.Add($sid) }

$MissingFile = Join-Path $OutputDir "missing_orders.txt"
[System.Collections.Generic.List[string]]$MissingIds = @()
foreach ($cid in $CommittedIds) {
    if (-not $survivingSet.Contains($cid)) { $MissingIds.Add($cid) }
}
$MissingIds | Set-Content $MissingFile
$MissingCount = $MissingIds.Count

# ── Check 2: Phantom orders ─────────────────────────────────────────────
$committedSet = [System.Collections.Generic.HashSet[string]]::new()
foreach ($cid in $CommittedIds) { [void]$committedSet.Add($cid) }

$PhantomFile = Join-Path $OutputDir "phantom_orders.txt"
[System.Collections.Generic.List[string]]$PhantomIds = @()
foreach ($sid in $SurvivingIds) {
    if (-not $committedSet.Contains($sid)) { $PhantomIds.Add($sid) }
}
$PhantomIds | Set-Content $PhantomFile
$PhantomCount = $PhantomIds.Count

# ── Check 3: Duplicates ─────────────────────────────────────────────────
$DuplicateCount = ($SurvivingIds | Group-Object | Where-Object { $_.Count -gt 1 }).Count

# ── Build report ─────────────────────────────────────────────────────────
if ($MissingCount -eq 0 -and $PhantomCount -eq 0 -and $DuplicateCount -eq 0) {
    $Verdict = "PASS"
} else {
    $Verdict = "FAIL"
}

$reportLines = @(
    "============================================================"
    "  FalconDB DCG PoC — Verification Report"
    "============================================================"
    ""
    "  Timestamp:       $((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'))"
    "  Survivor node:   ${Host_}:${SurvivorPort}"
    ""
    "  Committed orders (acknowledged): $CommittedCount"
    "  Orders after failover:           $SurvivingCount"
    "  Missing (data loss):             $MissingCount"
    "  Phantom (unexpected):            $PhantomCount"
    "  Duplicates:                      $DuplicateCount"
    ""
    "  Result: $Verdict"
)

if ($Verdict -eq "PASS") {
    $reportLines += "  No committed data was lost."
} else {
    if ($MissingCount -gt 0) { $reportLines += "  DATA LOSS: $MissingCount committed orders are missing." }
    if ($PhantomCount -gt 0) { $reportLines += "  PHANTOM: $PhantomCount uncommitted orders appeared." }
    if ($DuplicateCount -gt 0) { $reportLines += "  DUPLICATES: $DuplicateCount duplicate order_ids found." }
}

$reportLines += ""
$reportLines += "============================================================"

$report = $reportLines -join "`n"
Write-Host $report
$report | Set-Content $ReportFile

# Machine-readable evidence
$evidence = @{
    timestamp              = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    verdict                = $Verdict
    committed_by_client    = $CommittedCount
    surviving_after_failover = $SurvivingCount
    missing_data_loss      = $MissingCount
    phantom_uncommitted    = $PhantomCount
    duplicates             = $DuplicateCount
    survivor_node          = "${Host_}:${SurvivorPort}"
    claim                  = "If FalconDB returns COMMIT OK, the data survives primary crash + failover."
} | ConvertTo-Json -Depth 2

$evidence | Set-Content (Join-Path $OutputDir "verification_evidence.json")

Ok "Report:   $ReportFile"
Ok "Evidence: $(Join-Path $OutputDir 'verification_evidence.json')"

if ($Verdict -eq "PASS") { exit 0 } else { exit 1 }
