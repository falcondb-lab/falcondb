#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: Verify Integrity (Windows)
.DESCRIPTION
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

$CommittedFile = Join-Path $OutputDir "committed_markers.log"
$ReportFile    = Join-Path $OutputDir "verification_report.txt"

if (-not (Test-Path $CommittedFile)) {
    Fail "committed_markers.log not found. Run the workload first."
    exit 1
}

# Extract marker_ids (format: marker_id|timestamp)
$CommittedIds = Get-Content $CommittedFile | ForEach-Object { if ($_ -match '^(\d+)') { $Matches[1] } } | Sort-Object { [long]$_ }
$CommittedCount = $CommittedIds.Count

if ($CommittedCount -eq 0) {
    Fail "No committed marker IDs found in log."
    exit 1
}

Info "Committed markers (from client log): $CommittedCount"

# Get surviving marker_ids
$SurvivingFile = Join-Path $OutputDir "surviving_markers.txt"
$surviving = & psql -h $Host_ -p $SurvivorPort -U $DbUser -d $DbName -t -A -c "SELECT marker_id FROM tx_markers ORDER BY marker_id;" 2>$null
$SurvivingIds = ($surviving -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^\d+$' })
$SurvivingIds | Set-Content $SurvivingFile
$SurvivingCount = $SurvivingIds.Count

Info "Markers on survivor (from database): $SurvivingCount"

# Check 1: Missing
$survivingSet = [System.Collections.Generic.HashSet[string]]::new()
foreach ($sid in $SurvivingIds) { [void]$survivingSet.Add($sid) }

$MissingFile = Join-Path $OutputDir "missing_markers.txt"
[System.Collections.Generic.List[string]]$MissingIds = @()
foreach ($cid in $CommittedIds) {
    if (-not $survivingSet.Contains($cid)) { $MissingIds.Add($cid) }
}
$MissingIds | Set-Content $MissingFile
$MissingCount = $MissingIds.Count

# Check 2: Phantom
$committedSet = [System.Collections.Generic.HashSet[string]]::new()
foreach ($cid in $CommittedIds) { [void]$committedSet.Add($cid) }

$PhantomFile = Join-Path $OutputDir "phantom_markers.txt"
[System.Collections.Generic.List[string]]$PhantomIds = @()
foreach ($sid in $SurvivingIds) {
    if (-not $committedSet.Contains($sid)) { $PhantomIds.Add($sid) }
}
$PhantomIds | Set-Content $PhantomFile
$PhantomCount = $PhantomIds.Count

# Check 3: Duplicates
$DuplicateCount = ($SurvivingIds | Group-Object | Where-Object { $_.Count -gt 1 }).Count

# Downtime
$Downtime = "N/A"
$timelineFile = Join-Path $OutputDir "failover_timeline.json"
if (Test-Path $timelineFile) {
    $tl = Get-Content $timelineFile -Raw | ConvertFrom-Json
    $Downtime = $tl.estimated_downtime_sec
}

# Verdict
if ($MissingCount -eq 0 -and $PhantomCount -eq 0 -and $DuplicateCount -eq 0) {
    $Verdict = "PASS"
} else {
    $Verdict = "FAIL"
}

$reportLines = @(
    "============================================================"
    "  FalconDB PoC #3 — Failover Under Load: Verification Report"
    "============================================================"
    ""
    "  Timestamp:       $((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'))"
    "  Survivor node:   ${Host_}:${SurvivorPort}"
    ""
    "  Total committed markers (acknowledged): $CommittedCount"
    "  Markers after failover:                 $SurvivingCount"
    ""
    "  Missing (data loss):             $MissingCount"
    "  Phantom (unexpected):            $PhantomCount"
    "  Duplicates:                      $DuplicateCount"
    ""
    "  Downtime window:                 ${Downtime}s"
    ""
    "  Result: $Verdict"
)

if ($Verdict -eq "PASS") {
    $reportLines += "  All acknowledged commits survived the failover."
    $reportLines += "  No data loss. No phantom commits. No duplicates."
} else {
    if ($MissingCount -gt 0) { $reportLines += "  DATA LOSS: $MissingCount committed markers are missing." }
    if ($PhantomCount -gt 0) { $reportLines += "  PHANTOM: $PhantomCount uncommitted markers appeared." }
    if ($DuplicateCount -gt 0) { $reportLines += "  DUPLICATES: $DuplicateCount duplicate marker_ids found." }
}

$reportLines += ""
$reportLines += "============================================================"

$report = $reportLines -join "`n"
Write-Host $report
$report | Set-Content $ReportFile

# Machine-readable evidence
$evidence = @{
    timestamp                = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    verdict                  = $Verdict
    committed_by_client      = $CommittedCount
    surviving_after_failover = $SurvivingCount
    missing_data_loss        = $MissingCount
    phantom_uncommitted      = $PhantomCount
    duplicates               = $DuplicateCount
    estimated_downtime_sec   = "$Downtime"
    survivor_node            = "${Host_}:${SurvivorPort}"
    claim                    = "FalconDB remains correct and predictable even when the worst thing happens at the worst time."
} | ConvertTo-Json -Depth 2

$evidence | Set-Content (Join-Path $OutputDir "verification_evidence.json")

Ok "Report:   $ReportFile"
Ok "Evidence: $(Join-Path $OutputDir 'verification_evidence.json')"

if ($Verdict -eq "PASS") { exit 0 } else { exit 1 }
