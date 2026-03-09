#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #3 — Failover Under Load: Monitor Downtime (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }
function Banner($msg) { Write-Host "`n  $msg`n" -ForegroundColor Cyan }

Banner "Failover Timeline"

$CommittedFile = Join-Path $OutputDir "committed_markers.log"
$KillTsFile    = Join-Path $OutputDir "kill_timestamp.txt"
$PromoteTsFile = Join-Path $OutputDir "promote_timestamp.txt"
$MetricsFile   = Join-Path $OutputDir "load_metrics.json"
$TimelineFile  = Join-Path $OutputDir "failover_timeline.json"

$KillTS = if (Test-Path $KillTsFile) { (Get-Content $KillTsFile).Trim() } else { "unknown" }
Info "Primary killed at: $KillTS"

$PromoteTS = if (Test-Path $PromoteTsFile) { (Get-Content $PromoteTsFile).Trim() } else { "unknown" }
Info "Replica promoted at: $PromoteTS"

$TotalMarkers = (Get-Content $CommittedFile -ErrorAction SilentlyContinue | Where-Object { $_ -match '\d' }).Count
Info "Total committed markers: $TotalMarkers"

$LastBefore = "unknown"
$FirstAfter = "unknown"
$Reconnects = 0

if (Test-Path $MetricsFile) {
    $metrics = Get-Content $MetricsFile -Raw | ConvertFrom-Json
    $LastBefore = if ($metrics.last_commit_before_failure_ts) { $metrics.last_commit_before_failure_ts } else { "unknown" }
    $FirstAfter = if ($metrics.first_commit_after_reconnect_ts) { $metrics.first_commit_after_reconnect_ts } else { "unknown" }
    $Reconnects = if ($metrics.reconnect_count) { $metrics.reconnect_count } else { 0 }
}

Info "Last commit before failure: $LastBefore"
Info "First commit after reconnect: $FirstAfter"
Info "Reconnect attempts: $Reconnects"

$PreKillCount = 0
$preKillFile = Join-Path $OutputDir "pre_kill_marker_count.txt"
if (Test-Path $preKillFile) { $PreKillCount = [int](Get-Content $preKillFile).Trim() }

$DowntimeSec = "N/A"
if ($KillTS -ne "unknown" -and $PromoteTS -ne "unknown") {
    try {
        $t1 = [datetime]::Parse($KillTS)
        $t2 = [datetime]::Parse($PromoteTS)
        $DowntimeSec = [math]::Round(($t2 - $t1).TotalSeconds, 1)
    } catch {
        $DowntimeSec = "N/A"
    }
}

Info "Estimated downtime (kill -> promote ready): ${DowntimeSec}s"

$timeline = @{
    kill_timestamp                 = $KillTS
    promote_timestamp              = $PromoteTS
    last_commit_before_failure     = $LastBefore
    first_commit_after_reconnect   = $FirstAfter
    markers_before_kill            = $PreKillCount
    markers_total                  = $TotalMarkers
    reconnect_count                = $Reconnects
    estimated_downtime_sec         = "$DowntimeSec"
    note                           = "Downtime is informational, not a pass/fail metric."
} | ConvertTo-Json -Depth 2

$timeline | Set-Content $TimelineFile
Ok "Timeline: $TimelineFile"
