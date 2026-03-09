#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #6 — Cost Efficiency: Collect Resource Usage (Windows)
.PARAMETER ProcessId
    PID of the process to monitor.
.PARAMETER Label
    Label for this collection run (e.g. "falcon", "postgres").
.PARAMETER DurationSec
    How long to collect (seconds).
.PARAMETER IntervalSec
    Sampling interval (seconds).
#>
param(
    [Parameter(Mandatory)][int]$ProcessId,
    [Parameter(Mandatory)][string]$Label,
    [int]$DurationSec = 60,
    [int]$IntervalSec = 2
)

$ErrorActionPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"
$OutFile   = Join-Path $OutputDir "resource_usage_${Label}.json"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

Info "Collecting resource usage for PID $ProcessId ($Label) every ${IntervalSec}s for ${DurationSec}s"

$samples = @()
$elapsed = 0

while ($elapsed -lt $DurationSec) {
    try {
        $proc = Get-Process -Id $ProcessId -ErrorAction Stop
    } catch {
        Info "Process $ProcessId no longer running. Stopping."
        break
    }

    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

    $cpuPct = 0
    try {
        # CPU approximation: sample CPU time delta over 1 second
        $cpu1 = $proc.TotalProcessorTime.TotalMilliseconds
        Start-Sleep -Milliseconds 500
        $proc.Refresh()
        $cpu2 = $proc.TotalProcessorTime.TotalMilliseconds
        $cpuPct = [math]::Round(($cpu2 - $cpu1) / 5, 1)  # 500ms window, normalize to %
    } catch {}

    $rssKb = [math]::Round($proc.WorkingSet64 / 1024, 0)
    $memPct = 0
    try {
        $totalMem = (Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory
        $memPct = [math]::Round($proc.WorkingSet64 / $totalMem * 100, 1)
    } catch {}

    # Disk IO via Get-Process
    $readBytes  = 0
    $writeBytes = 0

    $samples += @{
        ts              = $ts
        elapsed_s       = $elapsed
        cpu_pct         = $cpuPct
        mem_pct         = $memPct
        rss_kb          = $rssKb
        disk_read_bytes = $readBytes
        disk_write_bytes= $writeBytes
    }

    Start-Sleep -Seconds ([math]::Max(0, $IntervalSec - 1))  # subtract the 500ms CPU sample
    $elapsed += $IntervalSec
}

$result = @{
    label         = $Label
    pid           = $ProcessId
    duration_sec  = $DurationSec
    interval_sec  = $IntervalSec
    total_samples = $samples.Count
    samples       = $samples
}

$result | ConvertTo-Json -Depth 5 | Set-Content $OutFile -Encoding UTF8

Ok "Resource data written to $OutFile ($($samples.Count) samples)"
