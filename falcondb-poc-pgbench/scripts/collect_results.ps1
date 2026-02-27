#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB pgbench PoC — Collect and Compare Results (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot    = Split-Path -Parent $ScriptDir
$ResultsDir = Join-Path $PocRoot "results"
$ParsedDir  = Join-Path $ResultsDir "parsed"

function Ok($msg)     { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg)   { Write-Host "  > $msg" -ForegroundColor Yellow }
function Banner($msg) { Write-Host "`n  $msg`n" -ForegroundColor Cyan }

New-Item -ItemType Directory -Path $ParsedDir -Force | Out-Null

Banner "Collecting benchmark results"

function Parse-Run {
    param([string]$LogFile)
    if (-not (Test-Path $LogFile)) { return @{ tps = 0; latency = 0 } }
    $content = Get-Content $LogFile -Raw
    $tpsMatch = [regex]::Match($content, 'tps = ([0-9.]+)')
    $latMatch = [regex]::Match($content, 'latency average = ([0-9.]+)')
    $tps = if ($tpsMatch.Success) { [double]$tpsMatch.Groups[1].Value } else { 0 }
    $lat = if ($latMatch.Success) { [double]$latMatch.Groups[1].Value } else { 0 }
    return @{ tps = $tps; latency = $lat }
}

function Parse-System {
    param([string]$System)
    $rawDir = Join-Path $ResultsDir "raw\$System"
    $tpsAll = @()
    $latAll = @()
    for ($run = 1; $run -le 3; $run++) {
        $result = Parse-Run -LogFile (Join-Path $rawDir "run_${run}.log")
        $tpsAll += $result.tps
        $latAll += $result.latency
        Info "$System run $run`: TPS=$($result.tps), latency=$($result.latency) ms"
    }
    $tpsSorted = $tpsAll | Sort-Object
    $latSorted = $latAll | Sort-Object
    return @{
        tps_median = $tpsSorted[1]
        lat_median = $latSorted[1]
        tps_runs   = $tpsAll
        lat_runs   = $latAll
    }
}

Banner "FalconDB Results"
$falcon = Parse-System -System "falcon"
Ok "FalconDB median: TPS=$($falcon.tps_median), latency=$($falcon.lat_median) ms"

Banner "PostgreSQL Results"
$pg = Parse-System -System "postgres"
Ok "PostgreSQL median: TPS=$($pg.tps_median), latency=$($pg.lat_median) ms"

# ── Compute ratio ─────────────────────────────────────────────────────────
$tpsRatio = if ($pg.tps_median -gt 0) { [math]::Round($falcon.tps_median / $pg.tps_median, 2) } else { "N/A" }
$latRatio = if ($falcon.lat_median -gt 0) { [math]::Round($pg.lat_median / $falcon.lat_median, 2) } else { "N/A" }

# ── Generate summary.json ────────────────────────────────────────────────
$summary = @{
    timestamp  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    falcondb   = @{
        tps_median             = $falcon.tps_median
        latency_avg_median_ms  = $falcon.lat_median
        tps_runs               = $falcon.tps_runs
        latency_runs_ms        = $falcon.lat_runs
    }
    postgresql = @{
        tps_median             = $pg.tps_median
        latency_avg_median_ms  = $pg.lat_median
        tps_runs               = $pg.tps_runs
        latency_runs_ms        = $pg.lat_runs
    }
    comparison = @{
        tps_ratio_falcon_to_pg    = "$tpsRatio"
        latency_ratio_pg_to_falcon = "$latRatio"
    }
} | ConvertTo-Json -Depth 3

$summary | Set-Content (Join-Path $ParsedDir "summary.json")
Ok "Summary: $(Join-Path $ParsedDir 'summary.json')"

# ── Generate report.md ────────────────────────────────────────────────────
$ft = $falcon.tps_runs; $fl = $falcon.lat_runs
$pt = $pg.tps_runs; $pl = $pg.lat_runs

$report = @"
# FalconDB vs PostgreSQL - pgbench Comparison Report

**Generated**: $((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"))

---

## Summary

| System       | TPS (median) | Avg Latency (ms) |
|:-------------|-------------:|------------------:|
| **PostgreSQL** | $($pg.tps_median) | $($pg.lat_median) |
| **FalconDB** | $($falcon.tps_median) | $($falcon.lat_median) |

**TPS ratio** (FalconDB / PostgreSQL): **${tpsRatio}x**
**Latency ratio** (PostgreSQL / FalconDB): **${latRatio}x**

---

## All Runs

### FalconDB

| Run | TPS | Avg Latency (ms) |
|:---:|----:|------------------:|
| 1 | $($ft[0]) | $($fl[0]) |
| 2 | $($ft[1]) | $($fl[1]) |
| 3 | $($ft[2]) | $($fl[2]) |
| **Median** | **$($falcon.tps_median)** | **$($falcon.lat_median)** |

### PostgreSQL

| Run | TPS | Avg Latency (ms) |
|:---:|----:|------------------:|
| 1 | $($pt[0]) | $($pl[0]) |
| 2 | $($pt[1]) | $($pl[1]) |
| 3 | $($pt[2]) | $($pl[2]) |
| **Median** | **$($pg.tps_median)** | **$($pg.lat_median)** |

---

## Methodology

- **Benchmark tool**: pgbench (built-in tpcb-like workload)
- **Warm-up**: 1 run (15s), not included in results
- **Measured runs**: 3 per system
- **Result selection**: Median of 3 runs
- **Durability**: WAL enabled, fsync/fdatasync on both systems
- **All raw output preserved** in ``results/raw/``

---

## Fairness Statement

Both systems were tested on the same machine, same OS, same dataset scale,
same concurrency, same duration. WAL and durability were enabled on both.
No unsafe optimizations were applied to either system. All runs are preserved.

See ``docs/benchmark_methodology.md`` for full details.
"@

$report | Set-Content (Join-Path $ResultsDir "report.md")
Ok "Report: $(Join-Path $ResultsDir 'report.md')"

# ── Print summary ─────────────────────────────────────────────────────────
Banner "============================================="
Write-Host ""
Write-Host "  System         TPS (median)    Avg Latency (ms)"
Write-Host "  -------------- -------------- -----------------"
Write-Host ("  PostgreSQL     {0,14} {1,17}" -f $pg.tps_median, $pg.lat_median)
Write-Host ("  FalconDB       {0,14} {1,17}" -f $falcon.tps_median, $falcon.lat_median)
Write-Host ""
Write-Host "  TPS ratio (FalconDB / PostgreSQL): ${tpsRatio}x"
Write-Host "  Latency ratio (PG / Falcon):       ${latRatio}x"
Write-Host ""
Banner "============================================="
