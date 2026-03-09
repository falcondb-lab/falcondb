#Requires -Version 7.0
<#
.SYNOPSIS
    FalconDB PoC #6 — Cost Efficiency: Generate Cost Comparison Report (Windows)
#>

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PocRoot   = Split-Path -Parent $ScriptDir
$OutputDir = Join-Path $PocRoot "output"
$Report    = Join-Path $OutputDir "cost_comparison_report.md"

function Ok($msg)   { Write-Host "  + $msg" -ForegroundColor Green }
function Info($msg) { Write-Host "  > $msg" -ForegroundColor Yellow }

Write-Host "`n  Generating Cost Comparison Report`n" -ForegroundColor Cyan

$pyScript = @"
import json, sys, os
from datetime import datetime

output_dir = r'$OutputDir'
report_path = r'$Report'

def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

def load_resource(path):
    data = load_json(path)
    if not data or not data.get("samples"):
        return {"avg_cpu": 0, "avg_rss_mb": 0, "peak_rss_mb": 0, "peak_cpu": 0}
    samples = data["samples"]
    cpus = [s.get("cpu_pct", 0) for s in samples]
    rss  = [s.get("rss_kb", 0) / 1024 for s in samples]
    return {
        "avg_cpu": round(sum(cpus) / len(cpus), 1) if cpus else 0,
        "peak_cpu": round(max(cpus), 1) if cpus else 0,
        "avg_rss_mb": round(sum(rss) / len(rss), 0) if rss else 0,
        "peak_rss_mb": round(max(rss), 0) if rss else 0,
    }

falcon_wl  = load_json(os.path.join(output_dir, "workload_results_falcon.json"))
pg_wl      = load_json(os.path.join(output_dir, "workload_results_postgres.json"))
falcon_res = load_resource(os.path.join(output_dir, "resource_usage_falcon.json"))
pg_res     = load_resource(os.path.join(output_dir, "resource_usage_postgres.json"))

falcon_hourly = 0.136
pg_hourly     = 0.4032
hours_per_month = 730
falcon_monthly = round(falcon_hourly * hours_per_month, 2)
pg_monthly     = round(pg_hourly * hours_per_month, 2)
savings_pct    = round((1 - falcon_monthly / pg_monthly) * 100, 0) if pg_monthly > 0 else 0

def fmt_wl(wl):
    if not wl:
        return {"rate":"N/A","committed":"N/A","errors":"N/A","p50":"N/A","p95":"N/A","p99":"N/A","max":"N/A"}
    return {
        "rate": f"{wl.get('actual_rate',0):.0f}",
        "committed": str(wl.get("total_committed",0)),
        "errors": str(wl.get("total_errors",0)),
        "p50": str(wl.get("latency_p50_us",0)),
        "p95": str(wl.get("latency_p95_us",0)),
        "p99": str(wl.get("latency_p99_us",0)),
        "max": str(wl.get("latency_max_us",0)),
    }

fw = fmt_wl(falcon_wl); pw = fmt_wl(pg_wl)

report = f'''# Cost Efficiency Comparison Report

**Generated**: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}

## Deployment Specifications

| | FalconDB (Small) | PostgreSQL (Production) |
|---|---|---|
| **vCPU** | 4 | 8 |
| **Memory Config** | 512 MB soft / 768 MB hard | 4 GB shared_buffers / 12 GB cache |
| **Durability** | WAL + fsync (FULL) | WAL + fsync (FULL) |

## Workload Results

| Metric | PostgreSQL | FalconDB |
|--------|-----------|----------|
| **Actual Rate (tx/s)** | {pw["rate"]} | {fw["rate"]} |
| **Total Committed** | {pw["committed"]} | {fw["committed"]} |
| **Total Errors** | {pw["errors"]} | {fw["errors"]} |
| **Latency p50 (us)** | {pw["p50"]} | {fw["p50"]} |
| **Latency p95 (us)** | {pw["p95"]} | {fw["p95"]} |
| **Latency p99 (us)** | {pw["p99"]} | {fw["p99"]} |

## Resource Usage

| Metric | PostgreSQL | FalconDB |
|--------|-----------|----------|
| **Avg CPU %** | {pg_res["avg_cpu"]} | {falcon_res["avg_cpu"]} |
| **Peak CPU %** | {pg_res["peak_cpu"]} | {falcon_res["peak_cpu"]} |
| **Avg RSS (MB)** | {pg_res["avg_rss_mb"]} | {falcon_res["avg_rss_mb"]} |
| **Peak RSS (MB)** | {pg_res["peak_rss_mb"]} | {falcon_res["peak_rss_mb"]} |

## Estimated Monthly Cost

| | PostgreSQL | FalconDB | Difference |
|---|---|---|---|
| **Monthly (730h)** | \${pg_monthly:,.2f} | \${falcon_monthly:,.2f} | **-{savings_pct:.0f}%** |

> Pricing: AWS on-demand, us-east-1. Illustrative.
'''

with open(report_path, 'w') as f:
    f.write(report)
print(f"Report written to {report_path}")
"@

& python -c $pyScript 2>&1
if ($LASTEXITCODE -ne 0) { & python3 -c $pyScript 2>&1 }

Ok "Cost comparison report generated: $Report"
Write-Host ""
