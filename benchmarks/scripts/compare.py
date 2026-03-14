#!/usr/bin/env python3
"""
FalconDB Benchmark Comparison Tool
====================================
Compares two benchmark result JSON files and outputs a Markdown table.
Zero external dependencies (stdlib only).

Usage:
  python3 compare.py benchmarks/baselines/main.json bench_results.json
  python3 compare.py --format json baseline.json current.json
"""

import argparse
import json
import sys
from typing import Any, Dict, Optional


def load(path: str) -> Dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(2)


def pct(base: float, cur: float) -> str:
    if base == 0:
        return "N/A"
    d = (cur - base) / abs(base) * 100.0
    sign = "+" if d > 0 else ""
    color = "🔺" if d > 5 else ("🔻" if d < -5 else "")
    return f"{sign}{d:.1f}% {color}".strip()


def tps_pct(base: float, cur: float) -> str:
    if base == 0:
        return "N/A"
    d = (cur - base) / abs(base) * 100.0
    sign = "+" if d > 0 else ""
    icon = "🎉" if d > 5 else ("❌" if d < -5 else "")
    return f"{sign}{d:.1f}% {icon}".strip()


def fmt_float(v: Optional[float], decimals: int = 0) -> str:
    if v is None:
        return "—"
    return f"{v:.{decimals}f}"


def compare_markdown(base_data: Dict, cur_data: Dict) -> str:
    base_results = base_data.get("results", {})
    cur_results  = cur_data.get("results", {})

    base_commit = base_data.get("git_commit", "baseline")[:8]
    cur_commit  = cur_data.get("git_commit",  "current")[:8]
    base_ts     = base_data.get("timestamp",  "?")
    cur_ts      = cur_data.get("timestamp",   "?")

    lines = [
        f"# FalconDB Benchmark Comparison",
        f"",
        f"| | Baseline | Current |",
        f"|---|---|---|",
        f"| Commit | `{base_commit}` | `{cur_commit}` |",
        f"| Timestamp | {base_ts} | {cur_ts} |",
        f"",
        f"## Results",
        f"",
        f"| Bench | Workers | TPS Base | TPS Cur | TPS Δ | P99 Base (µs) | P99 Cur (µs) | P99 Δ | P999 Cur (µs) |",
        f"|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    all_ids = sorted(set(list(base_results.keys()) + list(cur_results.keys())))

    for bench_id in all_ids:
        base = base_results.get(bench_id, {})
        cur  = cur_results.get(bench_id, {})

        workers  = cur.get("workers") or base.get("workers") or "?"
        tps_b    = base.get("tps", 0.0)
        tps_c    = cur.get("tps", 0.0)
        p99_b    = base.get("latency_p99_us", 0.0)
        p99_c    = cur.get("latency_p99_us", 0.0)
        p999_c   = cur.get("latency_p999_us")

        if not base:
            tps_delta = "🆕 NEW"
            p99_delta = "🆕 NEW"
        elif not cur:
            tps_delta = "⚠️ MISSING"
            p99_delta = "⚠️ MISSING"
        else:
            tps_delta = tps_pct(tps_b, tps_c)
            p99_delta = pct(p99_b, p99_c)

        lines.append(
            f"| `{bench_id}` | {workers} "
            f"| {fmt_float(tps_b if tps_b else None)} "
            f"| {fmt_float(tps_c if tps_c else None)} "
            f"| {tps_delta} "
            f"| {fmt_float(p99_b if p99_b else None)} "
            f"| {fmt_float(p99_c if p99_c else None)} "
            f"| {p99_delta} "
            f"| {fmt_float(p999_c)} |"
        )

    # Summary statistics
    regressions = 0
    improvements = 0
    for bench_id in all_ids:
        base = base_results.get(bench_id, {})
        cur  = cur_results.get(bench_id, {})
        if not base or not cur:
            continue
        tps_b, tps_c = base.get("tps", 0), cur.get("tps", 0)
        if tps_b > 0:
            d = (tps_c - tps_b) / tps_b * 100
            if d < -5:
                regressions += 1
            elif d > 5:
                improvements += 1

    lines += [
        f"",
        f"## Summary",
        f"",
        f"- **{improvements}** bench(es) improved by >5%",
        f"- **{regressions}** bench(es) regressed by >5%",
        f"- **{len(all_ids) - improvements - regressions}** bench(es) within noise",
    ]

    return "\n".join(lines)


def compare_json(base_data: Dict, cur_data: Dict) -> Dict:
    base_results = base_data.get("results", {})
    cur_results  = cur_data.get("results", {})
    deltas = {}
    for bench_id in sorted(set(list(base_results.keys()) + list(cur_results.keys()))):
        base = base_results.get(bench_id, {})
        cur  = cur_results.get(bench_id, {})
        if base and cur:
            tps_b, tps_c = base.get("tps", 0), cur.get("tps", 0)
            p99_b, p99_c = base.get("latency_p99_us", 0), cur.get("latency_p99_us", 0)
            deltas[bench_id] = {
                "tps_baseline":  tps_b,
                "tps_current":   tps_c,
                "tps_delta_pct": round((tps_c - tps_b) / tps_b * 100, 2) if tps_b else None,
                "p99_baseline":  p99_b,
                "p99_current":   p99_c,
                "p99_delta_pct": round((p99_c - p99_b) / p99_b * 100, 2) if p99_b else None,
            }
        elif not base:
            deltas[bench_id] = {"status": "NEW"}
        else:
            deltas[bench_id] = {"status": "MISSING_FROM_CURRENT"}
    return deltas


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two FalconDB benchmark runs")
    parser.add_argument("baseline", help="Baseline JSON file")
    parser.add_argument("current",  help="Current run JSON file")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown",
                        help="Output format (default: markdown)")
    parser.add_argument("--out", default=None, help="Write output to file instead of stdout")
    args = parser.parse_args()

    base_data = load(args.baseline)
    cur_data  = load(args.current)

    if args.format == "markdown":
        output = compare_markdown(base_data, cur_data)
    else:
        output = json.dumps(compare_json(base_data, cur_data), indent=2)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Written to {args.out}")
    else:
        print(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
