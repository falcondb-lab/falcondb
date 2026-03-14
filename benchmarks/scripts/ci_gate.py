#!/usr/bin/env python3
"""
FalconDB PR Performance Gate
=============================
Compares a current benchmark run against a stored baseline and enforces
regression thresholds.  Zero external dependencies (stdlib only).

Exit codes:
  0 = all gates passed
  1 = one or more FAIL-level regressions detected
  2 = argument / file error

Usage:
  python3 ci_gate.py \
      --baseline benchmarks/baselines/main.json \
      --current  bench_results.json \
      --tps-fail   5.0 \
      --p99-fail  10.0 \
      --wal-fail  15.0 \
      --alloc-warn 20.0 \
      --noise-floor 2.0 \
      --summary-out bench_summary.json
"""

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple


# ── Threshold defaults (must match regression.rs) ────────────────────────────
DEFAULT_TPS_FAIL    = 5.0    # % TPS drop → FAIL
DEFAULT_P99_FAIL    = 10.0   # % P99 rise → FAIL
DEFAULT_WAL_FAIL    = 15.0   # % WAL P99 rise → FAIL  (wal/* bench ids)
DEFAULT_ALLOC_WARN  = 20.0   # % alloc/op rise → WARN
DEFAULT_NOISE_FLOOR = 2.0    # % change below which = noise
DEFAULT_IMPROVE_PCT = 5.0    # % improvement worth reporting


# ── Report level ──────────────────────────────────────────────────────────────
FAIL    = "FAIL"
WARN    = "WARN"
IMPROVE = "IMPROVE"
NEW     = "NEW"
OK      = "OK"


def load_json(path: str) -> Dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {path}", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in {path}: {e}", file=sys.stderr)
        sys.exit(2)


def delta_pct(baseline: float, current: float) -> Optional[float]:
    if baseline == 0:
        return None
    return (current - baseline) / abs(baseline) * 100.0


def check_metric(
    bench_id: str,
    metric: str,
    baseline_val: Optional[float],
    current_val: Optional[float],
    fail_pct: float,
    noise_floor: float,
    improve_pct: float,
    higher_is_better: bool,
) -> Optional[Tuple[str, str, float]]:
    """Returns (level, message, delta_pct) or None if noise."""
    if baseline_val is None or current_val is None:
        return None
    d = delta_pct(baseline_val, current_val)
    if d is None or abs(d) < noise_floor:
        return None

    # For higher-is-better (TPS): regression = large negative delta
    # For lower-is-better (P99): regression = large positive delta
    if higher_is_better:
        if d < -fail_pct:
            return (FAIL, f"{metric} dropped {d:+.1f}% (threshold: -{fail_pct:.0f}%,"
                          f" baseline={baseline_val:.0f}, current={current_val:.0f})", d)
        if d > improve_pct:
            return (IMPROVE, f"{metric} improved {d:+.1f}%"
                             f" (baseline={baseline_val:.0f} → {current_val:.0f})", d)
    else:
        if d > fail_pct:
            return (FAIL, f"{metric} degraded {d:+.1f}% (threshold: +{fail_pct:.0f}%,"
                          f" baseline={baseline_val:.0f}µs, current={current_val:.0f}µs)", d)
        if d < -improve_pct:
            return (IMPROVE, f"{metric} improved {d:+.1f}%"
                             f" (baseline={baseline_val:.0f}µs → {current_val:.0f}µs)", d)
    return (OK, "", d)


def run_checks(
    baseline: Dict[str, Any],
    current: Dict[str, Any],
    cfg: argparse.Namespace,
) -> Tuple[List[Dict], bool]:
    """Returns (report_list, passed)."""
    base_results = baseline.get("results", {})
    curr_results = current.get("results", {})
    reports = []
    any_fail = False

    for bench_id, cur in curr_results.items():
        base = base_results.get(bench_id)
        if base is None:
            reports.append({"level": NEW, "bench_id": bench_id,
                             "message": "No baseline found; recording as new baseline"})
            continue

        is_wal = bench_id.startswith("wal/")

        # ── TPS ──────────────────────────────────────────────────────────────
        r = check_metric(
            bench_id, "TPS",
            base.get("tps"), cur.get("tps"),
            cfg.tps_fail, cfg.noise_floor, cfg.improve_pct,
            higher_is_better=True,
        )
        if r and r[0] != OK:
            level, msg, d = r
            reports.append({"level": level, "bench_id": bench_id, "metric": "TPS",
                             "delta_pct": round(d, 2), "message": msg})
            if level == FAIL:
                any_fail = True

        # ── P99 latency ───────────────────────────────────────────────────────
        p99_threshold = cfg.wal_fail if is_wal else cfg.p99_fail
        r = check_metric(
            bench_id, "P99_us",
            base.get("latency_p99_us"), cur.get("latency_p99_us"),
            p99_threshold, cfg.noise_floor, cfg.improve_pct,
            higher_is_better=False,
        )
        if r and r[0] != OK:
            level, msg, d = r
            reports.append({"level": level, "bench_id": bench_id, "metric": "P99_us",
                             "delta_pct": round(d, 2), "message": msg})
            if level == FAIL:
                any_fail = True

        # ── alloc/op (WARN only) ──────────────────────────────────────────────
        base_alloc = base.get("alloc_count_per_op")
        curr_alloc = cur.get("alloc_count_per_op")
        if base_alloc and curr_alloc:
            d = delta_pct(base_alloc, curr_alloc)
            if d is not None and d > cfg.alloc_warn:
                reports.append({
                    "level": WARN, "bench_id": bench_id, "metric": "alloc/op",
                    "delta_pct": round(d, 2),
                    "message": f"alloc/op increased {d:+.1f}% (threshold: +{cfg.alloc_warn:.0f}%)",
                })

    return reports, not any_fail


def build_markdown_summary(
    reports: List[Dict],
    current: Dict[str, Any],
    passed: bool,
) -> str:
    lines = ["## 🏁 FalconDB Performance Gate\n"]
    if passed:
        lines.append("✅ **All performance gates passed.**\n")
    else:
        lines.append("❌ **Performance regression detected — merge blocked.**\n")

    lines.append("\n| Bench | TPS | P99 µs | P999 µs | Status |")
    lines.append("|---|---:|---:|---:|:---:|")

    fail_ids = {r["bench_id"] for r in reports if r["level"] == FAIL}
    warn_ids = {r["bench_id"] for r in reports if r["level"] == WARN}
    impr_ids = {r["bench_id"] for r in reports if r["level"] == IMPROVE}

    results = current.get("results", {})
    for bench_id in sorted(results.keys()):
        m = results[bench_id]
        tps   = f"{m.get('tps', 0):.0f}"
        p99   = f"{m.get('latency_p99_us', 0):.0f}"
        p999  = f"{m.get('latency_p999_us', 0):.0f}"
        if bench_id in fail_ids:
            status = "❌ FAIL"
        elif bench_id in impr_ids:
            status = "🎉"
        elif bench_id in warn_ids:
            status = "⚠️ WARN"
        else:
            status = "✅"
        lines.append(f"| `{bench_id}` | {tps} | {p99} | {p999} | {status} |")

    fails_warns = [r for r in reports if r["level"] in (FAIL, WARN)]
    if fails_warns:
        lines.append("\n### Issues\n")
        for r in fails_warns:
            icon = "❌" if r["level"] == FAIL else "⚠️"
            lines.append(f"- {icon} `{r['bench_id']}` — {r['message']}")

    improves = [r for r in reports if r["level"] == IMPROVE]
    if improves:
        lines.append("\n### Improvements 🎉\n")
        for r in improves:
            lines.append(f"- ✨ `{r['bench_id']}` — {r['message']}")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="FalconDB PR performance gate")
    parser.add_argument("--baseline",    required=True,  help="Baseline JSON file")
    parser.add_argument("--current",     required=True,  help="Current run JSON file")
    parser.add_argument("--tps-fail",    type=float, default=DEFAULT_TPS_FAIL,    dest="tps_fail")
    parser.add_argument("--p99-fail",    type=float, default=DEFAULT_P99_FAIL,    dest="p99_fail")
    parser.add_argument("--wal-fail",    type=float, default=DEFAULT_WAL_FAIL,    dest="wal_fail")
    parser.add_argument("--alloc-warn",  type=float, default=DEFAULT_ALLOC_WARN,  dest="alloc_warn")
    parser.add_argument("--noise-floor", type=float, default=DEFAULT_NOISE_FLOOR, dest="noise_floor")
    parser.add_argument("--improve-pct", type=float, default=DEFAULT_IMPROVE_PCT, dest="improve_pct")
    parser.add_argument("--summary-out", default=None, dest="summary_out",
                        help="Write JSON + Markdown summary to this file (optional)")
    args = parser.parse_args()

    baseline = load_json(args.baseline)
    current  = load_json(args.current)

    reports, passed = run_checks(baseline, current, args)
    markdown = build_markdown_summary(reports, current, passed)

    # ── Console output ────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  FalconDB Performance Gate — {current.get('git_commit', 'unknown')}")
    print(f"{'='*60}")

    for r in reports:
        icon = {"FAIL": "❌", "WARN": "⚠️ ", "IMPROVE": "🎉", "NEW": "🆕", "OK": "  "}.get(r["level"], "  ")
        print(f"  {icon} [{r['level']:7}] {r['bench_id']}: {r.get('message', '')}")

    print()
    if passed:
        print("✅  PASSED — all performance gates cleared.")
    else:
        print("❌  FAILED — performance regression detected.")
        fails = [r for r in reports if r["level"] == FAIL]
        for f in fails:
            print(f"   → {f['bench_id']}: {f['message']}")
    print()

    # ── Optional summary file ─────────────────────────────────────────────────
    if args.summary_out:
        summary = {
            "passed":          passed,
            "git_commit":      current.get("git_commit", ""),
            "reports":         reports,
            "markdown_summary": markdown,
        }
        with open(args.summary_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"Summary written to {args.summary_out}")

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
