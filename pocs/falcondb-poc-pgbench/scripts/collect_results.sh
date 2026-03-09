#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Collect and Compare Results
# ============================================================================
# Parses raw pgbench output from both systems, selects the median run,
# and generates:
#   - results/parsed/summary.json  (machine-readable)
#   - results/report.md            (human-readable comparison)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"
PARSED_DIR="${RESULTS_DIR}/parsed"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

mkdir -p "${PARSED_DIR}"

banner "Collecting benchmark results"

# ── Parse function ────────────────────────────────────────────────────────
parse_run() {
  local log_file="$1"
  local tps lat_avg

  if [ ! -f "${log_file}" ]; then
    echo "0|0"
    return
  fi

  # pgbench output formats:
  #   tps = 12345.678901 (without initial connection time)
  #   latency average = 1.234 ms
  tps=$(grep -oP 'tps = \K[0-9.]+' "${log_file}" | tail -1 2>/dev/null || echo "0")
  lat_avg=$(grep -oP 'latency average = \K[0-9.]+' "${log_file}" 2>/dev/null || echo "0")

  echo "${tps}|${lat_avg}"
}

# ── Parse all runs for a system ───────────────────────────────────────────
parse_system() {
  local system="$1"
  local raw_dir="${RESULTS_DIR}/raw/${system}"
  local tps_values=()
  local lat_values=()

  for run in 1 2 3; do
    local log="${raw_dir}/run_${run}.log"
    local result
    result=$(parse_run "${log}")
    local tps=${result%%|*}
    local lat=${result##*|}

    tps_values+=("${tps}")
    lat_values+=("${lat}")

    info "${system} run ${run}: TPS=${tps}, latency=${lat} ms"
  done

  # Find median (sort 3 values, pick middle)
  local sorted_tps
  sorted_tps=$(printf '%s\n' "${tps_values[@]}" | sort -n)
  local median_tps
  median_tps=$(echo "${sorted_tps}" | sed -n '2p')

  local sorted_lat
  sorted_lat=$(printf '%s\n' "${lat_values[@]}" | sort -n)
  local median_lat
  median_lat=$(echo "${sorted_lat}" | sed -n '2p')

  echo "${median_tps}|${median_lat}|${tps_values[0]}|${tps_values[1]}|${tps_values[2]}|${lat_values[0]}|${lat_values[1]}|${lat_values[2]}"
}

# ── Parse both systems ────────────────────────────────────────────────────
banner "FalconDB Results"
FALCON_RESULT=$(parse_system "falcon")
FALCON_TPS_MEDIAN=${FALCON_RESULT%%|*}
FALCON_REST=${FALCON_RESULT#*|}
FALCON_LAT_MEDIAN=${FALCON_REST%%|*}
FALCON_REST2=${FALCON_REST#*|}
FALCON_TPS_1=${FALCON_REST2%%|*}; FALCON_REST2=${FALCON_REST2#*|}
FALCON_TPS_2=${FALCON_REST2%%|*}; FALCON_REST2=${FALCON_REST2#*|}
FALCON_TPS_3=${FALCON_REST2%%|*}; FALCON_REST2=${FALCON_REST2#*|}
FALCON_LAT_1=${FALCON_REST2%%|*}; FALCON_REST2=${FALCON_REST2#*|}
FALCON_LAT_2=${FALCON_REST2%%|*}; FALCON_REST2=${FALCON_REST2#*|}
FALCON_LAT_3=${FALCON_REST2}

ok "FalconDB median: TPS=${FALCON_TPS_MEDIAN}, latency=${FALCON_LAT_MEDIAN} ms"

banner "PostgreSQL Results"
PG_RESULT=$(parse_system "postgres")
PG_TPS_MEDIAN=${PG_RESULT%%|*}
PG_REST=${PG_RESULT#*|}
PG_LAT_MEDIAN=${PG_REST%%|*}
PG_REST2=${PG_REST#*|}
PG_TPS_1=${PG_REST2%%|*}; PG_REST2=${PG_REST2#*|}
PG_TPS_2=${PG_REST2%%|*}; PG_REST2=${PG_REST2#*|}
PG_TPS_3=${PG_REST2%%|*}; PG_REST2=${PG_REST2#*|}
PG_LAT_1=${PG_REST2%%|*}; PG_REST2=${PG_REST2#*|}
PG_LAT_2=${PG_REST2%%|*}; PG_REST2=${PG_REST2#*|}
PG_LAT_3=${PG_REST2}

ok "PostgreSQL median: TPS=${PG_TPS_MEDIAN}, latency=${PG_LAT_MEDIAN} ms"

# ── Compute ratio ─────────────────────────────────────────────────────────
TPS_RATIO="N/A"
LAT_RATIO="N/A"
if command -v bc &>/dev/null && [ "${PG_TPS_MEDIAN}" != "0" ]; then
  TPS_RATIO=$(echo "scale=2; ${FALCON_TPS_MEDIAN} / ${PG_TPS_MEDIAN}" | bc 2>/dev/null || echo "N/A")
fi
if command -v bc &>/dev/null && [ "${FALCON_LAT_MEDIAN}" != "0" ]; then
  LAT_RATIO=$(echo "scale=2; ${PG_LAT_MEDIAN} / ${FALCON_LAT_MEDIAN}" | bc 2>/dev/null || echo "N/A")
fi

# ── Generate summary.json ────────────────────────────────────────────────
cat > "${PARSED_DIR}/summary.json" <<EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "falcondb": {
    "tps_median": ${FALCON_TPS_MEDIAN},
    "latency_avg_median_ms": ${FALCON_LAT_MEDIAN},
    "tps_runs": [${FALCON_TPS_1}, ${FALCON_TPS_2}, ${FALCON_TPS_3}],
    "latency_runs_ms": [${FALCON_LAT_1}, ${FALCON_LAT_2}, ${FALCON_LAT_3}]
  },
  "postgresql": {
    "tps_median": ${PG_TPS_MEDIAN},
    "latency_avg_median_ms": ${PG_LAT_MEDIAN},
    "tps_runs": [${PG_TPS_1}, ${PG_TPS_2}, ${PG_TPS_3}],
    "latency_runs_ms": [${PG_LAT_1}, ${PG_LAT_2}, ${PG_LAT_3}]
  },
  "comparison": {
    "tps_ratio_falcon_to_pg": "${TPS_RATIO}",
    "latency_ratio_pg_to_falcon": "${LAT_RATIO}"
  }
}
EOF

ok "Summary: ${PARSED_DIR}/summary.json"

# ── Generate report.md ────────────────────────────────────────────────────
REPORT="${RESULTS_DIR}/report.md"

cat > "${REPORT}" <<REPORT
# FalconDB vs PostgreSQL — pgbench Comparison Report

**Generated**: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

---

## Summary

| System       | TPS (median) | Avg Latency (ms) |
|:-------------|-------------:|------------------:|
| **PostgreSQL** | ${PG_TPS_MEDIAN} | ${PG_LAT_MEDIAN} |
| **FalconDB** | ${FALCON_TPS_MEDIAN} | ${FALCON_LAT_MEDIAN} |

**TPS ratio** (FalconDB / PostgreSQL): **${TPS_RATIO}x**
**Latency ratio** (PostgreSQL / FalconDB): **${LAT_RATIO}x**

---

## All Runs

### FalconDB

| Run | TPS | Avg Latency (ms) |
|:---:|----:|------------------:|
| 1 | ${FALCON_TPS_1} | ${FALCON_LAT_1} |
| 2 | ${FALCON_TPS_2} | ${FALCON_LAT_2} |
| 3 | ${FALCON_TPS_3} | ${FALCON_LAT_3} |
| **Median** | **${FALCON_TPS_MEDIAN}** | **${FALCON_LAT_MEDIAN}** |

### PostgreSQL

| Run | TPS | Avg Latency (ms) |
|:---:|----:|------------------:|
| 1 | ${PG_TPS_1} | ${PG_LAT_1} |
| 2 | ${PG_TPS_2} | ${PG_LAT_2} |
| 3 | ${PG_TPS_3} | ${PG_LAT_3} |
| **Median** | **${PG_TPS_MEDIAN}** | **${PG_LAT_MEDIAN}** |

---

## Methodology

- **Benchmark tool**: pgbench (built-in tpcb-like workload)
- **Warm-up**: 1 run (15s), not included in results
- **Measured runs**: 3 per system
- **Result selection**: Median of 3 runs
- **Durability**: WAL enabled, fsync/fdatasync on both systems
- **All raw output preserved** in \`results/raw/\`

---

## Fairness Statement

Both systems were tested on the same machine, same OS, same dataset scale,
same concurrency, same duration. WAL and durability were enabled on both.
No unsafe optimizations were applied to either system. All runs are preserved.

See \`docs/benchmark_methodology.md\` for full details.
REPORT

ok "Report: ${REPORT}"

# ── Print summary to terminal ─────────────────────────────────────────────
banner "═══════════════════════════════════════════════"
echo ""
echo "  System         TPS (median)    Avg Latency (ms)"
echo "  -------------- -------------- -----------------"
printf "  PostgreSQL     %14s %17s\n" "${PG_TPS_MEDIAN}" "${PG_LAT_MEDIAN}"
printf "  FalconDB       %14s %17s\n" "${FALCON_TPS_MEDIAN}" "${FALCON_LAT_MEDIAN}"
echo ""
echo "  TPS ratio (FalconDB / PostgreSQL): ${TPS_RATIO}x"
echo "  Latency ratio (PG / Falcon):       ${LAT_RATIO}x"
echo ""
banner "═══════════════════════════════════════════════"
