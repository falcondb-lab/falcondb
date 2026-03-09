#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #6 — Cost Efficiency: Simulate Peak Load
# ============================================================================
# Temporarily increases workload rate (2x) for 30 seconds, then returns
# to normal. Observe latency spikes, CPU saturation, memory pressure.
# The system must recover automatically — no restart allowed.
#
# Usage:
#   ./simulate_peak.sh --target falcon   # hit FalconDB
#   ./simulate_peak.sh --target postgres  # hit PostgreSQL
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"
WORKLOAD_BIN="${POC_ROOT}/workload/target/release/oltp_writer"

TARGET="${1:---target}"
TARGET="${2:-falcon}"

# Parse --target flag
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    *) shift ;;
  esac
done

HOST="127.0.0.1"
THREADS="${WORKLOAD_THREADS:-4}"

if [ "${TARGET}" = "falcon" ]; then
  PORT=5433; USER="falcon"
elif [ "${TARGET}" = "postgres" ]; then
  PORT="${PG_PORT:-5432}"; USER="${PG_USER:-postgres}"
else
  echo "Usage: $0 --target [falcon|postgres]"
  exit 1
fi

NORMAL_RATE="${WORKLOAD_RATE:-1000}"
PEAK_RATE=$((NORMAL_RATE * 2))
PEAK_DURATION=30
NORMAL_DURATION=30

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

mkdir -p "${OUTPUT_DIR}"

if [ ! -f "${WORKLOAD_BIN}" ]; then
  info "Building workload generator..."
  (cd "${POC_ROOT}/workload" && cargo build --release 2>&1)
fi

banner "Peak Simulation: ${TARGET} (${NORMAL_RATE} → ${PEAK_RATE} → ${NORMAL_RATE} tx/s)"

# Phase 1: Normal baseline
info "Phase 1: Normal load (${NORMAL_RATE} tx/s × ${NORMAL_DURATION}s)"
"${WORKLOAD_BIN}" \
  --host "${HOST}" --port "${PORT}" --user "${USER}" --db bench \
  --rate "${NORMAL_RATE}" --duration "${NORMAL_DURATION}" --threads "${THREADS}" \
  --label "${TARGET}_pre_peak" \
  --output "${OUTPUT_DIR}/peak_pre_${TARGET}.json"
ok "Pre-peak baseline complete"

# Phase 2: Peak (2x)
echo ""
info "Phase 2: PEAK load (${PEAK_RATE} tx/s × ${PEAK_DURATION}s)"
echo -e "  ${RED}${BOLD}>>> 2x load spike <<<${NC}"
"${WORKLOAD_BIN}" \
  --host "${HOST}" --port "${PORT}" --user "${USER}" --db bench \
  --rate "${PEAK_RATE}" --duration "${PEAK_DURATION}" --threads "${THREADS}" \
  --label "${TARGET}_peak" \
  --output "${OUTPUT_DIR}/peak_during_${TARGET}.json"
ok "Peak phase complete"

# Phase 3: Recovery (back to normal)
echo ""
info "Phase 3: Recovery — normal load (${NORMAL_RATE} tx/s × ${NORMAL_DURATION}s)"
info "System must recover without restart."
"${WORKLOAD_BIN}" \
  --host "${HOST}" --port "${PORT}" --user "${USER}" --db bench \
  --rate "${NORMAL_RATE}" --duration "${NORMAL_DURATION}" --threads "${THREADS}" \
  --label "${TARGET}_post_peak" \
  --output "${OUTPUT_DIR}/peak_post_${TARGET}.json"
ok "Post-peak recovery complete"

banner "Peak Simulation Results (${TARGET})"
echo "  Pre-peak:  ${OUTPUT_DIR}/peak_pre_${TARGET}.json"
echo "  Peak:      ${OUTPUT_DIR}/peak_during_${TARGET}.json"
echo "  Post-peak: ${OUTPUT_DIR}/peak_post_${TARGET}.json"
echo ""
echo "  Compare p95/p99 latency across phases to measure stability."
echo ""
