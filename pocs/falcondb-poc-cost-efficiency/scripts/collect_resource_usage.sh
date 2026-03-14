#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #6 — Cost Efficiency: Collect Resource Usage
# ============================================================================
# Periodically samples CPU, memory, and disk IO for a given process.
# Works on Linux (pidstat / /proc) and macOS (ps).
#
# Usage:
#   ./collect_resource_usage.sh --pid 12345 --label falcon --duration 60
#   ./collect_resource_usage.sh --pid 12345 --label postgres --duration 60
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

# Parse args
PID=""
LABEL="unknown"
DURATION=60
INTERVAL=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pid)      PID="$2"; shift 2 ;;
    --label)    LABEL="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    *) shift ;;
  esac
done

if [ -z "${PID}" ]; then
  echo "Usage: $0 --pid <PID> --label <name> [--duration 60] [--interval 2]"
  exit 1
fi

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

mkdir -p "${OUTPUT_DIR}"
OUTFILE="${OUTPUT_DIR}/resource_usage_${LABEL}.json"

info "Collecting resource usage for PID ${PID} (${LABEL}) every ${INTERVAL}s for ${DURATION}s"

SAMPLES="["
FIRST=true
ELAPSED=0

while [ "${ELAPSED}" -lt "${DURATION}" ]; do
  if ! kill -0 "${PID}" 2>/dev/null; then
    info "Process ${PID} no longer running. Stopping."
    break
  fi

  TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # CPU and memory via /proc (Linux) or ps (macOS/fallback)
  if [ -f "/proc/${PID}/stat" ]; then
    # Linux: use /proc
    RSS_KB=$(awk '{print $24}' "/proc/${PID}/stat" 2>/dev/null || echo "0")
    RSS_KB=$((RSS_KB * 4 / 1024))  # pages to KB (assuming 4K pages)
    CPU_PCT=$(ps -p "${PID}" -o %cpu= 2>/dev/null | tr -d ' ' || echo "0")
    MEM_PCT=$(ps -p "${PID}" -o %mem= 2>/dev/null | tr -d ' ' || echo "0")

    # Disk IO from /proc/pid/io
    if [ -r "/proc/${PID}/io" ]; then
      READ_BYTES=$(grep "^read_bytes:" "/proc/${PID}/io" 2>/dev/null | awk '{print $2}' || echo "0")
      WRITE_BYTES=$(grep "^write_bytes:" "/proc/${PID}/io" 2>/dev/null | awk '{print $2}' || echo "0")
    else
      READ_BYTES=0
      WRITE_BYTES=0
    fi
  else
    # macOS / fallback
    CPU_PCT=$(ps -p "${PID}" -o %cpu= 2>/dev/null | tr -d ' ' || echo "0")
    MEM_PCT=$(ps -p "${PID}" -o %mem= 2>/dev/null | tr -d ' ' || echo "0")
    RSS_KB=$(ps -p "${PID}" -o rss= 2>/dev/null | tr -d ' ' || echo "0")
    READ_BYTES=0
    WRITE_BYTES=0
  fi

  # Build JSON sample
  SAMPLE="{\"ts\":\"${TS}\",\"elapsed_s\":${ELAPSED},\"cpu_pct\":${CPU_PCT:-0},\"mem_pct\":${MEM_PCT:-0},\"rss_kb\":${RSS_KB:-0},\"disk_read_bytes\":${READ_BYTES:-0},\"disk_write_bytes\":${WRITE_BYTES:-0}}"

  if [ "${FIRST}" = true ]; then
    SAMPLES="${SAMPLES}${SAMPLE}"
    FIRST=false
  else
    SAMPLES="${SAMPLES},${SAMPLE}"
  fi

  sleep "${INTERVAL}"
  ELAPSED=$((ELAPSED + INTERVAL))
done

# Finalize JSON
TOTAL_SAMPLES=$(echo "${SAMPLES}" | tr ',' '\n' | grep -c "ts" || echo "0")

cat > "${OUTFILE}" << EOF
{
  "label": "${LABEL}",
  "pid": ${PID},
  "duration_sec": ${DURATION},
  "interval_sec": ${INTERVAL},
  "total_samples": ${TOTAL_SAMPLES},
  "samples": ${SAMPLES}]
}
EOF

ok "Resource data written to ${OUTFILE} (${TOTAL_SAMPLES} samples)"
