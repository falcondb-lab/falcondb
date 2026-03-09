#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Monitor Downtime
# ============================================================================
# Reads timestamps from the committed_markers.log and kill/promote timestamps
# to calculate the failover downtime window.
#
# Output: output/failover_timeline.json
#
# This is informational — downtime is NOT a pass/fail metric.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Failover Timeline"

COMMITTED_FILE="${OUTPUT_DIR}/committed_markers.log"
KILL_TS_FILE="${OUTPUT_DIR}/kill_timestamp.txt"
PROMOTE_TS_FILE="${OUTPUT_DIR}/promote_timestamp.txt"
METRICS_FILE="${OUTPUT_DIR}/load_metrics.json"
TIMELINE_FILE="${OUTPUT_DIR}/failover_timeline.json"

# Read kill timestamp
KILL_TS="unknown"
if [ -f "${KILL_TS_FILE}" ]; then
  KILL_TS=$(cat "${KILL_TS_FILE}" | tr -d '[:space:]')
fi
info "Primary killed at: ${KILL_TS}"

# Read promote timestamp
PROMOTE_TS="unknown"
if [ -f "${PROMOTE_TS_FILE}" ]; then
  PROMOTE_TS=$(cat "${PROMOTE_TS_FILE}" | tr -d '[:space:]')
fi
info "Replica promoted at: ${PROMOTE_TS}"

# Parse committed markers for timestamps
TOTAL_MARKERS=$(wc -l < "${COMMITTED_FILE}" 2>/dev/null | tr -d ' ')
info "Total committed markers: ${TOTAL_MARKERS}"

# Find last commit before failure and first commit after reconnect from load_metrics
LAST_BEFORE="unknown"
FIRST_AFTER="unknown"
RECONNECTS=0

if [ -f "${METRICS_FILE}" ]; then
  LAST_BEFORE=$(grep -oP '"last_commit_before_failure_ts":\s*"\K[^"]+' "${METRICS_FILE}" 2>/dev/null || echo "unknown")
  FIRST_AFTER=$(grep -oP '"first_commit_after_reconnect_ts":\s*"\K[^"]+' "${METRICS_FILE}" 2>/dev/null || echo "unknown")
  RECONNECTS=$(grep -oP '"reconnect_count":\s*\K[0-9]+' "${METRICS_FILE}" 2>/dev/null || echo "0")
fi

info "Last commit before failure: ${LAST_BEFORE}"
info "First commit after reconnect: ${FIRST_AFTER}"
info "Reconnect attempts: ${RECONNECTS}"

# Pre-kill marker count
PRE_KILL_COUNT="unknown"
if [ -f "${OUTPUT_DIR}/pre_kill_marker_count.txt" ]; then
  PRE_KILL_COUNT=$(cat "${OUTPUT_DIR}/pre_kill_marker_count.txt" | tr -d '[:space:]')
fi

# Calculate downtime estimate (best effort with date arithmetic)
DOWNTIME_SEC="N/A"
if [ "${KILL_TS}" != "unknown" ] && [ "${PROMOTE_TS}" != "unknown" ]; then
  if command -v python3 &>/dev/null; then
    DOWNTIME_SEC=$(python3 -c "
from datetime import datetime
try:
    fmt1 = '%Y-%m-%dT%H:%M:%S.%fZ'
    fmt2 = '%Y-%m-%dT%H:%M:%SZ'
    for fmt in [fmt1, fmt2]:
        try:
            t1 = datetime.strptime('${KILL_TS}', fmt)
            break
        except: pass
    for fmt in [fmt1, fmt2]:
        try:
            t2 = datetime.strptime('${PROMOTE_TS}', fmt)
            break
        except: pass
    print(round((t2 - t1).total_seconds(), 1))
except:
    print('N/A')
" 2>/dev/null || echo "N/A")
  fi
fi

info "Estimated downtime (kill → promote ready): ${DOWNTIME_SEC}s"

# Write timeline JSON
cat > "${TIMELINE_FILE}" <<EOF
{
  "kill_timestamp": "${KILL_TS}",
  "promote_timestamp": "${PROMOTE_TS}",
  "last_commit_before_failure": "${LAST_BEFORE}",
  "first_commit_after_reconnect": "${FIRST_AFTER}",
  "markers_before_kill": ${PRE_KILL_COUNT:-0},
  "markers_total": ${TOTAL_MARKERS},
  "reconnect_count": ${RECONNECTS},
  "estimated_downtime_sec": "${DOWNTIME_SEC}",
  "note": "Downtime is informational, not a pass/fail metric."
}
EOF

ok "Timeline: ${TIMELINE_FILE}"
