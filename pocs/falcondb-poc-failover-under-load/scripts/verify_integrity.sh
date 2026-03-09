#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Verify Integrity
# ============================================================================
# THE MOST IMPORTANT SCRIPT IN THIS POC.
#
# Connects to the surviving node and verifies:
#   1. Every committed marker_id exists in the database
#   2. No duplicates
#   3. No phantom markers (in DB but never committed by client)
#
# Exit code: 0 = PASS, 1 = FAIL
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
SURVIVOR_PORT="${SURVIVOR_PORT:-5434}"
DB="falcon"
USER="falcon"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

COMMITTED_FILE="${OUTPUT_DIR}/committed_markers.log"
REPORT_FILE="${OUTPUT_DIR}/verification_report.txt"

if [ ! -f "${COMMITTED_FILE}" ]; then
  fail "committed_markers.log not found. Run the workload first."
  exit 1
fi

# Extract marker_ids from log (format: marker_id|timestamp)
COMMITTED_IDS=$(grep -oP '^\d+' "${COMMITTED_FILE}" | sort -n)
COMMITTED_COUNT=$(echo "${COMMITTED_IDS}" | grep -c '.' || echo "0")

if [ "${COMMITTED_COUNT}" -eq 0 ]; then
  fail "No committed marker IDs found in log."
  exit 1
fi

info "Committed markers (from client log): ${COMMITTED_COUNT}"

# Get surviving marker_ids from database
SURVIVING_FILE="${OUTPUT_DIR}/surviving_markers.txt"

psql -h "${HOST}" -p "${SURVIVOR_PORT}" -U "${USER}" -d "${DB}" \
  -t -A -c "SELECT marker_id FROM tx_markers ORDER BY marker_id;" \
  > "${SURVIVING_FILE}" 2>/dev/null

SURVIVING_COUNT=$(wc -l < "${SURVIVING_FILE}" | tr -d ' ')
info "Markers on survivor (from database): ${SURVIVING_COUNT}"

# Check 1: Missing markers (committed but lost)
MISSING_FILE="${OUTPUT_DIR}/missing_markers.txt"
> "${MISSING_FILE}"
MISSING_COUNT=0

while IFS= read -r marker_id; do
  if ! grep -q "^${marker_id}$" "${SURVIVING_FILE}"; then
    echo "${marker_id}" >> "${MISSING_FILE}"
    MISSING_COUNT=$((MISSING_COUNT + 1))
  fi
done <<< "${COMMITTED_IDS}"

# Check 2: Phantom markers (in DB but never committed by client)
PHANTOM_FILE="${OUTPUT_DIR}/phantom_markers.txt"
> "${PHANTOM_FILE}"
PHANTOM_COUNT=0

while IFS= read -r marker_id; do
  if ! echo "${COMMITTED_IDS}" | grep -q "^${marker_id}$"; then
    echo "${marker_id}" >> "${PHANTOM_FILE}"
    PHANTOM_COUNT=$((PHANTOM_COUNT + 1))
  fi
done < "${SURVIVING_FILE}"

# Check 3: Duplicates
DUPLICATE_COUNT=$(sort "${SURVIVING_FILE}" | uniq -d | wc -l | tr -d ' ')

# Read downtime info
DOWNTIME="N/A"
if [ -f "${OUTPUT_DIR}/failover_timeline.json" ]; then
  DOWNTIME=$(grep -oP '"estimated_downtime_sec":\s*"\K[^"]+' "${OUTPUT_DIR}/failover_timeline.json" 2>/dev/null || echo "N/A")
fi

# Verdict
if [ "${MISSING_COUNT}" -eq 0 ] && [ "${PHANTOM_COUNT}" -eq 0 ] && [ "${DUPLICATE_COUNT}" -eq 0 ]; then
  VERDICT="PASS"
else
  VERDICT="FAIL"
fi

# Build report
{
  echo "============================================================"
  echo "  FalconDB PoC #3 — Failover Under Load: Verification Report"
  echo "============================================================"
  echo ""
  echo "  Timestamp:       $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "  Survivor node:   ${HOST}:${SURVIVOR_PORT}"
  echo ""
  echo "  Total committed markers (acknowledged): ${COMMITTED_COUNT}"
  echo "  Markers after failover:                 ${SURVIVING_COUNT}"
  echo ""
  echo "  Missing (data loss):             ${MISSING_COUNT}"
  echo "  Phantom (unexpected):            ${PHANTOM_COUNT}"
  echo "  Duplicates:                      ${DUPLICATE_COUNT}"
  echo ""
  echo "  Downtime window:                 ${DOWNTIME}s"
  echo ""
  echo "  Result: ${VERDICT}"
  if [ "${VERDICT}" = "PASS" ]; then
    echo "  All acknowledged commits survived the failover."
    echo "  No data loss. No phantom commits. No duplicates."
  else
    [ "${MISSING_COUNT}" -gt 0 ] && echo "  DATA LOSS: ${MISSING_COUNT} committed markers are missing."
    [ "${PHANTOM_COUNT}" -gt 0 ] && echo "  PHANTOM: ${PHANTOM_COUNT} uncommitted markers appeared."
    [ "${DUPLICATE_COUNT}" -gt 0 ] && echo "  DUPLICATES: ${DUPLICATE_COUNT} duplicate marker_ids found."
  fi
  echo ""
  echo "============================================================"
} | tee "${REPORT_FILE}"

# Machine-readable evidence
cat > "${OUTPUT_DIR}/verification_evidence.json" <<EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "verdict": "${VERDICT}",
  "committed_by_client": ${COMMITTED_COUNT},
  "surviving_after_failover": ${SURVIVING_COUNT},
  "missing_data_loss": ${MISSING_COUNT},
  "phantom_uncommitted": ${PHANTOM_COUNT},
  "duplicates": ${DUPLICATE_COUNT},
  "estimated_downtime_sec": "${DOWNTIME}",
  "survivor_node": "${HOST}:${SURVIVOR_PORT}",
  "claim": "FalconDB remains correct and predictable even when the worst thing happens at the worst time."
}
EOF

ok "Report:   ${REPORT_FILE}"
ok "Evidence: ${OUTPUT_DIR}/verification_evidence.json"

if [ "${VERDICT}" = "PASS" ]; then
  exit 0
else
  exit 1
fi
