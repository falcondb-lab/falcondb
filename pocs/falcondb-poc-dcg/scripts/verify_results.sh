#!/usr/bin/env bash
# ============================================================================
# FalconDB DCG PoC — Verify Results
# ============================================================================
# Connects to the surviving node (replica, now promoted) and checks that
# every order_id logged in committed_orders.log exists in the database.
#
# This is the MOST IMPORTANT script in the PoC.
#
# Exit code:
#   0 = PASS (zero data loss, zero phantom commits)
#   1 = FAIL (data loss or phantom commits detected)
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

COMMITTED_FILE="${OUTPUT_DIR}/committed_orders.log"
REPORT_FILE="${OUTPUT_DIR}/verification_report.txt"

if [ ! -f "${COMMITTED_FILE}" ]; then
  fail "committed_orders.log not found. Run the workload first."
  exit 1
fi

# ── Get committed order IDs from log ──────────────────────────────────────
COMMITTED_IDS=$(grep -E '^\d+$' "${COMMITTED_FILE}" | sort -n)
COMMITTED_COUNT=$(echo "${COMMITTED_IDS}" | wc -l | tr -d ' ')

if [ "${COMMITTED_COUNT}" -eq 0 ]; then
  fail "No committed order IDs found in log."
  exit 1
fi

info "Committed orders (from client log): ${COMMITTED_COUNT}"

# ── Get surviving order IDs from database ─────────────────────────────────
SURVIVING_FILE="${OUTPUT_DIR}/surviving_orders.txt"

psql -h "${HOST}" -p "${SURVIVOR_PORT}" -U "${USER}" -d "${DB}" \
  -t -A -c "SELECT order_id FROM orders ORDER BY order_id;" \
  > "${SURVIVING_FILE}" 2>/dev/null

SURVIVING_COUNT=$(wc -l < "${SURVIVING_FILE}" | tr -d ' ')
info "Orders on survivor (from database): ${SURVIVING_COUNT}"

# ── Check 1: Missing orders (committed but lost) ─────────────────────────
MISSING_FILE="${OUTPUT_DIR}/missing_orders.txt"
> "${MISSING_FILE}"
MISSING_COUNT=0

while IFS= read -r order_id; do
  if ! grep -q "^${order_id}$" "${SURVIVING_FILE}"; then
    echo "${order_id}" >> "${MISSING_FILE}"
    MISSING_COUNT=$((MISSING_COUNT + 1))
  fi
done <<< "${COMMITTED_IDS}"

# ── Check 2: Phantom orders (in DB but never committed by client) ─────────
PHANTOM_FILE="${OUTPUT_DIR}/phantom_orders.txt"
> "${PHANTOM_FILE}"
PHANTOM_COUNT=0

while IFS= read -r order_id; do
  if ! echo "${COMMITTED_IDS}" | grep -q "^${order_id}$"; then
    echo "${order_id}" >> "${PHANTOM_FILE}"
    PHANTOM_COUNT=$((PHANTOM_COUNT + 1))
  fi
done < "${SURVIVING_FILE}"

# ── Check 3: Duplicates ──────────────────────────────────────────────────
DUPLICATE_COUNT=$(sort "${SURVIVING_FILE}" | uniq -d | wc -l | tr -d ' ')

# ── Build report ─────────────────────────────────────────────────────────
if [ "${MISSING_COUNT}" -eq 0 ] && [ "${PHANTOM_COUNT}" -eq 0 ] && [ "${DUPLICATE_COUNT}" -eq 0 ]; then
  VERDICT="PASS"
else
  VERDICT="FAIL"
fi

{
  echo "============================================================"
  echo "  FalconDB DCG PoC — Verification Report"
  echo "============================================================"
  echo ""
  echo "  Timestamp:       $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "  Survivor node:   ${HOST}:${SURVIVOR_PORT}"
  echo ""
  echo "  Committed orders (acknowledged): ${COMMITTED_COUNT}"
  echo "  Orders after failover:           ${SURVIVING_COUNT}"
  echo "  Missing (data loss):             ${MISSING_COUNT}"
  echo "  Phantom (unexpected):            ${PHANTOM_COUNT}"
  echo "  Duplicates:                      ${DUPLICATE_COUNT}"
  echo ""
  echo "  Result: ${VERDICT}"
  if [ "${VERDICT}" = "PASS" ]; then
    echo "  No committed data was lost."
  else
    [ "${MISSING_COUNT}" -gt 0 ] && echo "  DATA LOSS: ${MISSING_COUNT} committed orders are missing."
    [ "${PHANTOM_COUNT}" -gt 0 ] && echo "  PHANTOM: ${PHANTOM_COUNT} uncommitted orders appeared."
    [ "${DUPLICATE_COUNT}" -gt 0 ] && echo "  DUPLICATES: ${DUPLICATE_COUNT} duplicate order_ids found."
  fi
  echo ""
  echo "============================================================"
} | tee "${REPORT_FILE}"

# Also write machine-readable evidence
cat > "${OUTPUT_DIR}/verification_evidence.json" <<EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "verdict": "${VERDICT}",
  "committed_by_client": ${COMMITTED_COUNT},
  "surviving_after_failover": ${SURVIVING_COUNT},
  "missing_data_loss": ${MISSING_COUNT},
  "phantom_uncommitted": ${PHANTOM_COUNT},
  "duplicates": ${DUPLICATE_COUNT},
  "survivor_node": "${HOST}:${SURVIVOR_PORT}",
  "claim": "If FalconDB returns COMMIT OK, the data survives primary crash + failover."
}
EOF

ok "Report:   ${REPORT_FILE}"
ok "Evidence: ${OUTPUT_DIR}/verification_evidence.json"

if [ "${VERDICT}" = "PASS" ]; then
  exit 0
else
  exit 1
fi
