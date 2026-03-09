#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Generate Transactional Data
# ============================================================================
# Writes deterministic transactions to the accounts table.
# Each transaction: BEGIN → UPDATE one account → COMMIT
# Logs every committed transaction externally for verification.
#
# Usage:
#   ./scripts/generate_data.sh                  # 500 transactions
#   ./scripts/generate_data.sh --count 2000     # custom count
#   ./scripts/generate_data.sh --phase post     # label for post-backup phase
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
FALCON_PORT=5433
FALCON_USER="falcon"
FALCON_DB="pitr_demo"

TX_COUNT=500
PHASE="default"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --count) TX_COUNT="$2"; shift 2 ;;
    --phase) PHASE="$2"; shift 2 ;;
    *) shift ;;
  esac
done

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

TX_LOG="${OUTPUT_DIR}/txn_log_${PHASE}.csv"

banner "Generating Data: ${TX_COUNT} transactions (phase: ${PHASE})"

# Write CSV header
echo "seq,account_id,delta,balance_after,commit_ts" > "${TX_LOG}"

COMMITTED=0
ERRORS=0

for i in $(seq 1 "${TX_COUNT}"); do
  # Deterministic: account = (i % 100) + 1, delta = i (always positive for simplicity)
  ACCT_ID=$(( (i % 100) + 1 ))
  DELTA=$(( (i % 50) + 1 ))

  RESULT=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
    -t -A -c "
    BEGIN;
    UPDATE accounts SET balance = balance + ${DELTA}, updated_at = NOW() WHERE account_id = ${ACCT_ID};
    SELECT balance, NOW() FROM accounts WHERE account_id = ${ACCT_ID};
    COMMIT;
  " 2>/dev/null || echo "ERROR")

  if [ "${RESULT}" = "ERROR" ] || [ -z "${RESULT}" ]; then
    ERRORS=$((ERRORS + 1))
  else
    # Parse balance|timestamp from result
    BALANCE=$(echo "${RESULT}" | head -1 | cut -d'|' -f1 | tr -d '[:space:]')
    TS=$(echo "${RESULT}" | head -1 | cut -d'|' -f2 | tr -d '[:space:]')
    echo "${i},${ACCT_ID},${DELTA},${BALANCE},${TS}" >> "${TX_LOG}"
    COMMITTED=$((COMMITTED + 1))
  fi

  # Progress every 100
  if [ $((i % 100)) -eq 0 ]; then
    info "Progress: ${i}/${TX_COUNT} (committed: ${COMMITTED}, errors: ${ERRORS})"
  fi
done

ok "Phase '${PHASE}' complete: ${COMMITTED} committed, ${ERRORS} errors"
ok "Transaction log: ${TX_LOG}"

# Also record aggregate balance for quick verification
TOTAL=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT SUM(balance) FROM accounts;" 2>/dev/null)
echo ""
ok "Total balance across all accounts: ${TOTAL}"
echo "${PHASE},${COMMITTED},${TOTAL}" >> "${OUTPUT_DIR}/balance_checkpoints.csv"
echo ""
