#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Record Recovery Timestamp (T1)
# ============================================================================
# Captures a precise recovery target time. This is the exact point
# we will restore to after the disaster.
#
# Also records the database state at this moment for later verification.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
FALCON_PORT=5433
FALCON_USER="falcon"
FALCON_DB="pitr_demo"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

TS_FILE="${OUTPUT_DIR}/recovery_target.txt"
STATE_FILE="${OUTPUT_DIR}/state_at_t1.csv"

banner "Recording Recovery Target (T1)"

# Capture precise timestamp from the database itself (server time)
DB_TS=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT NOW();" 2>/dev/null)
# Also capture wall-clock time
WALL_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

info "Database timestamp: ${DB_TS}"
info "Wall-clock:         ${WALL_TS}"

# Write recovery target file (human + machine readable)
cat > "${TS_FILE}" << EOF
Recovery Target (T1)
====================
Database time: ${DB_TS}
Wall-clock:    ${WALL_TS}
Target type:   timestamp
EOF
ok "Recovery target written: ${TS_FILE}"

# Snapshot the database state at T1 for verification
info "Capturing database state at T1..."
ROW_COUNT=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT COUNT(*) FROM accounts;")
TOTAL_BALANCE=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT SUM(balance) FROM accounts;")

# Dump per-account balances for exact verification
psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT account_id, balance FROM accounts ORDER BY account_id;" \
  | sed 's/|/,/g' > "${STATE_FILE}"

ok "State captured: ${ROW_COUNT} accounts, total balance: ${TOTAL_BALANCE}"
ok "Per-account balances: ${STATE_FILE}"

# Count transactions in the log files
PRE_COUNT=0
POST_COUNT=0
[ -f "${OUTPUT_DIR}/txn_log_pre_backup.csv" ] && PRE_COUNT=$(tail -n +2 "${OUTPUT_DIR}/txn_log_pre_backup.csv" | wc -l | tr -d ' ')
[ -f "${OUTPUT_DIR}/txn_log_post_backup.csv" ] && POST_COUNT=$(tail -n +2 "${OUTPUT_DIR}/txn_log_post_backup.csv" | wc -l | tr -d ' ')

echo ""
echo "  ┌──────────────────────────────────────────┐"
echo "  │  Recovery Target (T1) Recorded            │"
echo "  │  Time:       ${DB_TS}                     │"
echo "  │  Accounts:   ${ROW_COUNT}                 │"
echo "  │  Balance:    ${TOTAL_BALANCE}              │"
echo "  │  Txns (pre): ${PRE_COUNT}                 │"
echo "  │  Txns (post):${POST_COUNT}                │"
echo "  └──────────────────────────────────────────┘"
echo ""
echo "  Everything after this timestamp will be LOST during recovery."
echo "  That is the point of PITR."
echo ""
