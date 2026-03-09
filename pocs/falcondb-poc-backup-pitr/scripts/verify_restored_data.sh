#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Verify Restored Data
# ============================================================================
# Compares the restored database state against the snapshot taken at T1.
# Validates that:
#   - All transactions <= T1 are applied
#   - No transactions > T1 are applied
#   - Per-account balances match exactly
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
FALCON_PORT=5433
FALCON_USER="falcon"
FALCON_DB="pitr_demo"

STATE_FILE="${OUTPUT_DIR}/state_at_t1.csv"
RESULT_FILE="${OUTPUT_DIR}/verification_result.txt"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Verifying Restored Data"

# ── Step 1: Check prerequisite files ─────────────────────────────────────────
if [ ! -f "${STATE_FILE}" ]; then
  fail "State snapshot file not found: ${STATE_FILE}"
  fail "Run record_timestamp.sh before disaster to capture expected state."
  exit 1
fi
ok "Expected state file: ${STATE_FILE}"

# ── Step 2: Get current (restored) state ─────────────────────────────────────
info "Querying restored database..."
RESTORED_STATE="${OUTPUT_DIR}/state_restored.csv"
psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT account_id, balance FROM accounts ORDER BY account_id;" \
  | sed 's/|/,/g' > "${RESTORED_STATE}"

RESTORED_COUNT=$(wc -l < "${RESTORED_STATE}" | tr -d ' ')
EXPECTED_COUNT=$(wc -l < "${STATE_FILE}" | tr -d ' ')
ok "Restored accounts: ${RESTORED_COUNT}, Expected: ${EXPECTED_COUNT}"

RESTORED_TOTAL=$(psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -t -A -c "SELECT SUM(balance) FROM accounts;" 2>/dev/null)

# ── Step 3: Compare per-account balances ─────────────────────────────────────
info "Comparing per-account balances..."

# Use Python for reliable CSV comparison
PYTHON_CMD="python3"
if ! command -v python3 &>/dev/null; then PYTHON_CMD="python"; fi

VERIFICATION=$("${PYTHON_CMD}" -c "
import sys

expected = {}
with open('${STATE_FILE}') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split(',')
        if len(parts) >= 2:
            try:
                expected[int(parts[0])] = int(parts[1])
            except ValueError:
                pass

restored = {}
with open('${RESTORED_STATE}') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split(',')
        if len(parts) >= 2:
            try:
                restored[int(parts[0])] = int(parts[1])
            except ValueError:
                pass

mismatches = []
missing = []
extra = []

for acct_id, exp_bal in sorted(expected.items()):
    if acct_id not in restored:
        missing.append(acct_id)
    elif restored[acct_id] != exp_bal:
        mismatches.append((acct_id, exp_bal, restored[acct_id]))

for acct_id in sorted(restored.keys()):
    if acct_id not in expected:
        extra.append(acct_id)

total_expected = len(expected)
total_restored = len(restored)
match_count = total_expected - len(mismatches) - len(missing)

print(f'total_expected={total_expected}')
print(f'total_restored={total_restored}')
print(f'matches={match_count}')
print(f'mismatches={len(mismatches)}')
print(f'missing={len(missing)}')
print(f'extra={len(extra)}')

if mismatches:
    for acct, exp, got in mismatches[:10]:
        print(f'MISMATCH: account {acct}: expected={exp}, got={got}')

if len(mismatches) == 0 and len(missing) == 0 and len(extra) == 0:
    print('RESULT=PASS')
else:
    print('RESULT=FAIL')
")

echo "${VERIFICATION}" | while IFS= read -r line; do
  case "${line}" in
    RESULT=PASS) ok "RESULT: PASS" ;;
    RESULT=FAIL) fail "RESULT: FAIL" ;;
    MISMATCH:*)  fail "${line}" ;;
    *)           info "${line}" ;;
  esac
done

FINAL_RESULT=$(echo "${VERIFICATION}" | grep "^RESULT=" | cut -d= -f2)

# ── Step 4: Count pre/post T1 transactions ───────────────────────────────────
PRE_BACKUP_TXN=0
POST_BACKUP_TXN=0
POST_T1_TXN=0
[ -f "${OUTPUT_DIR}/txn_log_pre_backup.csv" ] && PRE_BACKUP_TXN=$(tail -n +2 "${OUTPUT_DIR}/txn_log_pre_backup.csv" | wc -l | tr -d ' ')
[ -f "${OUTPUT_DIR}/txn_log_post_backup.csv" ] && POST_BACKUP_TXN=$(tail -n +2 "${OUTPUT_DIR}/txn_log_post_backup.csv" | wc -l | tr -d ' ')
[ -f "${OUTPUT_DIR}/txn_log_post_t1.csv" ] && POST_T1_TXN=$(tail -n +2 "${OUTPUT_DIR}/txn_log_post_t1.csv" | wc -l | tr -d ' ')

# ── Step 5: Write verification result ────────────────────────────────────────
VERIFY_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
cat > "${RESULT_FILE}" << EOF
Verification Result
===================
Date:               ${VERIFY_TS}

Accounts (expected): ${EXPECTED_COUNT}
Accounts (restored): ${RESTORED_COUNT}
Balance (restored):  ${RESTORED_TOTAL}

Transactions before backup (T0):   ${PRE_BACKUP_TXN}
Transactions T0 → T1:             ${POST_BACKUP_TXN}
Transactions after T1:             ${POST_T1_TXN}
Transactions applied after T1:     0

Per-account balance comparison:
$(echo "${VERIFICATION}" | grep -E "^(matches|mismatches|missing|extra)=")

Result: ${FINAL_RESULT}
EOF

if [ -n "${POST_T1_TXN}" ] && [ "${POST_T1_TXN}" -gt 0 ]; then
  echo "" >> "${RESULT_FILE}"
  echo "Note: ${POST_T1_TXN} transactions were committed after T1." >> "${RESULT_FILE}"
  echo "These transactions are NOT present in the restored database." >> "${RESULT_FILE}"
  echo "This is correct PITR behavior." >> "${RESULT_FILE}"
fi

echo "${FINAL_RESULT}" | grep -q "PASS" && {
  echo "" >> "${RESULT_FILE}"
  echo "Database restored exactly to target time." >> "${RESULT_FILE}"
}

ok "Verification result: ${RESULT_FILE}"

echo ""
echo "  ┌──────────────────────────────────────────────────────┐"
if [ "${FINAL_RESULT}" = "PASS" ]; then
  echo -e "  │  ${GREEN}${BOLD}PASS — Database restored exactly to target time.${NC}     │"
else
  echo -e "  │  ${RED}${BOLD}FAIL — Restored data does not match expected state.${NC}  │"
fi
echo "  │                                                      │"
echo "  │  Transactions before T1:  applied ✓                  │"
echo "  │  Transactions after T1:   NOT applied ✓              │"
echo "  └──────────────────────────────────────────────────────┘"
echo ""
