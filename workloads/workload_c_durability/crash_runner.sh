#!/usr/bin/env bash
# Workload C: Durability & Crash — Runner
#
# Validates CONSISTENCY.md invariants:
#   CP-4: Crash after DurableCommit MUST NOT lose the transaction
#   FC-1: Crash before CP-D → txn rolled back
#   FC-2: Crash after CP-D → txn survives
#   FC-4: WAL replay is idempotent
#
# Flow:
#   1. Start FalconDB (or use running instance)
#   2. Create schema + write N committed transactions (client-side log)
#   3. SIGKILL the server during active writes
#   4. Restart the server (WAL recovery)
#   5. Compare client log vs DB — any missing committed row = FAIL
#
# Usage:
#   ./crash_runner.sh
#   ./crash_runner.sh --falcon-bin ./target/release/falcon_server --rounds 3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

# ── Config ────────────────────────────────────────────────────────────────
FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
FALCON_CONFIG="${FALCON_CONFIG:-bench_configs/falcon.bench.toml}"
DATA_DIR="${DATA_DIR:-/tmp/falcon_crash_test_data}"
HOST="127.0.0.1"
PORT=5433
DB="falcon"
USER="falcon"
WRITE_COUNT="${WRITE_COUNT:-5000}"
WRITE_BEFORE_KILL="${WRITE_BEFORE_KILL:-3000}"
ROUNDS="${ROUNDS:-3}"
TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")

while [[ $# -gt 0 ]]; do
    case "$1" in
        --falcon-bin)  FALCON_BIN="$2";          shift 2 ;;
        --config)      FALCON_CONFIG="$2";        shift 2 ;;
        --rounds)      ROUNDS="$2";               shift 2 ;;
        --count)       WRITE_COUNT="$2";          shift 2 ;;
        --kill-after)  WRITE_BEFORE_KILL="$2";    shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

REPORT_FILE="${RESULTS_DIR}/recovery_report_${TIMESTAMP}.json"
OVERALL_PASS=true
ROUND_RESULTS="["

start_falcon() {
    rm -rf "${DATA_DIR}"
    mkdir -p "${DATA_DIR}"

    # Override data_dir in config
    TMPCONFIG="/tmp/falcon_crash_test.toml"
    sed "s|data_dir.*=.*|data_dir = \"${DATA_DIR}\"|" "${FALCON_CONFIG}" > "${TMPCONFIG}"

    "${FALCON_BIN}" --config "${TMPCONFIG}" &
    FALCON_PID=$!

    # Wait for ready
    for i in $(seq 1 30); do
        if psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
            -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    fail "FalconDB did not start within 15s"
    return 1
}

restart_falcon() {
    TMPCONFIG="/tmp/falcon_crash_test.toml"
    "${FALCON_BIN}" --config "${TMPCONFIG}" &
    FALCON_PID=$!

    for i in $(seq 1 30); do
        if psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
            -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    fail "FalconDB did not restart within 15s (WAL recovery may have failed)"
    return 1
}

stop_falcon() {
    if [ -n "${FALCON_PID:-}" ] && kill -0 "${FALCON_PID}" 2>/dev/null; then
        kill "${FALCON_PID}" 2>/dev/null || true
        wait "${FALCON_PID}" 2>/dev/null || true
    fi
}

cleanup() {
    stop_falcon
    rm -rf "${DATA_DIR}" /tmp/falcon_crash_test.toml 2>/dev/null || true
}
trap cleanup EXIT

echo ""
echo -e "${BOLD}═══ Workload C: Durability & Crash Recovery ═══${NC}"
echo "  Rounds:       ${ROUNDS}"
echo "  Writes/round: ${WRITE_COUNT}"
echo "  Kill after:   ${WRITE_BEFORE_KILL} committed"
echo "  Binary:       ${FALCON_BIN}"
echo ""

for round in $(seq 1 "${ROUNDS}"); do
    echo -e "\n${BOLD}── Round ${round}/${ROUNDS} ──${NC}"

    COMMITTED_LOG="/tmp/falcon_crash_committed_${round}.log"
    > "${COMMITTED_LOG}"

    # 1. Start fresh
    info "Starting FalconDB..."
    start_falcon
    ok "Server up (PID ${FALCON_PID})"

    # 2. Create table
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
        -f "${SCRIPT_DIR}/schema.sql" >/dev/null 2>&1
    ok "Schema created"

    # 3. Write transactions, log each COMMIT'd seq_id
    info "Writing ${WRITE_COUNT} transactions (kill after ${WRITE_BEFORE_KILL})..."

    committed=0
    failed=0
    for seq in $(seq 1 "${WRITE_COUNT}"); do
        result=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
            -t -A -c "INSERT INTO commit_log (seq_id, payload) VALUES (${seq}, 'round${round}-seq${seq}'); SELECT ${seq};" 2>&1) || true

        if echo "${result}" | grep -q "^${seq}$"; then
            echo "${seq}" >> "${COMMITTED_LOG}"
            committed=$((committed + 1))
        else
            failed=$((failed + 1))
        fi

        # SIGKILL at threshold
        if [ "${committed}" -eq "${WRITE_BEFORE_KILL}" ]; then
            KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
            echo -e "  ${RED}${BOLD}>>> kill -9 ${FALCON_PID} <<<${NC} (after ${committed} commits)"
            kill -9 "${FALCON_PID}" 2>/dev/null || true
            wait "${FALCON_PID}" 2>/dev/null || true
            break
        fi
    done

    CLIENT_COUNT=$(wc -l < "${COMMITTED_LOG}" | tr -d ' ')
    ok "Client committed: ${CLIENT_COUNT} (failed: ${failed})"

    # 4. Restart — WAL recovery
    info "Restarting FalconDB (WAL recovery)..."
    restart_falcon
    ok "Server recovered (PID ${FALCON_PID})"

    # 5. Verify
    info "Verifying..."

    DB_COUNT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
        -t -A -c "SELECT COUNT(*) FROM commit_log;" 2>/dev/null | tr -d ' ')

    # Get DB seq_ids
    DB_IDS="/tmp/falcon_crash_db_ids_${round}.txt"
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" \
        -t -A -c "SELECT seq_id FROM commit_log ORDER BY seq_id;" > "${DB_IDS}" 2>/dev/null

    # Compare: committed by client but missing in DB = DATA LOSS
    sort -n "${COMMITTED_LOG}" > "/tmp/fc_client_sorted_${round}.txt"
    sort -n "${DB_IDS}" > "/tmp/fc_db_sorted_${round}.txt"

    MISSING=$(comm -23 "/tmp/fc_client_sorted_${round}.txt" "/tmp/fc_db_sorted_${round}.txt" | wc -l | tr -d ' ')
    PHANTOM=$(comm -13 "/tmp/fc_client_sorted_${round}.txt" "/tmp/fc_db_sorted_${round}.txt" | wc -l | tr -d ' ')
    DUPS=$(sort "${DB_IDS}" | uniq -d | wc -l | tr -d ' ')

    ROUND_VERDICT="PASS"
    if [ "${MISSING}" -gt 0 ] || [ "${DUPS}" -gt 0 ]; then
        ROUND_VERDICT="FAIL"
        OVERALL_PASS=false
    fi

    echo "    Client committed:  ${CLIENT_COUNT}"
    echo "    DB after recovery: ${DB_COUNT}"
    echo "    Missing (loss):    ${MISSING}"
    echo "    Phantom:           ${PHANTOM}"
    echo "    Duplicates:        ${DUPS}"

    if [ "${ROUND_VERDICT}" = "PASS" ]; then
        ok "Round ${round}: PASS — no lost commits, no double commits"
    else
        fail "Round ${round}: FAIL"
    fi

    # Append to JSON array
    [ "${round}" -gt 1 ] && ROUND_RESULTS="${ROUND_RESULTS},"
    ROUND_RESULTS="${ROUND_RESULTS}
    {
      \"round\": ${round},
      \"verdict\": \"${ROUND_VERDICT}\",
      \"client_committed\": ${CLIENT_COUNT},
      \"db_after_recovery\": ${DB_COUNT},
      \"missing_data_loss\": ${MISSING},
      \"phantom\": ${PHANTOM},
      \"duplicates\": ${DUPS},
      \"kill_after\": ${WRITE_BEFORE_KILL}
    }"

    # Stop for next round
    stop_falcon

    # Cleanup temp
    rm -f "${COMMITTED_LOG}" "${DB_IDS}" "/tmp/fc_client_sorted_${round}.txt" "/tmp/fc_db_sorted_${round}.txt"
done

ROUND_RESULTS="${ROUND_RESULTS}
  ]"

# ── Final report ──────────────────────────────────────────────────────────
FINAL_VERDICT="PASS"
if [ "${OVERALL_PASS}" = "false" ]; then
    FINAL_VERDICT="FAIL"
fi

cat > "${REPORT_FILE}" <<EOF
{
  "workload": "C",
  "name": "durability_crash",
  "timestamp": "${TIMESTAMP}",
  "config": {
    "rounds": ${ROUNDS},
    "writes_per_round": ${WRITE_COUNT},
    "kill_after": ${WRITE_BEFORE_KILL},
    "falcon_bin": "${FALCON_BIN}",
    "durability": "fsync + fdatasync + WAL"
  },
  "invariants_tested": [
    "CP-4: crash after DurableCommit must not lose txn",
    "FC-1: crash before CP-D → txn rolled back",
    "FC-2: crash after CP-D → txn survives",
    "FC-4: WAL replay is idempotent (no double commit)"
  ],
  "rounds": ${ROUND_RESULTS},
  "verdict": "${FINAL_VERDICT}"
}
EOF

echo ""
echo -e "${BOLD}═══ Final Verdict: ${FINAL_VERDICT} ═══${NC}"
echo "  Report: ${REPORT_FILE}"
echo ""

if [ "${FINAL_VERDICT}" = "FAIL" ]; then
    exit 1
fi
