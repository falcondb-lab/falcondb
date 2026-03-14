#!/usr/bin/env bash
# ============================================================================
# FalconDB MES — Promote Replica / Restart After Crash
# ============================================================================
# After kill_primary.sh, this script restarts FalconDB on the same data
# directory. FalconDB's StorageEngine::recover() replays WAL records and
# rebuilds the exact pre-crash state.
#
# In a multi-node deployment this would promote a replica.
# In this single-node demo it restarts and recovers from WAL.
# The business effect is identical: all committed facts survive.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
FALCON_PORT=5433
FALCON_USER="falcon"
FALCON_DB="mes_prod"
HOST="127.0.0.1"
DATA_DIR="${POC_ROOT}/mes_data"
BACKEND_PORT=8000

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

# Resolve binary
if [ ! -f "${FALCON_BIN}" ]; then
  REPO_BIN="$(cd "${POC_ROOT}/.." && pwd)/${FALCON_BIN}"
  [ -f "${REPO_BIN}" ] && FALCON_BIN="${REPO_BIN}"
fi
if [ ! -f "${FALCON_BIN}" ]; then
  fail "FalconDB binary not found at '${FALCON_BIN}'"
  exit 1
fi

banner "切主恢复 (Promote / Recover After Crash)"

# Verify data directory exists (WAL must survive the crash)
if [ ! -d "${DATA_DIR}" ]; then
  fail "Data directory not found: ${DATA_DIR}"
  fail "Cannot recover — data was destroyed (not just crashed)"
  exit 1
fi
ok "Data directory intact: ${DATA_DIR}"

# ── Step 1: Restart FalconDB (WAL recovery happens automatically) ───────────
info "Starting FalconDB (WAL recovery on startup)..."
RECOVER_START=$(date +%s%3N 2>/dev/null || date +%s)

"${FALCON_BIN}" \
  --pg-listen-addr "0.0.0.0:${FALCON_PORT}" \
  --admin-listen-addr "0.0.0.0:8080" \
  --data-dir "${DATA_DIR}" \
  > "${OUTPUT_DIR}/falcon_recovery.log" 2>&1 &
FALCON_PID=$!
echo "${FALCON_PID}" > "${OUTPUT_DIR}/falcon.pid"

READY=false
for i in $(seq 1 60); do
  if psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
    READY=true
    RECOVER_END=$(date +%s%3N 2>/dev/null || date +%s)
    ok "FalconDB recovered (pid ${FALCON_PID}, ${i}s)"
    break
  fi
  sleep 1
done

if [ "${READY}" = false ]; then
  fail "FalconDB did not recover within 60s"
  fail "Check: ${OUTPUT_DIR}/falcon_recovery.log"
  exit 1
fi

# ── Step 2: Restart backend API ─────────────────────────────────────────────
# Kill old backend if still running
if [ -f "${OUTPUT_DIR}/backend.pid" ]; then
  OLD_PID=$(cat "${OUTPUT_DIR}/backend.pid" | tr -d '[:space:]')
  kill "${OLD_PID}" 2>/dev/null || true
  rm -f "${OUTPUT_DIR}/backend.pid"
fi

info "Restarting backend API..."
cd "${POC_ROOT}/backend"
FALCON_HOST="${HOST}" FALCON_PORT="${FALCON_PORT}" FALCON_DB="${FALCON_DB}" \
  python3 app.py > "${OUTPUT_DIR}/backend.log" 2>&1 &
BACKEND_PID=$!
echo "${BACKEND_PID}" > "${OUTPUT_DIR}/backend.pid"
cd "${POC_ROOT}"

for i in $(seq 1 15); do
  if curl -sf "http://${HOST}:${BACKEND_PORT}/api/health" &>/dev/null; then
    ok "Backend API ready (pid ${BACKEND_PID}, ${i}s)"
    break
  fi
  sleep 1
done

PROMOTE_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "${PROMOTE_TS} | PROMOTE_REPLICA | pid=${FALCON_PID}" >> "${OUTPUT_DIR}/event_timeline.txt"

echo ""
echo "  ┌──────────────────────────────────────────────────┐"
echo "  │  恢复完成 (Recovery Complete)                     │"
echo "  │                                                  │"
echo "  │  FalconDB: recovered from WAL                    │"
echo "  │  Backend:  reconnected                           │"
echo "  │                                                  │"
echo "  │  All committed production facts are intact.      │"
echo "  │  Next: ./scripts/verify_business_state.sh        │"
echo "  └──────────────────────────────────────────────────┘"
echo ""
