#!/usr/bin/env bash
# ============================================================================
# FalconDB MES — Start Cluster + Apply Schema + Launch Backend
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
  echo "  Build it: cargo build -p falcon_server --release"
  exit 1
fi
ok "Binary: ${FALCON_BIN}"

mkdir -p "${OUTPUT_DIR}"
rm -rf "${DATA_DIR}"
rm -f "${OUTPUT_DIR}/falcon.pid" "${OUTPUT_DIR}/backend.pid"

banner "启动 FalconDB MES 系统 (Starting FalconDB MES System)"

# ── Step 1: Start FalconDB ──────────────────────────────────────────────────
info "Starting FalconDB..."
"${FALCON_BIN}" \
  --pg-listen-addr "0.0.0.0:${FALCON_PORT}" \
  --admin-listen-addr "0.0.0.0:8080" \
  --data-dir "${DATA_DIR}" \
  > "${OUTPUT_DIR}/falcon.log" 2>&1 &
FALCON_PID=$!
echo "${FALCON_PID}" > "${OUTPUT_DIR}/falcon.pid"

for i in $(seq 1 30); do
  if psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
    ok "FalconDB ready (pid ${FALCON_PID}, ${i}s)"
    break
  fi
  sleep 1
done

# ── Step 2: Create database and apply schema ────────────────────────────────
psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d postgres \
  -c "CREATE DATABASE ${FALCON_DB};" 2>/dev/null || true
ok "Database '${FALCON_DB}' ready"

info "Applying MES schema..."
psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -f "${POC_ROOT}/schema/init.sql" > "${OUTPUT_DIR}/schema.log" 2>&1
ok "Schema applied (4 tables: work_order, operation, operation_report, work_order_state_log)"

# ── Step 3: Start backend API ───────────────────────────────────────────────
info "Starting MES backend API..."
cd "${POC_ROOT}/backend"
FALCON_HOST="${HOST}" FALCON_PORT="${FALCON_PORT}" FALCON_DB="${FALCON_DB}" \
  python3 app.py > "${OUTPUT_DIR}/backend.log" 2>&1 &
BACKEND_PID=$!
echo "${BACKEND_PID}" > "${OUTPUT_DIR}/backend.pid"
cd "${POC_ROOT}"

for i in $(seq 1 15); do
  if curl -sf "http://${HOST}:${BACKEND_PORT}/api/health" &>/dev/null; then
    ok "Backend API ready (pid ${BACKEND_PID}, port ${BACKEND_PORT}, ${i}s)"
    break
  fi
  sleep 1
done

echo ""
echo "  ┌──────────────────────────────────────────────────────┐"
echo "  │  FalconDB MES 系统已启动                              │"
echo "  │                                                      │"
echo "  │  Database:  psql -h ${HOST} -p ${FALCON_PORT} -d ${FALCON_DB} │"
echo "  │  REST API:  http://${HOST}:${BACKEND_PORT}/docs       │"
echo "  │  Health:    http://${HOST}:${BACKEND_PORT}/api/health │"
echo "  └──────────────────────────────────────────────────────┘"
echo ""
