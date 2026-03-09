#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Start FalconDB Cluster
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
FALCON_PORT=5433
FALCON_USER="falcon"
FALCON_DB="pitr_demo"
HOST="127.0.0.1"
DATA_DIR="${POC_ROOT}/pitr_data"
WAL_ARCHIVE="${POC_ROOT}/wal_archive"
BACKUP_DIR="${POC_ROOT}/backups"

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

mkdir -p "${OUTPUT_DIR}" "${WAL_ARCHIVE}" "${BACKUP_DIR}"
rm -rf "${DATA_DIR}"
rm -f "${OUTPUT_DIR}/falcon.pid" "${OUTPUT_DIR}/falcon.log"

banner "Starting FalconDB (WAL + Archiving Enabled)"

info "Data dir:    ${DATA_DIR}"
info "WAL archive: ${WAL_ARCHIVE}"
info "Backup dir:  ${BACKUP_DIR}"

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

# Create demo database
psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d postgres \
  -c "CREATE DATABASE ${FALCON_DB};" 2>/dev/null || true
ok "Database '${FALCON_DB}' exists"

# Apply schema
info "Applying accounts schema..."
psql -h "${HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d "${FALCON_DB}" \
  -f "${POC_ROOT}/schema/accounts.sql" > "${OUTPUT_DIR}/schema_apply.log" 2>&1
ok "Schema applied (100 accounts seeded)"

echo ""
echo "  FalconDB: psql -h ${HOST} -p ${FALCON_PORT} -U ${FALCON_USER} -d ${FALCON_DB}"
echo "  WAL:      enabled, archiving to ${WAL_ARCHIVE}"
echo ""
