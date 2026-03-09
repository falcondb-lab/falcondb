#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Start 2-Node Cluster
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
CONF_PRIMARY="${POC_ROOT}/configs/primary.toml"
CONF_REPLICA="${POC_ROOT}/configs/replica.toml"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
PRIMARY_PORT=5433
REPLICA_PORT=5434
DB="falcon"
USER="falcon"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

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

# Clean previous data
rm -rf ./poc_data_primary ./poc_data_replica
rm -f "${OUTPUT_DIR}/primary.pid" "${OUTPUT_DIR}/replica.pid"
rm -f "${OUTPUT_DIR}/primary.log" "${OUTPUT_DIR}/replica.log"

# Start PRIMARY
info "Starting PRIMARY on port ${PRIMARY_PORT}..."
"${FALCON_BIN}" -c "${CONF_PRIMARY}" > "${OUTPUT_DIR}/primary.log" 2>&1 &
PRIMARY_PID=$!
echo "${PRIMARY_PID}" > "${OUTPUT_DIR}/primary.pid"

# Start REPLICA
info "Starting REPLICA on port ${REPLICA_PORT}..."
"${FALCON_BIN}" -c "${CONF_REPLICA}" > "${OUTPUT_DIR}/replica.log" 2>&1 &
REPLICA_PID=$!
echo "${REPLICA_PID}" > "${OUTPUT_DIR}/replica.pid"

# Wait for nodes
wait_for_node() {
  local port="$1" label="$2" max=30
  for i in $(seq 1 ${max}); do
    if psql -h "${HOST}" -p "${port}" -U "${USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
      ok "${label} ready (${i}s)"
      return 0
    fi
    sleep 1
  done
  fail "${label} did not start within ${max}s"
  return 1
}

wait_for_node "${PRIMARY_PORT}" "PRIMARY (pid ${PRIMARY_PID})"
wait_for_node "${REPLICA_PORT}" "REPLICA (pid ${REPLICA_PID})"

# Create database and schema
info "Creating database and schema..."
psql -h "${HOST}" -p "${PRIMARY_PORT}" -U "${USER}" -d postgres \
  -c "CREATE DATABASE ${DB};" 2>/dev/null || true
psql -h "${HOST}" -p "${PRIMARY_PORT}" -U "${USER}" -d "${DB}" \
  -f "${POC_ROOT}/schema/tx_markers.sql" 2>/dev/null
ok "Database '${DB}' and table 'tx_markers' created"

sleep 2

echo ""
echo "  ┌──────────────────────────────────────────────┐"
echo "  │  FalconDB 2-node cluster is running           │"
echo "  │  PRIMARY:  ${HOST}:${PRIMARY_PORT} (pid ${PRIMARY_PID})   │"
echo "  │  REPLICA:  ${HOST}:${REPLICA_PORT} (pid ${REPLICA_PID})   │"
echo "  └──────────────────────────────────────────────┘"
echo ""
