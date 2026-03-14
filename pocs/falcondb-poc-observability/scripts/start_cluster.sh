#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Start 2-Node Cluster
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
CONF_NODE1="${POC_ROOT}/configs/node1.toml"
CONF_NODE2="${POC_ROOT}/configs/node2.toml"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
NODE1_PORT=5433
NODE2_PORT=5434
DB="falcon"
USER="falcon"

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

# Clean previous data
rm -rf ./obs_data_node1 ./obs_data_node2
rm -f "${OUTPUT_DIR}/node1.pid" "${OUTPUT_DIR}/node2.pid"
rm -f "${OUTPUT_DIR}/node1.log" "${OUTPUT_DIR}/node2.log"

# Start nodes
banner "Starting FalconDB 2-node cluster"

info "Starting Node 1 (primary) on port ${NODE1_PORT}..."
"${FALCON_BIN}" -c "${CONF_NODE1}" > "${OUTPUT_DIR}/node1.log" 2>&1 &
NODE1_PID=$!
echo "${NODE1_PID}" > "${OUTPUT_DIR}/node1.pid"

info "Starting Node 2 (replica) on port ${NODE2_PORT}..."
"${FALCON_BIN}" -c "${CONF_NODE2}" > "${OUTPUT_DIR}/node2.log" 2>&1 &
NODE2_PID=$!
echo "${NODE2_PID}" > "${OUTPUT_DIR}/node2.pid"

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

wait_for_node "${NODE1_PORT}" "Node 1 / primary (pid ${NODE1_PID})"
wait_for_node "${NODE2_PORT}" "Node 2 / replica (pid ${NODE2_PID})"

# Create database + demo table
info "Creating database and demo table..."
psql -h "${HOST}" -p "${NODE1_PORT}" -U "${USER}" -d postgres \
  -c "CREATE DATABASE ${DB};" 2>/dev/null || true
psql -h "${HOST}" -p "${NODE1_PORT}" -U "${USER}" -d "${DB}" <<'SQL' 2>/dev/null
CREATE TABLE IF NOT EXISTS demo_events (
    id         BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload    TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
SQL
ok "Database '${DB}' and table 'demo_events' created"

# Verify metrics endpoints
for port in 8080 8081; do
  if curl -sf "http://${HOST}:${port}/metrics" > /dev/null 2>&1; then
    ok "Metrics endpoint http://${HOST}:${port}/metrics is live"
  else
    info "Metrics endpoint on port ${port} not yet available (may need a moment)"
  fi
done

sleep 2

echo ""
echo "  ┌────────────────────────────────────────────────────────┐"
echo "  │  FalconDB cluster is running                           │"
echo "  │  Node 1 (primary): psql -h ${HOST} -p ${NODE1_PORT}            │"
echo "  │  Node 2 (replica): psql -h ${HOST} -p ${NODE2_PORT}            │"
echo "  │  Metrics: http://${HOST}:8080/metrics                  │"
echo "  │           http://${HOST}:8081/metrics                  │"
echo "  └────────────────────────────────────────────────────────┘"
echo ""
