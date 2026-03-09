#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Start FalconDB Server
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"

FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
CONF="${POC_ROOT}/conf/falcon.bench.toml"
HOST="127.0.0.1"
PORT=5433

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

# Resolve binary
if [ ! -f "${FALCON_BIN}" ]; then
  REPO_BIN="$(cd "${POC_ROOT}/.." && pwd)/${FALCON_BIN}"
  [ -f "${REPO_BIN}" ] && FALCON_BIN="${REPO_BIN}"
fi

if [ ! -f "${FALCON_BIN}" ]; then
  fail "FalconDB binary not found. Build: cargo build -p falcon_server --release"
  exit 1
fi

# Clean previous data
rm -rf ./falcon_bench_data

mkdir -p "${RESULTS_DIR}/raw/falcon"

info "Starting FalconDB on port ${PORT}..."
"${FALCON_BIN}" -c "${CONF}" > "${RESULTS_DIR}/raw/falcon/server.log" 2>&1 &
FALCON_PID=$!
echo "${FALCON_PID}" > "${RESULTS_DIR}/raw/falcon/falcon.pid"

# Wait for ready
for i in $(seq 1 30); do
  if psql -h "${HOST}" -p "${PORT}" -U falcon -d postgres -c "SELECT 1;" &>/dev/null; then
    ok "FalconDB ready (pid ${FALCON_PID}, ${i}s)"
    exit 0
  fi
  sleep 1
done

fail "FalconDB did not start within 30s"
cat "${RESULTS_DIR}/raw/falcon/server.log" | tail -20
exit 1
