#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Start PostgreSQL Server
# ============================================================================
# Starts a local PostgreSQL instance using a temporary data directory
# with the PoC benchmark configuration.
#
# If PostgreSQL is already running on port 5432, this script will use it
# as-is (just verifies connectivity).
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"

PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-$(whoami)}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_DATADIR="${PG_DATADIR:-./pg_bench_data}"
PG_CONF="${POC_ROOT}/conf/postgres.bench.conf"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

mkdir -p "${RESULTS_DIR}/raw/postgres"

# ── Check if PostgreSQL is already running ─────────────────────────────────
if pg_isready -h "${PG_HOST}" -p "${PG_PORT}" &>/dev/null; then
  ok "PostgreSQL already running on port ${PG_PORT}"
  PG_VER=$(psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres \
    -t -A -c "SHOW server_version;" 2>/dev/null || echo "unknown")
  ok "Version: ${PG_VER}"
  echo "existing" > "${RESULTS_DIR}/raw/postgres/start_mode.txt"
  exit 0
fi

# ── Initialize fresh data directory ────────────────────────────────────────
info "PostgreSQL not running. Initializing fresh instance..."

if ! command -v initdb &>/dev/null; then
  fail "initdb not found. Install PostgreSQL server package."
  exit 1
fi

rm -rf "${PG_DATADIR}"

initdb -D "${PG_DATADIR}" --auth=trust --username="${PG_USER}" \
  > "${RESULTS_DIR}/raw/postgres/initdb.log" 2>&1

# Apply benchmark config
if [ -f "${PG_CONF}" ]; then
  cat "${PG_CONF}" >> "${PG_DATADIR}/postgresql.conf"
  ok "Applied benchmark config"
fi

# Set port
echo "port = ${PG_PORT}" >> "${PG_DATADIR}/postgresql.conf"

# ── Start PostgreSQL ──────────────────────────────────────────────────────
info "Starting PostgreSQL on port ${PG_PORT}..."

pg_ctl -D "${PG_DATADIR}" -l "${RESULTS_DIR}/raw/postgres/server.log" \
  -o "-p ${PG_PORT}" start

# Wait for ready
for i in $(seq 1 30); do
  if pg_isready -h "${PG_HOST}" -p "${PG_PORT}" &>/dev/null; then
    PG_PID=$(head -1 "${PG_DATADIR}/postmaster.pid" 2>/dev/null || echo "unknown")
    echo "${PG_PID}" > "${RESULTS_DIR}/raw/postgres/postgres.pid"
    ok "PostgreSQL ready (pid ${PG_PID}, ${i}s)"
    echo "managed" > "${RESULTS_DIR}/raw/postgres/start_mode.txt"
    exit 0
  fi
  sleep 1
done

fail "PostgreSQL did not start within 30s"
exit 1
