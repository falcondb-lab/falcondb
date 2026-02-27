#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #6 — Cost Efficiency: Start PostgreSQL (Production-Sized)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_DB="bench"
PG_DATA="${POC_ROOT}/pg_data_large"
PG_CONF="${POC_ROOT}/conf/postgres.large.conf"
HOST="127.0.0.1"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

if ! command -v pg_ctl &>/dev/null; then
  fail "pg_ctl not found. Install PostgreSQL."
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

banner "Starting PostgreSQL (Production-Sized Reference)"

info "Config: ${PG_CONF}"
info "shared_buffers: 4 GB | effective_cache_size: 12 GB"
info "Port: ${PG_PORT}"

if psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
  ok "PostgreSQL already running on port ${PG_PORT}"
else
  if [ ! -d "${PG_DATA}" ]; then
    info "Initializing PostgreSQL data directory..."
    initdb -D "${PG_DATA}" --auth=trust --username="${PG_USER}" > "${OUTPUT_DIR}/pg_init.log" 2>&1
    # Copy our production config
    cp "${PG_CONF}" "${PG_DATA}/postgresql.conf"
    ok "Data directory initialized with production config"
  fi

  info "Starting PostgreSQL..."
  pg_ctl -D "${PG_DATA}" -l "${OUTPUT_DIR}/postgres.log" \
    -o "-p ${PG_PORT} -k /tmp" start > /dev/null 2>&1

  for i in $(seq 1 30); do
    if psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
      ok "PostgreSQL ready on port ${PG_PORT} (${i}s)"
      break
    fi
    sleep 1
  done
fi

psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres \
  -c "CREATE DATABASE ${PG_DB};" 2>/dev/null || true
ok "Database '${PG_DB}' exists"

echo ""
echo "  PostgreSQL (large): psql -h ${HOST} -p ${PG_PORT} -U ${PG_USER} -d ${PG_DB}"
echo "  Resource envelope: 4 GB shared_buffers / 12 GB cache / fsync ON"
echo ""
