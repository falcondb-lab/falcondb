#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #5 — Migration: Start PostgreSQL
# ============================================================================
# Starts a local PostgreSQL instance for the source database.
# Requires: PostgreSQL installed (pg_ctl, psql, initdb)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_DB="shop_demo"
PG_DATA="${POC_ROOT}/pg_data"
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

banner "Starting PostgreSQL (source database)"

# Check if already running on this port
if psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
  ok "PostgreSQL already running on port ${PG_PORT}"
else
  # Initialize if needed
  if [ ! -d "${PG_DATA}" ]; then
    info "Initializing PostgreSQL data directory..."
    initdb -D "${PG_DATA}" --auth=trust --username="${PG_USER}" > "${OUTPUT_DIR}/pg_init.log" 2>&1
    ok "Data directory initialized: ${PG_DATA}"
  fi

  # Start
  info "Starting PostgreSQL on port ${PG_PORT}..."
  pg_ctl -D "${PG_DATA}" -l "${OUTPUT_DIR}/postgres.log" \
    -o "-p ${PG_PORT} -k /tmp" start > /dev/null 2>&1

  # Wait for ready
  for i in $(seq 1 20); do
    if psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres -c "SELECT 1;" &>/dev/null; then
      ok "PostgreSQL ready on port ${PG_PORT} (${i}s)"
      break
    fi
    sleep 1
  done
fi

# Create database
psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres \
  -c "CREATE DATABASE ${PG_DB};" 2>/dev/null || true
ok "Database '${PG_DB}' exists"

echo ""
echo "  PostgreSQL: psql -h ${HOST} -p ${PG_PORT} -U ${PG_USER} -d ${PG_DB}"
echo ""
