#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Initialize pgbench Tables
# ============================================================================
# Creates the pgbench schema on both FalconDB and PostgreSQL.
# Records initialization time for both systems.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"

FALCON_HOST="${FALCON_HOST:-127.0.0.1}"
FALCON_PORT="${FALCON_PORT:-5433}"
FALCON_USER="${FALCON_USER:-falcon}"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-$(whoami)}"

SCALE="${PGBENCH_SCALE:-10}"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Initializing pgbench (scale=${SCALE})"

mkdir -p "${RESULTS_DIR}/raw/falcon" "${RESULTS_DIR}/raw/postgres"

# ── FalconDB ──────────────────────────────────────────────────────────────
banner "FalconDB (${FALCON_HOST}:${FALCON_PORT})"

# Create database
psql -h "${FALCON_HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" -d postgres \
  -c "CREATE DATABASE pgbench;" 2>/dev/null || true

info "Running pgbench -i -s ${SCALE} on FalconDB..."
FALCON_INIT_START=$(date +%s%N 2>/dev/null || date +%s)

pgbench -h "${FALCON_HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" \
  -i -s "${SCALE}" pgbench \
  > "${RESULTS_DIR}/raw/falcon/init.log" 2>&1

FALCON_INIT_END=$(date +%s%N 2>/dev/null || date +%s)

# Verify tables
FALCON_ROWS=$(psql -h "${FALCON_HOST}" -p "${FALCON_PORT}" -U "${FALCON_USER}" \
  -d pgbench -t -A -c "SELECT COUNT(*) FROM pgbench_accounts;" 2>/dev/null || echo "0")
FALCON_ROWS=$(echo "${FALCON_ROWS}" | tr -d '[:space:]')

EXPECTED_ROWS=$((SCALE * 100000))
if [ "${FALCON_ROWS}" = "${EXPECTED_ROWS}" ]; then
  ok "FalconDB: ${FALCON_ROWS} accounts (expected ${EXPECTED_ROWS})"
else
  fail "FalconDB: ${FALCON_ROWS} accounts (expected ${EXPECTED_ROWS})"
fi

# ── PostgreSQL ────────────────────────────────────────────────────────────
banner "PostgreSQL (${PG_HOST}:${PG_PORT})"

psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d postgres \
  -c "CREATE DATABASE pgbench;" 2>/dev/null || true

info "Running pgbench -i -s ${SCALE} on PostgreSQL..."
PG_INIT_START=$(date +%s%N 2>/dev/null || date +%s)

pgbench -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" \
  -i -s "${SCALE}" pgbench \
  > "${RESULTS_DIR}/raw/postgres/init.log" 2>&1

PG_INIT_END=$(date +%s%N 2>/dev/null || date +%s)

PG_ROWS=$(psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" \
  -d pgbench -t -A -c "SELECT COUNT(*) FROM pgbench_accounts;" 2>/dev/null || echo "0")
PG_ROWS=$(echo "${PG_ROWS}" | tr -d '[:space:]')

if [ "${PG_ROWS}" = "${EXPECTED_ROWS}" ]; then
  ok "PostgreSQL: ${PG_ROWS} accounts (expected ${EXPECTED_ROWS})"
else
  fail "PostgreSQL: ${PG_ROWS} accounts (expected ${EXPECTED_ROWS})"
fi

# ── Save init metadata ────────────────────────────────────────────────────
cat > "${RESULTS_DIR}/parsed/init_metadata.json" <<EOF
{
  "scale": ${SCALE},
  "expected_rows": ${EXPECTED_ROWS},
  "falcon_rows": ${FALCON_ROWS},
  "postgres_rows": ${PG_ROWS},
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

ok "Initialization complete for both systems"
