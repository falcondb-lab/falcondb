#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #5 — Migration: Initialize Sample App Database on PostgreSQL
# ============================================================================
# Applies schema + seed data to the PostgreSQL source database.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_DB="shop_demo"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Initializing PostgreSQL Source Database"

# Apply schema
info "Applying schema..."
if psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" \
  -f "${POC_ROOT}/schema/postgres_schema.sql" > "${OUTPUT_DIR}/schema_apply_pg.log" 2>&1; then
  ok "Schema applied"
else
  fail "Schema apply failed — check ${OUTPUT_DIR}/schema_apply_pg.log"
  exit 1
fi

# Apply seed data
info "Loading seed data..."
if psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" \
  -f "${POC_ROOT}/data/sample_seed.sql" > "${OUTPUT_DIR}/seed_apply_pg.log" 2>&1; then
  ok "Seed data loaded"
else
  fail "Seed data load failed — check ${OUTPUT_DIR}/seed_apply_pg.log"
  exit 1
fi

# Verify
info "Verifying row counts..."
COUNTS=$(psql -h "${HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -t -A -c "
  SELECT 'customers=' || COUNT(*) FROM customers
  UNION ALL
  SELECT 'orders=' || COUNT(*) FROM orders
  UNION ALL
  SELECT 'order_items=' || COUNT(*) FROM order_items
  UNION ALL
  SELECT 'payments=' || COUNT(*) FROM payments;
")
echo "${COUNTS}" | while IFS= read -r line; do
  ok "${line}"
done

echo ""
ok "PostgreSQL source database ready"
echo ""
