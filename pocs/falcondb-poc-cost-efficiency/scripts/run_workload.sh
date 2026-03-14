#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #6 — Cost Efficiency: Run Workload Against Both Databases
# ============================================================================
# Runs the same rate-limited OLTP workload against FalconDB and PostgreSQL.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"
WORKLOAD_BIN="${POC_ROOT}/workload/target/release/oltp_writer"

RATE="${WORKLOAD_RATE:-1000}"
DURATION="${WORKLOAD_DURATION:-60}"
THREADS="${WORKLOAD_THREADS:-4}"

HOST="127.0.0.1"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
FALCON_PORT=5433
FALCON_USER="falcon"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

mkdir -p "${OUTPUT_DIR}"

# Build workload generator if needed
if [ ! -f "${WORKLOAD_BIN}" ]; then
  info "Building workload generator..."
  (cd "${POC_ROOT}/workload" && cargo build --release 2>&1) || {
    fail "Failed to build workload generator"
    exit 1
  }
  ok "Workload generator built"
fi

banner "Running Workload: ${RATE} tx/s × ${DURATION}s (${THREADS} threads)"

# ── Run against FalconDB ────────────────────────────────────────────────────
banner "Phase 1: FalconDB (small footprint)"
"${WORKLOAD_BIN}" \
  --host "${HOST}" --port "${FALCON_PORT}" --user "${FALCON_USER}" --db bench \
  --rate "${RATE}" --duration "${DURATION}" --threads "${THREADS}" \
  --label falcon \
  --output "${OUTPUT_DIR}/workload_results_falcon.json"

ok "FalconDB workload complete"

# Brief cooldown
info "Cooldown (5s)..."
sleep 5

# ── Run against PostgreSQL ──────────────────────────────────────────────────
banner "Phase 2: PostgreSQL (production-sized)"
"${WORKLOAD_BIN}" \
  --host "${HOST}" --port "${PG_PORT}" --user "${PG_USER}" --db bench \
  --rate "${RATE}" --duration "${DURATION}" --threads "${THREADS}" \
  --label postgres \
  --output "${OUTPUT_DIR}/workload_results_postgres.json"

ok "PostgreSQL workload complete"

# ── Quick comparison ────────────────────────────────────────────────────────
banner "Quick Comparison"
echo "  See ${OUTPUT_DIR}/workload_results_falcon.json"
echo "  See ${OUTPUT_DIR}/workload_results_postgres.json"
echo ""
echo "  Run ./scripts/generate_cost_report.sh for full analysis."
echo ""
