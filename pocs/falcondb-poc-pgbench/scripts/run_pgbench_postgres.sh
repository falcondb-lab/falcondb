#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Run pgbench Against PostgreSQL
# ============================================================================
# Runs 1 warm-up + 3 measured runs against PostgreSQL.
# All raw output is preserved under results/raw/postgres/.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-$(whoami)}"

CONCURRENCY="${PGBENCH_CLIENTS:-10}"
JOBS="${PGBENCH_JOBS:-2}"
DURATION="${PGBENCH_DURATION:-60}"
MODE="${PGBENCH_MODE:-tpcb-like}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

RAW_DIR="${RESULTS_DIR}/raw/postgres"
mkdir -p "${RAW_DIR}"

# Build mode flag
MODE_FLAG=""
case "${MODE}" in
  "read-only"|"select-only") MODE_FLAG="-S" ;;
  "simple-update")           MODE_FLAG="-N" ;;
  "tpcb-like"|"default"|"")  MODE_FLAG="" ;;
  *) echo "Unknown mode: ${MODE}"; exit 1 ;;
esac

banner "PostgreSQL pgbench: clients=${CONCURRENCY}, jobs=${JOBS}, duration=${DURATION}s, mode=${MODE}"

# ── Warm-up run (not recorded in final results) ──────────────────────────
info "Warm-up run (15s, not measured)..."
pgbench -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" \
  -c "${CONCURRENCY}" -j "${JOBS}" -T 15 ${MODE_FLAG} \
  pgbench > "${RAW_DIR}/warmup.log" 2>&1 || true
ok "Warm-up complete"

# ── Measured runs ─────────────────────────────────────────────────────────
for RUN in 1 2 3; do
  info "Run ${RUN}/3 (${DURATION}s)..."

  pgbench -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" \
    -c "${CONCURRENCY}" -j "${JOBS}" -T "${DURATION}" ${MODE_FLAG} \
    --progress=10 \
    pgbench > "${RAW_DIR}/run_${RUN}.log" 2>&1

  # Extract TPS from output
  TPS=$(grep -oP 'tps = \K[0-9.]+' "${RAW_DIR}/run_${RUN}.log" | tail -1 || echo "N/A")
  LATENCY=$(grep -oP 'latency average = \K[0-9.]+' "${RAW_DIR}/run_${RUN}.log" || echo "N/A")

  ok "Run ${RUN}: TPS=${TPS}, avg latency=${LATENCY} ms"
done

# ── Save run parameters ──────────────────────────────────────────────────
cat > "${RAW_DIR}/parameters.json" <<EOF
{
  "system": "postgresql",
  "host": "${PG_HOST}",
  "port": ${PG_PORT},
  "clients": ${CONCURRENCY},
  "jobs": ${JOBS},
  "duration_sec": ${DURATION},
  "mode": "${MODE}",
  "runs": 3,
  "warmup_sec": 15,
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

ok "All PostgreSQL runs complete. Raw output: ${RAW_DIR}/"
