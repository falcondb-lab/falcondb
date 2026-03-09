#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — One-Command Benchmark
# ============================================================================
#
# Runs the entire FalconDB vs PostgreSQL comparison end-to-end:
#
#   1. Check environment prerequisites
#   2. Start FalconDB
#   3. Start PostgreSQL
#   4. Initialize pgbench tables on both
#   5. Run pgbench against FalconDB (warm-up + 3 measured)
#   6. Run pgbench against PostgreSQL (warm-up + 3 measured)
#   7. Collect and compare results
#   8. Cleanup
#
# Usage:
#   ./run_benchmark.sh
#   PGBENCH_SCALE=50 PGBENCH_CLIENTS=20 PGBENCH_DURATION=120 ./run_benchmark.sh
#
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
export PGBENCH_SCALE="${PGBENCH_SCALE:-10}"
export PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-10}"
export PGBENCH_JOBS="${PGBENCH_JOBS:-2}"
export PGBENCH_DURATION="${PGBENCH_DURATION:-60}"
export PGBENCH_MODE="${PGBENCH_MODE:-tpcb-like}"

BLUE='\033[0;34m'; BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
banner() { echo -e "\n${BLUE}${BOLD}═══  $1  ═══${NC}\n"; }

# Cleanup on exit
trap "bash ${SCRIPT_DIR}/scripts/cleanup.sh 2>/dev/null || true" EXIT

banner "FalconDB vs PostgreSQL — pgbench Comparison"
echo ""
echo "  Scale:       ${PGBENCH_SCALE}"
echo "  Clients:     ${PGBENCH_CLIENTS}"
echo "  Jobs:        ${PGBENCH_JOBS}"
echo "  Duration:    ${PGBENCH_DURATION}s per run"
echo "  Mode:        ${PGBENCH_MODE}"
echo "  Runs:        1 warm-up + 3 measured (per system)"
echo ""

# ── Step 1: Environment check ─────────────────────────────────────────────
banner "Step 1/7: Environment Check"
bash "${SCRIPT_DIR}/scripts/check_env.sh"

# ── Step 2: Start FalconDB ────────────────────────────────────────────────
banner "Step 2/7: Starting FalconDB"
bash "${SCRIPT_DIR}/scripts/start_falcondb.sh"

# ── Step 3: Start PostgreSQL ──────────────────────────────────────────────
banner "Step 3/7: Starting PostgreSQL"
bash "${SCRIPT_DIR}/scripts/start_postgres.sh"

# ── Step 4: Initialize pgbench ────────────────────────────────────────────
banner "Step 4/7: Initializing pgbench Tables"
bash "${SCRIPT_DIR}/scripts/init_pgbench.sh"

# ── Step 5: Benchmark FalconDB ────────────────────────────────────────────
banner "Step 5/7: Benchmarking FalconDB"
bash "${SCRIPT_DIR}/scripts/run_pgbench_falcon.sh"

# ── Step 6: Benchmark PostgreSQL ──────────────────────────────────────────
banner "Step 6/7: Benchmarking PostgreSQL"
bash "${SCRIPT_DIR}/scripts/run_pgbench_postgres.sh"

# ── Step 7: Collect results ───────────────────────────────────────────────
banner "Step 7/7: Collecting Results"
bash "${SCRIPT_DIR}/scripts/collect_results.sh"

echo ""
echo -e "  ${GREEN}${BOLD}Benchmark complete.${NC}"
echo ""
echo "  Report:  ${SCRIPT_DIR}/results/report.md"
echo "  Summary: ${SCRIPT_DIR}/results/parsed/summary.json"
echo "  Raw:     ${SCRIPT_DIR}/results/raw/"
echo ""
