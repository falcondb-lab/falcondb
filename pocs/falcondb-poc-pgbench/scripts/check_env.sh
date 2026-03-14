#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Environment Check
# ============================================================================
# Verifies all prerequisites are met before running the benchmark.
# Prints hardware/software info for reproducibility.
# Exits non-zero if any requirement is missing.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"

FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

ERRORS=0

banner "FalconDB pgbench PoC — Environment Check"

# ── Required tools ────────────────────────────────────────────────────────
banner "1. Required Tools"

check_tool() {
  local cmd="$1" label="$2"
  if command -v "${cmd}" &>/dev/null; then
    local ver
    ver=$("${cmd}" --version 2>/dev/null | head -1 || echo "unknown")
    ok "${label}: ${ver}"
  else
    fail "${label}: NOT FOUND"
    ERRORS=$((ERRORS + 1))
  fi
}

check_tool "pgbench" "pgbench"
check_tool "psql" "psql"

# FalconDB binary
if [ ! -f "${FALCON_BIN}" ]; then
  # Try from repo root
  REPO_BIN="$(cd "${POC_ROOT}/.." && pwd)/${FALCON_BIN}"
  if [ -f "${REPO_BIN}" ]; then
    FALCON_BIN="${REPO_BIN}"
    ok "FalconDB: ${FALCON_BIN}"
  else
    fail "FalconDB binary not found at '${FALCON_BIN}'"
    info "Build it: cargo build -p falcon_server --release"
    ERRORS=$((ERRORS + 1))
  fi
else
  ok "FalconDB: ${FALCON_BIN}"
fi

# PostgreSQL server
if command -v pg_isready &>/dev/null; then
  ok "pg_isready available"
elif command -v pg_ctl &>/dev/null; then
  ok "pg_ctl available"
else
  fail "PostgreSQL server tools (pg_isready or pg_ctl) not found"
  ERRORS=$((ERRORS + 1))
fi

# ── Hardware info ─────────────────────────────────────────────────────────
banner "2. Hardware"

ENV_FILE="${RESULTS_DIR}/parsed/environment.json"
mkdir -p "${RESULTS_DIR}/parsed"

OS_NAME=$(uname -s 2>/dev/null || echo "unknown")
OS_VERSION=$(uname -r 2>/dev/null || echo "unknown")
OS_FULL="${OS_NAME} ${OS_VERSION}"

if [ -f /proc/cpuinfo ]; then
  CPU_MODEL=$(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | xargs || echo "unknown")
  CPU_CORES=$(grep -c "^processor" /proc/cpuinfo 2>/dev/null || echo "unknown")
elif command -v sysctl &>/dev/null; then
  CPU_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
  CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "unknown")
else
  CPU_MODEL="unknown"
  CPU_CORES="unknown"
fi

if [ -f /proc/meminfo ]; then
  RAM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
  RAM_GB=$(echo "scale=1; ${RAM_KB}/1048576" | bc 2>/dev/null || echo "unknown")
elif command -v sysctl &>/dev/null; then
  RAM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo "0")
  RAM_GB=$(echo "scale=1; ${RAM_BYTES}/1073741824" | bc 2>/dev/null || echo "unknown")
else
  RAM_GB="unknown"
fi

info "OS:   ${OS_FULL}"
info "CPU:  ${CPU_MODEL} (${CPU_CORES} cores)"
info "RAM:  ${RAM_GB} GB"

# Storage type detection (best effort)
STORAGE="unknown"
if command -v lsblk &>/dev/null; then
  STORAGE=$(lsblk -d -o NAME,ROTA 2>/dev/null | grep -m1 " 0$" && echo "SSD/NVMe" || echo "HDD or unknown")
fi
info "Storage: ${STORAGE}"

# ── Write environment JSON ────────────────────────────────────────────────
cat > "${ENV_FILE}" <<EOF
{
  "os": "${OS_FULL}",
  "cpu_model": "${CPU_MODEL}",
  "cpu_cores": "${CPU_CORES}",
  "ram_gb": "${RAM_GB}",
  "storage": "${STORAGE}",
  "pgbench_version": "$(pgbench --version 2>/dev/null | head -1 || echo 'unknown')",
  "psql_version": "$(psql --version 2>/dev/null | head -1 || echo 'unknown')",
  "falcon_bin": "${FALCON_BIN}",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

ok "Environment saved: ${ENV_FILE}"

# ── Verdict ───────────────────────────────────────────────────────────────
banner "Result"

if [ "${ERRORS}" -eq 0 ]; then
  ok "All prerequisites met. Ready to benchmark."
  exit 0
else
  fail "${ERRORS} prerequisite(s) missing. Fix the above errors and re-run."
  exit 1
fi
