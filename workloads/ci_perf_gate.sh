#!/usr/bin/env bash
# CI Performance Gate — runs all workloads and enforces minimum thresholds.
#
# Usage:
#   ./ci_perf_gate.sh                              # full suite
#   ./ci_perf_gate.sh --workload a                 # single workload
#   ./ci_perf_gate.sh --min-tps-a 5000 --min-tps-b 8000
#
# Exit codes:
#   0 = all gates passed
#   1 = at least one gate failed
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/ci_results"
mkdir -p "${RESULTS_DIR}"

# ── Thresholds (override via args or env) ─────────────────────────────────
MIN_TPS_A="${MIN_TPS_A:-3000}"
MIN_TPS_B="${MIN_TPS_B:-5000}"
WORKLOAD=""
CLIENTS="${CLIENTS:-16}"
DURATION="${DURATION:-30}"
TARGET="${TARGET:-falcondb}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload)   WORKLOAD="$2";    shift 2 ;;
        --min-tps-a)  MIN_TPS_A="$2";   shift 2 ;;
        --min-tps-b)  MIN_TPS_B="$2";   shift 2 ;;
        --clients)    CLIENTS="$2";     shift 2 ;;
        --duration)   DURATION="$2";    shift 2 ;;
        --target)     TARGET="$2";      shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
GATE_LOG="${RESULTS_DIR}/gate_${TIMESTAMP}.log"
GATE_JSON="${RESULTS_DIR}/gate_${TIMESTAMP}.json"
PASS=true

RED='\033[0;31m'; GREEN='\033[0;32m'; BOLD='\033[1m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1" | tee -a "${GATE_LOG}"; }
fail() { echo -e "  ${RED}✗${NC} $1" | tee -a "${GATE_LOG}"; PASS=false; }

echo "" | tee -a "${GATE_LOG}"
echo -e "${BOLD}═══ FalconDB CI Performance Gate ═══${NC}" | tee -a "${GATE_LOG}"
echo "  Timestamp: ${TIMESTAMP}" | tee -a "${GATE_LOG}"
echo "  Target:    ${TARGET}" | tee -a "${GATE_LOG}"
echo "  Clients:   ${CLIENTS}" | tee -a "${GATE_LOG}"
echo "  Duration:  ${DURATION}s" | tee -a "${GATE_LOG}"
echo "" | tee -a "${GATE_LOG}"

TPS_A="0"
TPS_B="0"
VERDICT_C="SKIP"

# ── Workload A ────────────────────────────────────────────────────────────
if [ -z "${WORKLOAD}" ] || [ "${WORKLOAD}" = "a" ]; then
    echo -e "\n${BOLD}── Workload A: Write-Heavy ──${NC}" | tee -a "${GATE_LOG}"

    bash "${SCRIPT_DIR}/workload_a_write_heavy/run.sh" \
        --target "${TARGET}" --clients "${CLIENTS}" --duration "${DURATION}" \
        2>&1 | tee -a "${GATE_LOG}"

    # Find latest result JSON
    LATEST_A=$(ls -t "${SCRIPT_DIR}/workload_a_write_heavy/results/workload_a_${TARGET}_"*.json 2>/dev/null | head -1 || true)
    if [ -n "${LATEST_A}" ]; then
        TPS_A=$(grep -oP '"tps":\s*\K[0-9.]+' "${LATEST_A}" | head -1 || echo "0")
        TPS_A_INT=${TPS_A%.*}

        if [ "${TPS_A_INT}" -ge "${MIN_TPS_A}" ]; then
            ok "Workload A: ${TPS_A} TPS >= ${MIN_TPS_A} threshold"
        else
            fail "Workload A: ${TPS_A} TPS < ${MIN_TPS_A} threshold"
        fi
    else
        fail "Workload A: no result file found"
    fi
fi

# ── Workload B ────────────────────────────────────────────────────────────
if [ -z "${WORKLOAD}" ] || [ "${WORKLOAD}" = "b" ]; then
    echo -e "\n${BOLD}── Workload B: Read-Write OLTP ──${NC}" | tee -a "${GATE_LOG}"

    bash "${SCRIPT_DIR}/workload_b_read_write/run.sh" \
        --target "${TARGET}" --clients "${CLIENTS}" --duration "${DURATION}" \
        2>&1 | tee -a "${GATE_LOG}"

    LATEST_B=$(ls -t "${SCRIPT_DIR}/workload_b_read_write/results/workload_b_${TARGET}_"*.json 2>/dev/null | head -1 || true)
    if [ -n "${LATEST_B}" ]; then
        TPS_B=$(grep -oP '"tps":\s*\K[0-9.]+' "${LATEST_B}" | head -1 || echo "0")
        TPS_B_INT=${TPS_B%.*}

        if [ "${TPS_B_INT}" -ge "${MIN_TPS_B}" ]; then
            ok "Workload B: ${TPS_B} TPS >= ${MIN_TPS_B} threshold"
        else
            fail "Workload B: ${TPS_B} TPS < ${MIN_TPS_B} threshold"
        fi
    else
        fail "Workload B: no result file found"
    fi
fi

# ── Workload C ────────────────────────────────────────────────────────────
if [ -z "${WORKLOAD}" ] || [ "${WORKLOAD}" = "c" ]; then
    echo -e "\n${BOLD}── Workload C: Durability & Crash ──${NC}" | tee -a "${GATE_LOG}"

    if bash "${SCRIPT_DIR}/workload_c_durability/crash_runner.sh" \
        --rounds 2 --count 2000 --kill-after 1000 2>&1 | tee -a "${GATE_LOG}"; then
        VERDICT_C="PASS"
        ok "Workload C: crash recovery PASS"
    else
        VERDICT_C="FAIL"
        fail "Workload C: crash recovery FAIL"
    fi
fi

# ── Gate result ───────────────────────────────────────────────────────────
FINAL="PASS"
if [ "${PASS}" = "false" ]; then
    FINAL="FAIL"
fi

cat > "${GATE_JSON}" <<EOF
{
  "ci_perf_gate": true,
  "timestamp": "${TIMESTAMP}",
  "target": "${TARGET}",
  "config": {
    "clients": ${CLIENTS},
    "duration_sec": ${DURATION}
  },
  "gates": {
    "workload_a": {
      "tps": ${TPS_A},
      "threshold": ${MIN_TPS_A},
      "pass": $([ "${TPS_A%.*}" -ge "${MIN_TPS_A}" ] 2>/dev/null && echo "true" || echo "false")
    },
    "workload_b": {
      "tps": ${TPS_B},
      "threshold": ${MIN_TPS_B},
      "pass": $([ "${TPS_B%.*}" -ge "${MIN_TPS_B}" ] 2>/dev/null && echo "true" || echo "false")
    },
    "workload_c": {
      "verdict": "${VERDICT_C}",
      "pass": $([ "${VERDICT_C}" = "PASS" ] && echo "true" || echo "false")
    }
  },
  "verdict": "${FINAL}"
}
EOF

echo "" | tee -a "${GATE_LOG}"
echo -e "${BOLD}═══ Gate Verdict: ${FINAL} ═══${NC}" | tee -a "${GATE_LOG}"
echo "  Log:    ${GATE_LOG}" | tee -a "${GATE_LOG}"
echo "  JSON:   ${GATE_JSON}" | tee -a "${GATE_LOG}"
echo ""

if [ "${FINAL}" = "FAIL" ]; then
    exit 1
fi
