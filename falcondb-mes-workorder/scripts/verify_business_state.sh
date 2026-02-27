#!/usr/bin/env bash
# ============================================================================
# FalconDB MES — Verify Business State After Failover
# ============================================================================
# Compares post-failover state against pre-failover snapshot.
# Checks all 4 business invariants.
# Outputs a human-readable report suitable for non-technical audiences.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
API="http://${HOST}:8000"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

REPORT="${OUTPUT_DIR}/verification_report.txt"
OVERALL_PASS=true

banner "业务状态验证 (Business State Verification After Failover)"

# ── Check API health ─────────────────────────────────────────────────────────
info "Checking system health..."
HEALTH=$(curl -sf "${API}/api/health" 2>/dev/null || echo '{"status":"down"}')
DB_STATUS=$(echo "${HEALTH}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','down'))" 2>/dev/null)
if [ "${DB_STATUS}" = "healthy" ]; then
  ok "System healthy: database connected"
else
  fail "System unhealthy: ${DB_STATUS}"
  OVERALL_PASS=false
fi

# ── Verify all work orders ───────────────────────────────────────────────────
info "Retrieving all work orders..."
WOS=$(curl -sf "${API}/api/work-orders" 2>/dev/null)
WO_COUNT=$(echo "${WOS}" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null)
ok "Found ${WO_COUNT} work orders"

# Start report
cat > "${REPORT}" << EOF
══════════════════════════════════════════════════════
  FalconDB MES — 业务状态验证报告
  Business State Verification Report
══════════════════════════════════════════════════════
  时间 (Time):  $(date -u +"%Y-%m-%d %H:%M:%S UTC")
  工单数 (Work Orders): ${WO_COUNT}
──────────────────────────────────────────────────────

EOF

# ── Verify each work order ───────────────────────────────────────────────────
echo "${WOS}" | python3 -c "
import sys, json
wos = json.load(sys.stdin)
for wo in wos:
    print(wo['work_order_id'])
" 2>/dev/null | while read -r WO_ID; do
  info "Verifying work order #${WO_ID}..."

  VERIFY=$(curl -sf "${API}/api/work-orders/${WO_ID}/verify" 2>/dev/null)

  PLANNED=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['planned_qty'])")
  COMPLETED=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed_qty'])")
  REPORTED=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_reported_qty'])")
  STATUS=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  RESULT=$(echo "${VERIFY}" | python3 -c "import sys,json; print(json.load(sys.stdin)['overall'])")

  if [ "${RESULT}" = "PASS" ]; then
    ok "Work order #${WO_ID}: ${RESULT}"
  else
    fail "Work order #${WO_ID}: ${RESULT}"
    OVERALL_PASS=false
  fi

  # Invariant details
  echo "${VERIFY}" | python3 -c "
import sys, json
v = json.load(sys.stdin)
for inv in v['invariants']:
    status = 'PASS' if inv['passed'] else 'FAIL'
    print(f'    {status}: {inv[\"name\"]} — {inv[\"detail\"]}')
" 2>/dev/null | while read -r LINE; do
    echo -e "  ${LINE}"
  done

  # Append to report
  cat >> "${REPORT}" << EOF
  工单 #${WO_ID} (Work Order #${WO_ID})
  ────────────────────────────────────
  产品 (Product):        $(echo "${VERIFY}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('product_code',''))" 2>/dev/null || echo "N/A")
  计划产量 (Planned):    ${PLANNED}
  已报工产量 (Reported): ${REPORTED}
  已完成产量 (Completed):${COMPLETED}
  工单状态 (Status):     ${STATUS}
  验证结果 (Result):     ${RESULT}

EOF
done

# ── Compare with pre-failover state (Scenario B) ────────────────────────────
BEFORE_FILE="${OUTPUT_DIR}/before_kill.json"
if [ -f "${BEFORE_FILE}" ]; then
  banner "Failover 对比 (Before vs After Kill)"

  BEFORE_QTY=$(python3 -c "import json; print(json.load(open('${BEFORE_FILE}'))['completed_qty'])")
  BEFORE_REPORTED=$(python3 -c "import json; print(json.load(open('${BEFORE_FILE}'))['total_reported_qty'])")
  BEFORE_STATUS=$(python3 -c "import json; print(json.load(open('${BEFORE_FILE}'))['status'])")
  BEFORE_WO_ID=$(python3 -c "import json; print(json.load(open('${BEFORE_FILE}'))['work_order_id'])")

  AFTER=$(curl -sf "${API}/api/work-orders/${BEFORE_WO_ID}/verify" 2>/dev/null)
  AFTER_QTY=$(echo "${AFTER}" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed_qty'])")
  AFTER_REPORTED=$(echo "${AFTER}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_reported_qty'])")
  AFTER_STATUS=$(echo "${AFTER}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")

  echo "  ┌────────────────────────────────────────────────┐"
  echo "  │  工单 #${BEFORE_WO_ID} Failover 对比             │"
  echo "  │                Before Kill    After Recovery    │"
  echo "  │  报工产量:     ${BEFORE_REPORTED}               ${AFTER_REPORTED}    │"
  echo "  │  完成产量:     ${BEFORE_QTY}               ${AFTER_QTY}    │"
  echo "  │  工单状态:     ${BEFORE_STATUS}       ${AFTER_STATUS}    │"
  echo "  └────────────────────────────────────────────────┘"

  if [ "${BEFORE_QTY}" = "${AFTER_QTY}" ] && \
     [ "${BEFORE_REPORTED}" = "${AFTER_REPORTED}" ] && \
     [ "${BEFORE_STATUS}" = "${AFTER_STATUS}" ]; then
    ok "Failover 验证通过: 宕机前后数据完全一致"
    echo "" >> "${REPORT}"
    echo "  Failover Comparison: IDENTICAL" >> "${REPORT}"
    echo "  reported_qty: ${BEFORE_REPORTED} → ${AFTER_REPORTED} (no change)" >> "${REPORT}"
    echo "  completed_qty: ${BEFORE_QTY} → ${AFTER_QTY} (no change)" >> "${REPORT}"
    echo "  status: ${BEFORE_STATUS} → ${AFTER_STATUS} (no change)" >> "${REPORT}"
  else
    fail "Failover 验证失败: 宕机前后数据不一致!"
    OVERALL_PASS=false
    echo "" >> "${REPORT}"
    echo "  Failover Comparison: MISMATCH" >> "${REPORT}"
  fi
fi

# ── Final result ─────────────────────────────────────────────────────────────
echo ""
echo "" >> "${REPORT}"
if [ "${OVERALL_PASS}" = true ]; then
  echo "  ══════════════════════════════════════════════" >> "${REPORT}"
  echo "  最终结果 (Final Result): PASS" >> "${REPORT}"
  echo "  所有生产事实在宕机后完整保留。" >> "${REPORT}"
  echo "  All production facts survived the crash." >> "${REPORT}"
  echo "  ══════════════════════════════════════════════" >> "${REPORT}"

  echo -e "  ${GREEN}${BOLD}╔═══════════════════════════════════════════════╗${NC}"
  echo -e "  ${GREEN}${BOLD}║  最终结果: PASS                               ║${NC}"
  echo -e "  ${GREEN}${BOLD}║  Final Result: PASS                           ║${NC}"
  echo -e "  ${GREEN}${BOLD}║                                               ║${NC}"
  echo -e "  ${GREEN}${BOLD}║  所有生产事实在宕机后完整保留。                ║${NC}"
  echo -e "  ${GREEN}${BOLD}║  All production facts survived the crash.      ║${NC}"
  echo -e "  ${GREEN}${BOLD}║                                               ║${NC}"
  echo -e "  ${GREEN}${BOLD}║  报工成功 = 永不丢失。                         ║${NC}"
  echo -e "  ${GREEN}${BOLD}║  Confirmed report = never lost.                ║${NC}"
  echo -e "  ${GREEN}${BOLD}╚═══════════════════════════════════════════════╝${NC}"
else
  echo "  最终结果 (Final Result): FAIL" >> "${REPORT}"
  echo -e "  ${RED}${BOLD}FAIL — Business state mismatch after failover${NC}"
fi

echo ""
ok "Full report: ${REPORT}"
echo ""
