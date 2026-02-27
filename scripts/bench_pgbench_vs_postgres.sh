#!/usr/bin/env bash
# ============================================================================
# FalconDB vs PostgreSQL — pgbench Comparison Benchmark
# ============================================================================
# Produces a reproducible report under bench_out/<timestamp>/
#
# Usage:
#   ./scripts/bench_pgbench_vs_postgres.sh
#   SCALE=100 CONCURRENCY=64 DURATION_SEC=120 ./scripts/bench_pgbench_vs_postgres.sh
#   MODE=read_only RUNS=5 ./scripts/bench_pgbench_vs_postgres.sh
#
# Environment variables (all optional, sensible defaults provided):
#   FALCON_HOST, FALCON_PORT, FALCON_DB, FALCON_USER
#   PG_HOST, PG_PORT, PG_DB, PG_USER
#   CONCURRENCY, JOBS, DURATION_SEC, SCALE, WARMUP_SEC, RUNS
#   MODE = tpcb_like | simple_update | read_only | custom
#   FALCON_BIN   — path to FalconDB server binary
#   FALCON_CONF  — path to FalconDB config file
#   SKIP_FALCON  — set to 1 to skip FalconDB runs
#   SKIP_PG      — set to 1 to skip PostgreSQL runs
# ============================================================================
set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────────
FALCON_HOST="${FALCON_HOST:-127.0.0.1}"
FALCON_PORT="${FALCON_PORT:-5433}"
FALCON_DB="${FALCON_DB:-falcon}"
FALCON_USER="${FALCON_USER:-falcon}"
FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
FALCON_CONF="${FALCON_CONF:-bench_configs/falcon.bench.toml}"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-pgbench}"
PG_USER="${PG_USER:-postgres}"

CONCURRENCY="${CONCURRENCY:-32}"
JOBS="${JOBS:-$CONCURRENCY}"
DURATION_SEC="${DURATION_SEC:-60}"
SCALE="${SCALE:-50}"
WARMUP_SEC="${WARMUP_SEC:-15}"
RUNS="${RUNS:-3}"
MODE="${MODE:-tpcb_like}"

SKIP_FALCON="${SKIP_FALCON:-0}"
SKIP_PG="${SKIP_PG:-0}"

TS=$(date -u +"%Y%m%dT%H%M%SZ")
OUT_DIR="bench_out/${TS}"

# ── Dependency check ───────────────────────────────────────────────────────
check_cmd() {
  if ! command -v "$1" &>/dev/null; then
    echo "ERROR: required command '$1' not found. $2" >&2
    exit 1
  fi
}

check_cmd pgbench "Install PostgreSQL client tools."
check_cmd psql    "Install PostgreSQL client tools."

# ── Mode flag ──────────────────────────────────────────────────────────────
pgbench_mode_flag() {
  case "${MODE}" in
    tpcb_like)     echo "" ;;            # default pgbench TPC-B like
    simple_update) echo "-N" ;;          # skip vacuum
    read_only)     echo "-S" ;;          # SELECT-only
    custom)        echo "-f bench_configs/custom_pgbench.sql" ;;
    *)             echo "ERROR: unknown MODE=${MODE}" >&2; exit 1 ;;
  esac
}

MODE_FLAG=$(pgbench_mode_flag)

# ── Output directory ───────────────────────────────────────────────────────
mkdir -p "${OUT_DIR}/pgbench/falcon" "${OUT_DIR}/pgbench/postgres"

echo "============================================================"
echo "FalconDB vs PostgreSQL — pgbench Benchmark"
echo "============================================================"
echo "Timestamp : ${TS}"
echo "Output    : ${OUT_DIR}/"
echo "Mode      : ${MODE}"
echo "Scale     : ${SCALE}"
echo "Concurrency: ${CONCURRENCY}  Jobs: ${JOBS}"
echo "Duration  : ${DURATION_SEC}s  Warmup: ${WARMUP_SEC}s  Runs: ${RUNS}"
echo "============================================================"

# ── Environment snapshot ──────────────────────────────────────────────────
collect_env_snapshot() {
  local envfile="${OUT_DIR}/environment.txt"
  {
    echo "=== Environment Snapshot ==="
    echo "Timestamp: ${TS}"
    echo "Git commit: $(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
    echo "Git branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')"
    echo ""
    echo "=== OS ==="
    uname -a 2>/dev/null || echo "uname unavailable"
    echo ""
    echo "=== CPU ==="
    if command -v lscpu &>/dev/null; then
      lscpu 2>/dev/null | head -20
    elif command -v sysctl &>/dev/null; then
      sysctl -n machdep.cpu.brand_string 2>/dev/null || true
      sysctl -n hw.ncpu 2>/dev/null || true
    else
      echo "CPU info unavailable"
    fi
    echo ""
    echo "=== Memory ==="
    if command -v free &>/dev/null; then
      free -h 2>/dev/null
    elif command -v sysctl &>/dev/null; then
      sysctl -n hw.memsize 2>/dev/null || true
    else
      echo "Memory info unavailable"
    fi
    echo ""
    echo "=== Disk ==="
    df -h . 2>/dev/null || echo "Disk info unavailable"
    echo ""
    echo "=== FalconDB Config ==="
    if [ -f "${FALCON_CONF}" ]; then
      cat "${FALCON_CONF}"
    else
      echo "(config file not found: ${FALCON_CONF})"
    fi
  } > "${envfile}"
  echo "  Environment snapshot saved to ${envfile}"
}

collect_env_snapshot

# ── Save config copy ──────────────────────────────────────────────────────
if [ -f "${FALCON_CONF}" ]; then
  cp "${FALCON_CONF}" "${OUT_DIR}/falcon_config_used.toml"
fi

# ── Parse pgbench output ──────────────────────────────────────────────────
# Extracts TPS (including connections), TPS (excluding), latency avg
parse_pgbench_log() {
  local logfile="$1"
  local tps_incl tps_excl lat_avg

  tps_incl=$(grep -oP 'tps = \K[0-9.]+(?= \(including)' "$logfile" 2>/dev/null || echo "0")
  tps_excl=$(grep -oP 'tps = \K[0-9.]+(?= \(excluding)' "$logfile" 2>/dev/null || echo "0")
  lat_avg=$(grep -oP 'latency average = \K[0-9.]+' "$logfile" 2>/dev/null || echo "0")

  echo "${tps_incl}|${tps_excl}|${lat_avg}"
}

# ── Init database for a target ────────────────────────────────────────────
init_db() {
  local label="$1" host="$2" port="$3" db="$4" user="$5"
  echo "  Initializing pgbench schema on ${label} (${host}:${port}/${db})..."

  # Create database if it doesn't exist (ignore errors)
  psql -h "$host" -p "$port" -U "$user" -d postgres \
    -c "CREATE DATABASE ${db};" 2>/dev/null || true

  pgbench -h "$host" -p "$port" -U "$user" -d "$db" \
    -i -s "${SCALE}" --no-vacuum 2>&1 | tee "${OUT_DIR}/pgbench/${label}/init.log"
  echo "  ${label} init complete (scale=${SCALE})."
}

# ── Run pgbench against a target ──────────────────────────────────────────
run_pgbench_target() {
  local label="$1" host="$2" port="$3" db="$4" user="$5"
  local target_dir="${OUT_DIR}/pgbench/${label}"

  echo ""
  echo "── ${label}: warm-up (${WARMUP_SEC}s) ──"
  local warmup_cmd="pgbench -h ${host} -p ${port} -U ${user} -d ${db} \
    -c ${CONCURRENCY} -j ${JOBS} -T ${WARMUP_SEC} ${MODE_FLAG}"
  echo "  CMD: ${warmup_cmd}"
  eval "${warmup_cmd}" > "${target_dir}/warmup.log" 2>&1 || true

  local run_results=()
  for i in $(seq 1 "${RUNS}"); do
    echo "── ${label}: run ${i}/${RUNS} (${DURATION_SEC}s) ──"
    local run_cmd="pgbench -h ${host} -p ${port} -U ${user} -d ${db} \
      -c ${CONCURRENCY} -j ${JOBS} -T ${DURATION_SEC} ${MODE_FLAG}"
    echo "  CMD: ${run_cmd}"
    eval "${run_cmd}" > "${target_dir}/run${i}.log" 2>&1

    local parsed
    parsed=$(parse_pgbench_log "${target_dir}/run${i}.log")
    run_results+=("${parsed}")

    local tps_incl tps_excl lat_avg
    IFS='|' read -r tps_incl tps_excl lat_avg <<< "${parsed}"
    echo "  Run ${i}: TPS(incl)=${tps_incl}  TPS(excl)=${tps_excl}  Latency=${lat_avg}ms"
  done

  # Write per-target results
  {
    echo "target=${label}"
    for i in $(seq 0 $(( RUNS - 1 ))); do
      echo "run$((i+1))=${run_results[$i]}"
    done
  } > "${target_dir}/results.txt"
}

# ── Compute mean of pipe-delimited values ─────────────────────────────────
compute_summary() {
  local target_dir="$1" label="$2"
  local sum_tps_incl=0 sum_tps_excl=0 sum_lat=0 count=0

  for f in "${target_dir}"/run*.log; do
    local parsed
    parsed=$(parse_pgbench_log "$f")
    IFS='|' read -r ti te la <<< "${parsed}"
    sum_tps_incl=$(echo "${sum_tps_incl} + ${ti}" | bc -l 2>/dev/null || echo "0")
    sum_tps_excl=$(echo "${sum_tps_excl} + ${te}" | bc -l 2>/dev/null || echo "0")
    sum_lat=$(echo "${sum_lat} + ${la}" | bc -l 2>/dev/null || echo "0")
    count=$((count + 1))
  done

  if [ "${count}" -gt 0 ]; then
    local avg_tps_incl avg_tps_excl avg_lat
    avg_tps_incl=$(echo "scale=2; ${sum_tps_incl} / ${count}" | bc -l 2>/dev/null || echo "0")
    avg_tps_excl=$(echo "scale=2; ${sum_tps_excl} / ${count}" | bc -l 2>/dev/null || echo "0")
    avg_lat=$(echo "scale=2; ${sum_lat} / ${count}" | bc -l 2>/dev/null || echo "0")
    echo "${label}|${avg_tps_incl}|${avg_tps_excl}|${avg_lat}|${count}"
  else
    echo "${label}|0|0|0|0"
  fi
}

# ── Generate JSON summary ─────────────────────────────────────────────────
generate_json_summary() {
  local json_file="${OUT_DIR}/summary_pgbench.json"
  local falcon_summary="" pg_summary=""

  if [ "${SKIP_FALCON}" != "1" ]; then
    falcon_summary=$(compute_summary "${OUT_DIR}/pgbench/falcon" "falcon")
  fi
  if [ "${SKIP_PG}" != "1" ]; then
    pg_summary=$(compute_summary "${OUT_DIR}/pgbench/postgres" "postgres")
  fi

  {
    echo "{"
    echo "  \"timestamp\": \"${TS}\","
    echo "  \"git_commit\": \"$(git rev-parse HEAD 2>/dev/null || echo 'unknown')\","
    echo "  \"mode\": \"${MODE}\","
    echo "  \"scale\": ${SCALE},"
    echo "  \"concurrency\": ${CONCURRENCY},"
    echo "  \"duration_sec\": ${DURATION_SEC},"
    echo "  \"warmup_sec\": ${WARMUP_SEC},"
    echo "  \"runs\": ${RUNS},"
    echo "  \"results\": {"

    local first=true
    for entry in ${falcon_summary} ${pg_summary}; do
      [ -z "${entry}" ] && continue
      IFS='|' read -r lbl ti te la cnt <<< "${entry}"
      [ "${first}" = "true" ] && first=false || echo "    ,"
      cat <<ENTRY
    "${lbl}": {
      "tps_including_connections": ${ti},
      "tps_excluding_connections": ${te},
      "latency_avg_ms": ${la},
      "run_count": ${cnt}
    }
ENTRY
    done

    echo "  }"
    echo "}"
  } > "${json_file}"
  echo "  JSON summary: ${json_file}"
}

# ── Generate Markdown report ──────────────────────────────────────────────
generate_report() {
  local report="${OUT_DIR}/REPORT.md"
  local falcon_summary="" pg_summary=""

  if [ "${SKIP_FALCON}" != "1" ]; then
    falcon_summary=$(compute_summary "${OUT_DIR}/pgbench/falcon" "FalconDB")
  fi
  if [ "${SKIP_PG}" != "1" ]; then
    pg_summary=$(compute_summary "${OUT_DIR}/pgbench/postgres" "PostgreSQL")
  fi

  {
    cat <<HEADER
# pgbench Benchmark Report

**Date**: ${TS}
**Git Commit**: $(git rev-parse HEAD 2>/dev/null || echo 'unknown')

## Parameters

| Parameter    | Value           |
|-------------|-----------------|
| Mode        | ${MODE}          |
| Scale       | ${SCALE}         |
| Concurrency | ${CONCURRENCY}   |
| Jobs        | ${JOBS}          |
| Duration    | ${DURATION_SEC}s |
| Warmup      | ${WARMUP_SEC}s   |
| Runs        | ${RUNS}          |

## Results

| Target     | TPS (incl conn) | TPS (excl conn) | Latency avg (ms) | Runs |
|-----------|-----------------|-----------------|-------------------|------|
HEADER

    for entry in ${falcon_summary} ${pg_summary}; do
      [ -z "${entry}" ] && continue
      IFS='|' read -r lbl ti te la cnt <<< "${entry}"
      printf "| %-9s | %15s | %15s | %17s | %4s |\n" "${lbl}" "${ti}" "${te}" "${la}" "${cnt}"
    done

    echo ""
    echo "## Commands Used"
    echo ""
    echo '```bash'
    echo "# Init"
    [ "${SKIP_FALCON}" != "1" ] && \
      echo "pgbench -h ${FALCON_HOST} -p ${FALCON_PORT} -U ${FALCON_USER} -d ${FALCON_DB} -i -s ${SCALE} --no-vacuum"
    [ "${SKIP_PG}" != "1" ] && \
      echo "pgbench -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USER} -d ${PG_DB} -i -s ${SCALE} --no-vacuum"
    echo ""
    echo "# Benchmark"
    [ "${SKIP_FALCON}" != "1" ] && \
      echo "pgbench -h ${FALCON_HOST} -p ${FALCON_PORT} -U ${FALCON_USER} -d ${FALCON_DB} -c ${CONCURRENCY} -j ${JOBS} -T ${DURATION_SEC} ${MODE_FLAG}"
    [ "${SKIP_PG}" != "1" ] && \
      echo "pgbench -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USER} -d ${PG_DB} -c ${CONCURRENCY} -j ${JOBS} -T ${DURATION_SEC} ${MODE_FLAG}"
    echo '```'

    echo ""
    echo "## Environment"
    echo ""
    echo '```'
    cat "${OUT_DIR}/environment.txt" 2>/dev/null || echo "(environment snapshot not available)"
    echo '```'

    echo ""
    echo "## Notes"
    echo ""
    echo "- FalconDB authentication mode: TRUST (no password required)"
    echo "- WAL enabled with fdatasync durability"
    echo "- Results are not official TPC-B/TPC-C compliant"
    echo "- See docs/benchmark_methodology.md for reproducibility rules"

  } > "${report}"
  echo "  Report: ${report}"
}

# ── Main ──────────────────────────────────────────────────────────────────

# Init + run FalconDB
if [ "${SKIP_FALCON}" != "1" ]; then
  echo ""
  echo "=== FalconDB Target ==="
  init_db "falcon" "${FALCON_HOST}" "${FALCON_PORT}" "${FALCON_DB}" "${FALCON_USER}"
  run_pgbench_target "falcon" "${FALCON_HOST}" "${FALCON_PORT}" "${FALCON_DB}" "${FALCON_USER}"
fi

# Init + run PostgreSQL
if [ "${SKIP_PG}" != "1" ]; then
  echo ""
  echo "=== PostgreSQL Target ==="
  init_db "postgres" "${PG_HOST}" "${PG_PORT}" "${PG_DB}" "${PG_USER}"
  run_pgbench_target "postgres" "${PG_HOST}" "${PG_PORT}" "${PG_DB}" "${PG_USER}"
fi

# Generate outputs
echo ""
echo "=== Generating Report ==="
generate_json_summary
generate_report

echo ""
echo "============================================================"
echo "DONE — Results in: ${OUT_DIR}/"
echo "  REPORT.md             — Human-readable report"
echo "  summary_pgbench.json  — Machine-readable metrics"
echo "  pgbench/*/run*.log    — Raw pgbench output"
echo "  environment.txt       — System snapshot"
echo "============================================================"
