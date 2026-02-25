#!/usr/bin/env bash
# ============================================================================
# FalconDB Benchmark Matrix — 4 Engines × 5 Workloads
# ============================================================================
#
# Runs all workloads against FalconDB, PostgreSQL, VoltDB, and SingleStore,
# then generates a comparison matrix report.
#
# Usage:
#   ./benchmarks/scripts/run_matrix.sh                         # full (60s)
#   ./benchmarks/scripts/run_matrix.sh --quick                 # quick (10s)
#   ./benchmarks/scripts/run_matrix.sh --engines falcondb,pg   # subset
#   ./benchmarks/scripts/run_matrix.sh --workloads w1,w3       # subset
#
# Prerequisites (install only engines you want to benchmark):
#   - FalconDB: cargo build --release -p falcon_server
#   - PostgreSQL: pg_ctl, pgbench, psql on PATH
#   - VoltDB: VOLTDB_HOME set, sqlcmd on PATH
#   - SingleStore: Docker or native install
#
# Engines not installed are automatically skipped.
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_DIR="$REPO_ROOT/benchmarks"
SCRIPTS_DIR="$BENCH_DIR/scripts"
RESULTS_DIR="$BENCH_DIR/results"

# ── Parse arguments ──
DURATION=60
ALL_ENGINES="falcondb,postgresql,voltdb,singlestore"
ALL_WORKLOADS="w1,w2,w3,w4,w5"
SELECTED_ENGINES="$ALL_ENGINES"
SELECTED_WORKLOADS="$ALL_WORKLOADS"
THREAD_COUNTS="1,4,8,16"

while [ $# -gt 0 ]; do
    case "$1" in
        --quick)       DURATION=10;               shift ;;
        --engines)     SELECTED_ENGINES="$2";     shift 2 ;;
        --workloads)   SELECTED_WORKLOADS="$2";   shift 2 ;;
        --threads)     THREAD_COUNTS="$2";        shift 2 ;;
        --duration)    DURATION="$2";             shift 2 ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: run_matrix.sh [--quick] [--engines e1,e2] [--workloads w1,w2] [--threads 1,4,8] [--duration 60]"
            exit 1
            ;;
    esac
done

IFS=',' read -ra ENGINES <<< "$SELECTED_ENGINES"
IFS=',' read -ra WORKLOADS <<< "$SELECTED_WORKLOADS"
IFS=',' read -ra THREADS <<< "$THREAD_COUNTS"

TIMESTAMP=$(date +%Y%m%dT%H%M%S)
MATRIX_DIR="$RESULTS_DIR/matrix_${TIMESTAMP}"
mkdir -p "$MATRIX_DIR"

TOTAL_RUNS=$(( ${#ENGINES[@]} * ${#WORKLOADS[@]} * ${#THREADS[@]} ))

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  FalconDB Benchmark Matrix — Public Reproducible Comparison    ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Engines:    ${SELECTED_ENGINES}"
echo "║  Workloads:  ${SELECTED_WORKLOADS}"
echo "║  Threads:    ${THREAD_COUNTS}"
echo "║  Duration:   ${DURATION}s per run"
echo "║  Total runs: ${TOTAL_RUNS}"
echo "║  Output:     ${MATRIX_DIR}"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# ── Capture environment ──
ENV_FILE="$MATRIX_DIR/environment.txt"
{
    echo "=== System ==="
    uname -a 2>/dev/null || echo "OS: $(cmd /c ver 2>/dev/null || echo unknown)"
    echo ""
    echo "=== CPU ==="
    nproc 2>/dev/null || echo "CPU cores: $(wmic cpu get NumberOfCores 2>/dev/null || echo unknown)"
    lscpu 2>/dev/null | head -20 || true
    echo ""
    echo "=== Memory ==="
    free -h 2>/dev/null || echo "Memory: unknown"
    echo ""
    echo "=== Benchmark parameters ==="
    echo "Timestamp: $TIMESTAMP"
    echo "Duration:  ${DURATION}s"
    echo "Engines:   ${SELECTED_ENGINES}"
    echo "Workloads: ${SELECTED_WORKLOADS}"
    echo "Threads:   ${THREAD_COUNTS}"
    echo ""
    echo "=== Engine versions ==="
    "$REPO_ROOT/target/release/falcon_server" version 2>/dev/null || echo "FalconDB: not built"
    psql --version 2>/dev/null || echo "PostgreSQL: not installed"
    echo "VoltDB: $($VOLTDB_HOME/bin/voltdb --version 2>/dev/null || echo 'not installed')"
    echo "SingleStore: $(docker exec falcondb-bench-singlestore memsql --version 2>/dev/null || echo 'not installed/running')"
} > "$ENV_FILE" 2>&1
echo "Environment captured: $ENV_FILE"
echo ""

# ── Engine availability check ──
check_engine() {
    local engine="$1"
    case "$engine" in
        falcondb)
            [ -x "$REPO_ROOT/target/release/falcon_server" ] && return 0
            echo "SKIP: FalconDB not built (run: cargo build --release -p falcon_server)"
            return 1
            ;;
        postgresql)
            command -v psql >/dev/null 2>&1 && command -v pgbench >/dev/null 2>&1 && return 0
            echo "SKIP: PostgreSQL not installed (need psql + pgbench)"
            return 1
            ;;
        voltdb)
            [ -n "${VOLTDB_HOME:-}" ] && [ -x "${VOLTDB_HOME}/bin/voltdb" ] && return 0
            echo "SKIP: VoltDB not installed (set VOLTDB_HOME)"
            return 1
            ;;
        singlestore)
            docker ps >/dev/null 2>&1 && return 0
            command -v singlestore >/dev/null 2>&1 && return 0
            echo "SKIP: SingleStore not available (need Docker or native install)"
            return 1
            ;;
    esac
}

# ── Connection params per engine ──
engine_conn() {
    local engine="$1"
    case "$engine" in
        falcondb)    echo "127.0.0.1 5443 falcon falcon pgwire" ;;
        postgresql)  echo "127.0.0.1 5432 $(whoami) falconbench pgwire" ;;
        voltdb)      echo "127.0.0.1 5444 $(whoami) falconbench pgwire" ;;
        singlestore) echo "127.0.0.1 ${SINGLESTORE_PORT:-3306} root falconbench mysql" ;;
    esac
}

# ── Start engines ──
STARTED_ENGINES=()
cleanup() {
    echo ""
    echo "Cleaning up started engines..."
    for e in "${STARTED_ENGINES[@]}"; do
        case "$e" in
            falcondb)
                [ -f "$BENCH_DIR/falcondb/falcon.pid" ] && kill "$(cat "$BENCH_DIR/falcondb/falcon.pid")" 2>/dev/null || true
                ;;
            postgresql)
                pg_ctl -D "$BENCH_DIR/postgresql/pg_data" stop 2>/dev/null || true
                ;;
            voltdb)
                "${VOLTDB_HOME:-/opt/voltdb}/bin/voltadmin" shutdown 2>/dev/null || true
                ;;
            singlestore)
                docker rm -f falcondb-bench-singlestore 2>/dev/null || true
                ;;
        esac
    done
}
trap cleanup EXIT

for engine in "${ENGINES[@]}"; do
    if ! check_engine "$engine"; then
        continue
    fi

    echo "Starting $engine..."
    if [ -x "$BENCH_DIR/$engine/run.sh" ]; then
        chmod +x "$BENCH_DIR/$engine/run.sh"
        if "$BENCH_DIR/$engine/run.sh"; then
            STARTED_ENGINES+=("$engine")
            echo "  ✓ $engine started"
        else
            echo "  ✗ $engine failed to start — skipping"
        fi
    else
        echo "  WARN: No run.sh for $engine — assuming already running"
        STARTED_ENGINES+=("$engine")
    fi
    echo ""
done

if [ ${#STARTED_ENGINES[@]} -eq 0 ]; then
    echo "ERROR: No engines available. Install at least one."
    exit 1
fi

echo "Available engines: ${STARTED_ENGINES[*]}"
echo ""

# ── Run benchmark matrix ──
RUN_COUNT=0

run_one() {
    local engine="$1" workload="$2" threads="$3"
    RUN_COUNT=$((RUN_COUNT + 1))

    read -r host port user db protocol <<< "$(engine_conn "$engine")"
    local outfile="$MATRIX_DIR/${engine}_${workload}_t${threads}.txt"

    echo "[$RUN_COUNT/$TOTAL_RUNS] $engine / $workload / ${threads}T / ${DURATION}s"

    # Setup schema (per-workload)
    local setup_sql="$BENCH_DIR/workloads/setup.sql"
    if [ "$workload" = "w5" ]; then
        setup_sql="$BENCH_DIR/workloads/setup_batch.sql"
    fi

    # Engine-specific schema setup
    case "$engine" in
        voltdb)
            setup_sql="$BENCH_DIR/workloads/setup_voltdb.sql"
            ;;
        singlestore)
            setup_sql="$BENCH_DIR/workloads/setup_singlestore.sql"
            ;;
    esac

    # Load schema
    case "$protocol" in
        pgwire)
            psql -h "$host" -p "$port" -U "$user" -d "$db" -f "$setup_sql" > /dev/null 2>&1 || true
            ;;
        mysql)
            mysql -h "$host" -P "$port" -u "$user" -p"${SINGLESTORE_PASSWORD:-bench_password}" "$db" < "$setup_sql" 2>/dev/null || true
            ;;
    esac

    # Build pgbench script
    local pgbench_script
    pgbench_script=$(mktemp)

    case "$workload" in
        w1)
            cat > "$pgbench_script" <<'PGEOF'
\set random_id random(1, 100000)
\set delta random(-100, 100)
\set op random(1, 100)
\if :op <= 70
SELECT id, balance, name FROM accounts WHERE id = :random_id;
\elif :op <= 90
UPDATE accounts SET balance = balance + :delta WHERE id = :random_id;
\else
\set new_id random(200001, 999999)
INSERT INTO accounts (id, balance, name) VALUES (:new_id, :random_id, 'bench') ON CONFLICT DO NOTHING;
\endif
PGEOF
            ;;
        w2)
            cat > "$pgbench_script" <<'PGEOF'
\set from_id random(1, 100000)
\set to_id random(1, 100000)
\set amount random(1, 100)
BEGIN;
SELECT balance FROM accounts WHERE id = :from_id;
INSERT INTO transfers (from_id, to_id, amount) VALUES (:from_id, :to_id, :amount);
UPDATE accounts SET balance = balance - :amount WHERE id = :from_id;
UPDATE accounts SET balance = balance + :amount WHERE id = :to_id;
COMMIT;
PGEOF
            ;;
        w3)
            cat > "$pgbench_script" <<'PGEOF'
\set lo random(1, 90000)
\set hi :lo + 10000
SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance)
  FROM accounts
 WHERE id BETWEEN :lo AND :hi;
PGEOF
            ;;
        w4)
            cat > "$pgbench_script" <<'PGEOF'
\set hot_id random(1, 10)
\set delta random(-50, 50)
UPDATE accounts SET balance = balance + :delta WHERE id = :hot_id;
PGEOF
            ;;
        w5)
            cat > "$pgbench_script" <<'PGEOF'
\set seq_id random(1, 2000000000)
\set amount random(1, 10000)
INSERT INTO bench_events (id, account_id, event_type, amount)
VALUES (:seq_id, random(1, 100000), 'transfer', :amount);
PGEOF
            ;;
    esac

    # Run via pgbench (works for PG-wire engines) or sysbench/mysql
    case "$protocol" in
        pgwire)
            pgbench -h "$host" -p "$port" -U "$user" -d "$db" \
                -f "$pgbench_script" \
                -c "$threads" -j "$threads" -T "$DURATION" \
                --progress=10 > "$outfile" 2>&1 || {
                echo "  WARN: $engine/$workload/t$threads pgbench failed"
                echo "FAILED" > "$outfile"
            }
            ;;
        mysql)
            # For MySQL-wire engines, use mysqlslap or pgbench with a wrapper
            # pgbench can connect to MySQL via PG-wire compatibility layer if available
            # Fallback: use mysql client in a loop with time measurement
            {
                echo "=== mysqlslap: $engine / $workload / ${threads}T / ${DURATION}s ==="
                local start_time end_time elapsed_s
                start_time=$(date +%s%N)

                # Use timeout to limit duration
                timeout "${DURATION}" bash -c "
                    count=0
                    while true; do
                        mysql -h $host -P $port -u $user -p${SINGLESTORE_PASSWORD:-bench_password} $db \
                            -e \"$(cat "$pgbench_script" | sed 's/\\\\set.*//g; s/:random_id/FLOOR(1+RAND()*100000)/g; s/:delta/FLOOR(-100+RAND()*200)/g; s/:lo/FLOOR(1+RAND()*90000)/g; s/:hi/:lo+10000/g; s/:hot_id/FLOOR(1+RAND()*10)/g; s/:amount/FLOOR(1+RAND()*10000)/g; s/:seq_id/FLOOR(1+RAND()*2000000000)/g; /^$/d')\" \
                            2>/dev/null
                        count=\$((count + 1))
                    done
                    echo \"total_transactions: \$count\"
                " || true

                end_time=$(date +%s%N)
                elapsed_s=$(( (end_time - start_time) / 1000000000 ))
                echo "elapsed: ${elapsed_s}s"
            } > "$outfile" 2>&1 || {
                echo "  WARN: $engine/$workload/t$threads mysql failed"
                echo "FAILED" > "$outfile"
            }
            ;;
    esac

    rm -f "$pgbench_script"

    # Extract key metrics
    if [ -f "$outfile" ] && ! grep -q "FAILED" "$outfile"; then
        local tps latency_avg
        tps=$(grep -oP 'tps = \K[0-9.]+' "$outfile" 2>/dev/null | tail -1 || echo "—")
        latency_avg=$(grep -oP 'latency average = \K[0-9.]+' "$outfile" 2>/dev/null || echo "—")
        echo "  → TPS: $tps  Avg latency: ${latency_avg}ms"
    else
        echo "  → FAILED or no data"
    fi
}

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Running benchmark matrix..."
echo "═══════════════════════════════════════════════════════════════════"

for workload in "${WORKLOADS[@]}"; do
    for threads in "${THREADS[@]}"; do
        for engine in "${STARTED_ENGINES[@]}"; do
            run_one "$engine" "$workload" "$threads"
        done
    done
done

# ── Generate matrix report ──
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Generating comparison matrix..."
echo "═══════════════════════════════════════════════════════════════════"

REPORT="$MATRIX_DIR/MATRIX_REPORT.md"
{
    echo "# FalconDB Benchmark Matrix — 4-Engine Comparison"
    echo ""
    echo "> **Generated**: $TIMESTAMP"
    echo "> **Duration**: ${DURATION}s per run"
    echo "> **Engines**: ${STARTED_ENGINES[*]}"
    echo "> **Environment**: see environment.txt"
    echo ""

    for workload in "${WORKLOADS[@]}"; do
        local wname
        case "$workload" in
            w1) wname="W1: Single-Table OLTP (70% SELECT, 20% UPDATE, 10% INSERT)" ;;
            w2) wname="W2: Multi-Table Transaction (BEGIN→SELECT→INSERT→UPDATE×2→COMMIT)" ;;
            w3) wname="W3: Analytic Range Scan (COUNT/SUM/AVG over 10% of rows)" ;;
            w4) wname="W4: Hot-Key Contention (10 hot keys, all threads competing)" ;;
            w5) wname="W5: Batch Insert Throughput (single-row INSERT into append table)" ;;
            *)  wname="$workload" ;;
        esac

        echo "## $wname"
        echo ""

        # Header row
        printf "| Threads |"
        for engine in "${STARTED_ENGINES[@]}"; do
            printf " %s TPS | %s p99 (ms) |" "$engine" "$engine"
        done
        echo ""

        # Separator
        printf "|---------|"
        for engine in "${STARTED_ENGINES[@]}"; do
            printf "------------|-------------|"
        done
        echo ""

        for threads in "${THREADS[@]}"; do
            printf "| %-7s |" "$threads"
            for engine in "${STARTED_ENGINES[@]}"; do
                local outfile="$MATRIX_DIR/${engine}_${workload}_t${threads}.txt"
                local tps="—" p99="—"
                if [ -f "$outfile" ] && ! grep -q "FAILED" "$outfile"; then
                    tps=$(grep -oP 'tps = \K[0-9.]+' "$outfile" 2>/dev/null | tail -1 || echo "—")
                    p99=$(grep -oP 'lat.*99th.*= \K[0-9.]+' "$outfile" 2>/dev/null || echo "—")
                fi
                printf " %10s | %11s |" "$tps" "$p99"
            done
            echo ""
        done
        echo ""
    done

    echo "## Notes"
    echo ""
    echo "- All engines run on the **same machine** with identical memory allocation (2 GB)"
    echo "- Measurement uses pgbench (PG-wire) for FalconDB, PostgreSQL, and VoltDB"
    echo "- SingleStore uses MySQL-wire client"
    echo "- '—' indicates the engine was not available or the run failed"
    echo "- See \`environment.txt\` for hardware and engine version details"
    echo "- Full raw output is in each \`<engine>_<workload>_t<threads>.txt\` file"

} > "$REPORT"

echo ""
cat "$REPORT"

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Benchmark matrix complete!                                    ║"
echo "║  Results: $MATRIX_DIR"
echo "║  Report:  $REPORT"
echo "╚══════════════════════════════════════════════════════════════════╝"
