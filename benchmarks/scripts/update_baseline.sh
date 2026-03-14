#!/usr/bin/env bash
# update_baseline.sh
# ─────────────────────────────────────────────────────────────────────────────
# Update the main benchmark baseline after a performance-improving PR is merged.
#
# Usage:
#   bash benchmarks/scripts/update_baseline.sh [--bench <bench_filter>] [--dry-run]
#
# Steps:
#   1. Build release binary
#   2. Run the CI smoke suite (or full suite if --full)
#   3. Validate JSON output
#   4. Copy to benchmarks/baselines/main.json
#   5. Create a git-annotated tag with the new baseline summary
#
# [skip bench] in commit message prevents the bench CI from re-running.

set -euo pipefail

BASELINE_FILE="benchmarks/baselines/main.json"
BENCH_RESULTS="bench_results_$(date +%Y%m%d_%H%M%S).json"
DRY_RUN=false
FULL=false
BENCH_FILTER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        --full)    FULL=true;    shift ;;
        --bench)   BENCH_FILTER="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

echo "═══════════════════════════════════════════════════"
echo "  FalconDB Baseline Update"
echo "  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "═══════════════════════════════════════════════════"

# 1. Build
echo "→ Building release..."
cargo build --release -p falcon_server -p falcon_bench 2>&1 | tail -5

# 2. Run benchmarks
if $FULL; then
    echo "→ Running full benchmark suite..."
    BENCH_CMD="cargo bench -p falcon_bench -- --output-format json"
else
    echo "→ Running CI smoke suite..."
    BENCH_CMD="cargo run -p falcon_bench -- --export json --ops 50000 --threads 32"
fi

if [[ -n "$BENCH_FILTER" ]]; then
    BENCH_CMD="$BENCH_CMD $BENCH_FILTER"
fi

GIT_COMMIT=$(git rev-parse --short HEAD)
MACHINE=$(hostname)
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

eval "$BENCH_CMD" | python3 - <<'EOF' > "$BENCH_RESULTS"
import json, sys, datetime, socket, subprocess

raw = sys.stdin.read().strip()
try:
    data = json.loads(raw)
except json.JSONDecodeError:
    # Wrap plain text output in minimal JSON structure
    data = {"results": {}}

commit = subprocess.check_output(["git","rev-parse","--short","HEAD"],
                                  text=True).strip()
data.setdefault("run_id",     commit + "_" + datetime.datetime.utcnow().strftime("%Y%m%d"))
data.setdefault("git_commit", commit)
data.setdefault("timestamp",  datetime.datetime.utcnow().isoformat() + "Z")
data.setdefault("machine",    socket.gethostname())

print(json.dumps(data, indent=2))
EOF

echo "→ Results written to $BENCH_RESULTS"

# 3. Validate
python3 -c "
import json, sys
with open('$BENCH_RESULTS') as f:
    data = json.load(f)
n = len(data.get('results', {}))
print(f'  Validated: {n} bench result(s)')
if n == 0:
    print('[WARN] Zero results — check bench output', file=sys.stderr)
"

if $DRY_RUN; then
    echo "→ [DRY RUN] Would copy $BENCH_RESULTS → $BASELINE_FILE"
    echo "→ [DRY RUN] Would commit baseline update"
    exit 0
fi

# 4. Copy to baseline
cp "$BENCH_RESULTS" "$BASELINE_FILE"
echo "→ Updated $BASELINE_FILE"

# 5. Git commit
git add "$BASELINE_FILE"
git commit -m "perf: update benchmark baseline $(date +%Y-%m-%d) [skip bench]

Machine: $MACHINE
Commit:  $GIT_COMMIT
Updated: $TIMESTAMP

$(python3 -c "
import json
with open('$BASELINE_FILE') as f:
    data = json.load(f)
results = data.get('results', {})
lines = []
for bid, m in sorted(results.items()):
    tps = m.get('tps', 0)
    p99 = m.get('latency_p99_us', 0)
    lines.append(f'  {bid}: tps={tps:.0f} p99={p99:.0f}us')
print('\n'.join(lines[:10]))
if len(results) > 10:
    print(f'  ... and {len(results)-10} more')
")" || echo "→ Nothing to commit (baseline unchanged)"

echo "✅ Baseline update complete."
