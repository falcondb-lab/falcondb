# ============================================================================
# FalconDB — One-click standalone demo (Windows PowerShell)
# ============================================================================
# Prerequisites: Rust 1.75+, psql (PostgreSQL client in PATH)
#
# Usage:  .\scripts\demo_standalone.ps1
# ============================================================================
$ErrorActionPreference = "Stop"

function Info($msg)  { Write-Host "[demo] $msg" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "[  ok] $msg" -ForegroundColor Green }

$DATA_DIR = Join-Path $env:TEMP "falcondb_demo_$(Get-Random)"
$PG_PORT  = 5433

# ── 1. Build ────────────────────────────────────────────────────────────────
Info "Building FalconDB (release)..."
cargo build --release -p falcon_server
if ($LASTEXITCODE -ne 0) { throw "Build failed" }

# ── 2. Start server ────────────────────────────────────────────────────────
Info "Starting FalconDB on port $PG_PORT (data: $DATA_DIR)..."
$server = Start-Process -NoNewWindow -PassThru -FilePath "cargo" `
    -ArgumentList "run","--release","-p","falcon_server","--","--pg-addr","0.0.0.0:$PG_PORT","--data-dir",$DATA_DIR,"--no-wal"
Start-Sleep -Seconds 3

if ($server.HasExited) { throw "FalconDB failed to start" }
Ok "FalconDB running (PID $($server.Id))"

try {
    # ── 3. Health check ─────────────────────────────────────────────────────
    Info "Health check..."
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:8080/health" -TimeoutSec 5
    Ok "Health endpoint OK: $health"

    # ── 4. SQL smoke test ───────────────────────────────────────────────────
    Info "Running SQL smoke test..."
    $sql = @"
CREATE TABLE demo (id INT PRIMARY KEY, name TEXT, score FLOAT8);
INSERT INTO demo VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.0), (3, 'Eve', 92.3);
UPDATE demo SET score = 99.0 WHERE id = 1;
DELETE FROM demo WHERE id = 3;
SELECT * FROM demo ORDER BY id;
SELECT COUNT(*), AVG(score), MAX(score) FROM demo;
SHOW falcon.txn_stats;
SHOW falcon.node_role;
DROP TABLE demo;
"@
    $sql | psql -h 127.0.0.1 -p $PG_PORT -U falcon
    if ($LASTEXITCODE -ne 0) { throw "SQL smoke test failed" }
    Ok "SQL smoke test passed"

    # ── 5. Benchmark ────────────────────────────────────────────────────────
    Info "Running quick benchmark (1000 ops)..."
    cargo run --release -p falcon_bench -- --ops 1000 --shards 2
    Ok "Benchmark complete"

    Write-Host ""
    Write-Host "=== FalconDB demo completed successfully ===" -ForegroundColor Green
    Write-Host ""
}
finally {
    # ── Cleanup ─────────────────────────────────────────────────────────────
    Info "Stopping FalconDB..."
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
    if (Test-Path $DATA_DIR) { Remove-Item -Recurse -Force $DATA_DIR }
    Info "Cleaned up $DATA_DIR"
}
