# Cedar M1 Benchmark Runner
# ==========================
# Runs all frozen M1 benchmark configurations and outputs CSV results.
# Usage: powershell -File scripts/run_m1_benchmark.ps1

$ErrorActionPreference = "Stop"

Write-Host "==============================================="
Write-Host "  Cedar M1 Benchmark Suite"
Write-Host "==============================================="
Write-Host ""

# Build release first
Write-Host "Building release..."
cargo build --release -p cedar_bench
Write-Host ""

# Config 1: High Local (90:10)
Write-Host "--- Config 1: High Local (90% local, 10% global) ---"
cargo run --release -p cedar_bench -- --ops 20000 --read-pct 50 --local-pct 90 --shards 4 --export csv
Write-Host ""

# Config 2: Balanced (50:50)
Write-Host "--- Config 2: Balanced (50% local, 50% global) ---"
cargo run --release -p cedar_bench -- --ops 20000 --read-pct 50 --local-pct 50 --shards 4 --export csv
Write-Host ""

# Config 3: All Global (0:100)
Write-Host "--- Config 3: All Global (0% local, 100% global) ---"
cargo run --release -p cedar_bench -- --ops 20000 --read-pct 50 --local-pct 0 --shards 4 --export csv
Write-Host ""

# Chart 1: TPS vs Shard Count
Write-Host "--- Chart 1: TPS vs Shard Count (1/2/4/8) ---"
cargo run --release -p cedar_bench -- --scaleout --ops 10000 --export csv
Write-Host ""

# Chart 2: Fast vs Slow p99 Latency
Write-Host "--- Chart 2: Fast-Path ON vs OFF ---"
cargo run --release -p cedar_bench -- --compare --ops 20000 --export csv
Write-Host ""

# Chart 3: Failover Before/After
Write-Host "--- Chart 3: Failover Benchmark ---"
cargo run --release -p cedar_bench -- --failover --ops 10000 --export csv
Write-Host ""

Write-Host "==============================================="
Write-Host "  M1 Benchmark Suite Complete"
Write-Host "==============================================="
