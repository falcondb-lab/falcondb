# ============================================================================
# FalconDB — E2E Two-Node Failover Test (Windows PowerShell)
# ============================================================================
# Minimal closed-loop test:
#   1. Start primary (WAL-enabled, gRPC replication)
#   2. Start replica (connects to primary)
#   3. Write test data on primary via psql, verify
#   4. Kill primary (simulate failure)
#   5. Promote replica (restart as standalone)
#   6. Verify old data readable + new writes succeed
#   7. Output PASS/FAIL with diagnostics
#
# Prerequisites: Rust 1.75+, psql in PATH, ports 5433/5434/50051 free
# Usage:  .\scripts\e2e_two_node_failover.ps1
# ============================================================================
$ErrorActionPreference = "Stop"

function Info($msg)  { Write-Host "[e2e ] $msg" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "[ ok ] $msg" -ForegroundColor Green }
function Fail($msg)  { Write-Host "[FAIL] $msg" -ForegroundColor Red; $script:Result = "FAIL" }

$script:Result = "PASS"
$DIR_PRIMARY = Join-Path $env:TEMP "falcon_e2e_primary_$(Get-Random)"
$DIR_REPLICA = Join-Path $env:TEMP "falcon_e2e_replica_$(Get-Random)"
$PRIMARY_PG  = 5433
$REPLICA_PG  = 5434
$GRPC_PORT   = 50051
$Processes   = @()

function WaitForPg($port, $label, $maxWait = 15) {
    for ($i = 0; $i -lt $maxWait; $i++) {
        try {
            $out = psql -h 127.0.0.1 -p $port -U falcon -t -A -c "SELECT 1;" 2>$null
            if ($out -match "1") { return $true }
        } catch {}
        Start-Sleep -Seconds 1
    }
    return $false
}

function Cleanup {
    Info "Cleaning up..."
    foreach ($p in $script:Processes) {
        try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
    }
    Start-Sleep -Seconds 1
    if ($script:Result -ne "PASS") {
        Write-Host ""
        Write-Host "=== DIAGNOSTICS ===" -ForegroundColor Red
        Write-Host "Primary data dir: $DIR_PRIMARY"
        Write-Host "Replica data dir: $DIR_REPLICA"
        Write-Host "Primary PG port:  $PRIMARY_PG"
        Write-Host "Replica PG port:  $REPLICA_PG"
        Write-Host "gRPC port:        $GRPC_PORT"
        if (Test-Path "$DIR_PRIMARY\server.log") {
            Write-Host "--- Primary log (last 30 lines) ---"
            Get-Content "$DIR_PRIMARY\server.log" -Tail 30
        }
        if (Test-Path "$DIR_REPLICA\server.log") {
            Write-Host "--- Replica log (last 30 lines) ---"
            Get-Content "$DIR_REPLICA\server.log" -Tail 30
        }
    } else {
        Remove-Item -Recurse -Force $DIR_PRIMARY -ErrorAction SilentlyContinue
        Remove-Item -Recurse -Force $DIR_REPLICA -ErrorAction SilentlyContinue
    }
}

try {
    # ── Build ───────────────────────────────────────────────────────────────
    Info "Building FalconDB (release)..."
    cargo build --release -p falcon_server
    if ($LASTEXITCODE -ne 0) { throw "Build failed" }

    New-Item -ItemType Directory -Path $DIR_PRIMARY -Force | Out-Null
    New-Item -ItemType Directory -Path $DIR_REPLICA -Force | Out-Null

    # ── Step 1: Start Primary ───────────────────────────────────────────────
    Info "Step 1: Starting PRIMARY (PG=$PRIMARY_PG, gRPC=$GRPC_PORT)..."
    $pPrimary = Start-Process -NoNewWindow -PassThru -FilePath "cargo" `
        -ArgumentList "run","--release","-p","falcon_server","--",`
            "--role","primary",`
            "--pg-addr","0.0.0.0:$PRIMARY_PG",`
            "--grpc-addr","0.0.0.0:$GRPC_PORT",`
            "--data-dir",$DIR_PRIMARY,`
            "--metrics-addr","0.0.0.0:9090" `
        -RedirectStandardOutput "$DIR_PRIMARY\server.log" `
        -RedirectStandardError "$DIR_PRIMARY\server_err.log"
    $script:Processes += $pPrimary

    if (-not (WaitForPg $PRIMARY_PG "primary")) {
        Fail "Primary failed to start within 15s"; throw "abort"
    }
    Ok "Primary ready on port $PRIMARY_PG (PID $($pPrimary.Id))"

    # ── Step 2: Start Replica ───────────────────────────────────────────────
    Info "Step 2: Starting REPLICA (PG=$REPLICA_PG)..."
    $pReplica = Start-Process -NoNewWindow -PassThru -FilePath "cargo" `
        -ArgumentList "run","--release","-p","falcon_server","--",`
            "--role","replica",`
            "--pg-addr","0.0.0.0:$REPLICA_PG",`
            "--primary-endpoint","http://127.0.0.1:$GRPC_PORT",`
            "--data-dir",$DIR_REPLICA,`
            "--metrics-addr","0.0.0.0:9091" `
        -RedirectStandardOutput "$DIR_REPLICA\server.log" `
        -RedirectStandardError "$DIR_REPLICA\server_err.log"
    $script:Processes += $pReplica

    if (-not (WaitForPg $REPLICA_PG "replica")) {
        Fail "Replica failed to start within 15s"; throw "abort"
    }
    Ok "Replica ready on port $REPLICA_PG (PID $($pReplica.Id))"

    # ── Step 3: Wait for replication ────────────────────────────────────────
    Info "Step 3: Waiting for replication sync (3s)..."
    Start-Sleep -Seconds 3

    # ── Step 4: Write + verify on Primary ───────────────────────────────────
    Info "Step 4: Writing test data on PRIMARY..."
    @"
CREATE TABLE e2e_test (id INT PRIMARY KEY, name TEXT, score FLOAT8);
INSERT INTO e2e_test VALUES (1, 'Alice', 95.5);
INSERT INTO e2e_test VALUES (2, 'Bob', 87.0);
INSERT INTO e2e_test VALUES (3, 'Charlie', 92.3);
"@ | psql -h 127.0.0.1 -p $PRIMARY_PG -U falcon | Out-Null

    $rowCount = (psql -h 127.0.0.1 -p $PRIMARY_PG -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;").Trim()
    if ($rowCount -eq "3") { Ok "Primary has 3 rows" }
    else { Fail "Primary expected 3 rows, got: $rowCount" }

    Info "Waiting for WAL replication (2s)..."
    Start-Sleep -Seconds 2

    # Check replica
    try {
        $repCount = (psql -h 127.0.0.1 -p $REPLICA_PG -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;" 2>$null).Trim()
        if ($repCount -eq "3") { Ok "Replica has 3 rows (replication confirmed)" }
        else { Info "Replica row count: $repCount (replication may not be instant)" }
    } catch { Info "Replica query failed (replication may still be catching up)" }

    # ── Step 5: Kill Primary ────────────────────────────────────────────────
    Info "Step 5: Killing PRIMARY (simulating failure)..."
    Stop-Process -Id $pPrimary.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Ok "Primary killed"

    # ── Step 6: Promote Replica ─────────────────────────────────────────────
    Info "Step 6: Promoting REPLICA (restart as standalone)..."
    Stop-Process -Id $pReplica.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1

    $pPromoted = Start-Process -NoNewWindow -PassThru -FilePath "cargo" `
        -ArgumentList "run","--release","-p","falcon_server","--",`
            "--role","standalone",`
            "--pg-addr","0.0.0.0:$REPLICA_PG",`
            "--data-dir",$DIR_REPLICA,`
            "--metrics-addr","0.0.0.0:9091" `
        -RedirectStandardOutput "$DIR_REPLICA\promoted.log" `
        -RedirectStandardError "$DIR_REPLICA\promoted_err.log"
    $script:Processes += $pPromoted

    if (-not (WaitForPg $REPLICA_PG "promoted")) {
        Fail "Promoted replica failed to start"; throw "abort"
    }
    Ok "Promoted replica ready on port $REPLICA_PG"

    # ── Step 7: Verify data + new writes ────────────────────────────────────
    Info "Step 7: Verifying data integrity..."

    $promCount = (psql -h 127.0.0.1 -p $REPLICA_PG -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;").Trim()
    if ($promCount -eq "3") { Ok "All 3 rows readable after promotion" }
    else { Fail "Expected 3 rows after promotion, got: $promCount" }

    Info "Writing new data on promoted node..."
    @"
INSERT INTO e2e_test VALUES (4, 'Diana', 88.8);
INSERT INTO e2e_test VALUES (5, 'Eve', 91.1);
"@ | psql -h 127.0.0.1 -p $REPLICA_PG -U falcon | Out-Null

    $finalCount = (psql -h 127.0.0.1 -p $REPLICA_PG -U falcon -t -A -c "SELECT COUNT(*) FROM e2e_test;").Trim()
    if ($finalCount -eq "5") { Ok "New writes succeeded - 5 rows total" }
    else { Fail "Expected 5 rows after new writes, got: $finalCount" }

    $alice = (psql -h 127.0.0.1 -p $REPLICA_PG -U falcon -t -A -c "SELECT name FROM e2e_test WHERE id = 1;").Trim()
    if ($alice -eq "Alice") { Ok "Data integrity: Alice found at id=1" }
    else { Fail "Data integrity: expected Alice, got: $alice" }

    # ── Result ──────────────────────────────────────────────────────────────
    Write-Host ""
    Write-Host "============================================================"
    if ($script:Result -eq "PASS") {
        Write-Host "  E2E TWO-NODE FAILOVER TEST: PASS" -ForegroundColor Green
    } else {
        Write-Host "  E2E TWO-NODE FAILOVER TEST: FAIL" -ForegroundColor Red
    }
    Write-Host "============================================================"
    Write-Host ""
}
finally {
    Cleanup
}

if ($script:Result -ne "PASS") { exit 1 }
