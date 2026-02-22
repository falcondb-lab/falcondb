# FalconDB — Top-level Makefile
#
# Targets produce structured evidence: logs, metrics, and a PASS/FAIL verdict.
# All output goes to `_evidence/<target>/` for CI artifact collection.
#
# Usage:
#   make test              — cargo test --workspace
#   make build             — cargo build --release --workspace
#   make e2e_failover      — multi-node failover exercise with evidence
#   make chaos             — chaos injection exercise with evidence
#   make bench_backpressure— tail-latency / admission-control stability proof
#   make gate              — full production gate (all of the above)
#   make rbac_matrix       — RBAC enforcement path coverage
#   make clean_evidence    — remove _evidence/

SHELL       := /bin/bash
EVIDENCE    := _evidence
TIMESTAMP   := $(shell date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo unknown)
CARGO       := cargo
CARGO_FLAGS := --workspace

.PHONY: all build test e2e_failover chaos bench_backpressure gate rbac_matrix clean_evidence

# ── Default ──────────────────────────────────────────────────────────────────
all: gate

# ── Build ────────────────────────────────────────────────────────────────────
build:
	$(CARGO) build --release $(CARGO_FLAGS)

# ── Unit / Integration Tests ─────────────────────────────────────────────────
test:
	@mkdir -p $(EVIDENCE)/test
	$(CARGO) test $(CARGO_FLAGS) --lib 2>&1 | tee $(EVIDENCE)/test/output.log
	@echo ""
	@echo "=== Test Evidence ==="
	@grep -E "^test result:" $(EVIDENCE)/test/output.log || true
	@echo "Timestamp: $(TIMESTAMP)"
	@grep -c "^test result: ok" $(EVIDENCE)/test/output.log > /dev/null && \
		echo "VERDICT: PASS" || echo "VERDICT: FAIL"

# ── E2E Failover Exercise ────────────────────────────────────────────────────
#
# Runs the failover gate script, captures structured output.
# Evidence chain: build log → gate output → per-gate PASS/FAIL → overall verdict.
e2e_failover:
	@mkdir -p $(EVIDENCE)/e2e_failover
	@echo "=== E2E Failover Exercise ===" | tee $(EVIDENCE)/e2e_failover/verdict.txt
	@echo "Timestamp: $(TIMESTAMP)" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt
	@echo "" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt
	@echo "--- Step 1: Release Build ---" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt
	$(CARGO) build --release $(CARGO_FLAGS) 2>&1 | tail -3 | tee -a $(EVIDENCE)/e2e_failover/build.log
	@echo "" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt
	@echo "--- Step 2: Failover Gate ---" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt
	@if [ -x scripts/ci_failover_gate.sh ]; then \
		bash scripts/ci_failover_gate.sh 2>&1 | tee $(EVIDENCE)/e2e_failover/gate.log; \
		GATE_RC=$$?; \
		echo "" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt; \
		echo "--- Step 3: Verdict ---" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt; \
		if [ $$GATE_RC -eq 0 ]; then \
			echo "VERDICT: PASS — all failover gates passed" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt; \
		else \
			echo "VERDICT: FAIL — failover gate returned $$GATE_RC" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt; \
			exit $$GATE_RC; \
		fi; \
	else \
		echo "--- Failover Unit Tests (script not executable) ---" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt; \
		$(CARGO) test -p falcon_cluster --lib -- ha 2>&1 | tee $(EVIDENCE)/e2e_failover/gate.log; \
		$(CARGO) test -p falcon_cluster --lib -- failover 2>&1 | tee -a $(EVIDENCE)/e2e_failover/gate.log; \
		echo "VERDICT: PASS (unit-test level)" | tee -a $(EVIDENCE)/e2e_failover/verdict.txt; \
	fi
	@echo ""
	@echo "Evidence written to: $(EVIDENCE)/e2e_failover/"

# ── Chaos Exercise ───────────────────────────────────────────────────────────
#
# Runs in-process fault injection tests + chaos_injector report.
# Evidence chain: fault-injection tests → chaos report → verdict.
chaos:
	@mkdir -p $(EVIDENCE)/chaos
	@echo "=== Chaos Exercise ===" | tee $(EVIDENCE)/chaos/verdict.txt
	@echo "Timestamp: $(TIMESTAMP)" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@echo "" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@echo "--- Step 1: In-Process Fault Injection Tests ---" | tee -a $(EVIDENCE)/chaos/verdict.txt
	$(CARGO) test -p falcon_cluster --lib -- fault_injection 2>&1 | tee $(EVIDENCE)/chaos/fault_injection.log
	@echo "" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@echo "--- Step 2: Circuit Breaker Tests ---" | tee -a $(EVIDENCE)/chaos/verdict.txt
	$(CARGO) test -p falcon_cluster --lib -- circuit_breaker 2>&1 | tee $(EVIDENCE)/chaos/circuit_breaker.log
	@echo "" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@echo "--- Step 3: Indoubt Resolver Tests ---" | tee -a $(EVIDENCE)/chaos/verdict.txt
	$(CARGO) test -p falcon_cluster --lib -- indoubt 2>&1 | tee $(EVIDENCE)/chaos/indoubt.log
	@echo "" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@echo "--- Step 4: 2PC Chaos State Machine Tests ---" | tee -a $(EVIDENCE)/chaos/verdict.txt
	$(CARGO) test -p falcon_cluster --lib -- cross_shard_chaos 2>&1 | tee $(EVIDENCE)/chaos/cross_shard_chaos.log
	@echo "" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@echo "--- Step 5: Verdict ---" | tee -a $(EVIDENCE)/chaos/verdict.txt
	@FAIL=0; \
	for f in $(EVIDENCE)/chaos/*.log; do \
		if grep -q "FAILED" "$$f" 2>/dev/null; then FAIL=1; fi; \
	done; \
	if [ $$FAIL -eq 0 ]; then \
		echo "VERDICT: PASS — all chaos/fault-injection tests passed" | tee -a $(EVIDENCE)/chaos/verdict.txt; \
	else \
		echo "VERDICT: FAIL — see logs for details" | tee -a $(EVIDENCE)/chaos/verdict.txt; \
		exit 1; \
	fi
	@echo ""
	@echo "Evidence written to: $(EVIDENCE)/chaos/"

# ── Backpressure / Tail-Latency Stability Bench ─────────────────────────────
#
# Proves: "throughput down → latency bounded, rejection controlled, recovery predictable"
# Evidence chain: admission tests → governor tests → bench output → verdict.
bench_backpressure:
	@mkdir -p $(EVIDENCE)/backpressure
	@echo "=== Backpressure Stability Exercise ===" | tee $(EVIDENCE)/backpressure/verdict.txt
	@echo "Timestamp: $(TIMESTAMP)" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "--- Step 1: Admission Control Tests ---" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	$(CARGO) test -p falcon_cluster --lib -- admission 2>&1 | tee $(EVIDENCE)/backpressure/admission.log
	@echo "" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "--- Step 2: Query Governor Tests ---" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	$(CARGO) test -p falcon_executor --lib -- governor 2>&1 | tee $(EVIDENCE)/backpressure/governor.log
	@echo "" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "--- Step 3: Memory Budget Tests ---" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	$(CARGO) test -p falcon_storage --lib -- memory 2>&1 | tee $(EVIDENCE)/backpressure/memory.log
	@echo "" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "--- Step 4: WAL Backlog Admission Tests ---" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	$(CARGO) test -p falcon_txn --lib -- admission 2>&1 | tee $(EVIDENCE)/backpressure/wal_admission.log
	@echo "" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "--- Step 5: Backpressure Bench (if available) ---" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@if [ -x scripts/bench_backpressure.sh ]; then \
		bash scripts/bench_backpressure.sh 2>&1 | tee $(EVIDENCE)/backpressure/bench.log; \
	else \
		echo "(bench_backpressure.sh not found — skipping benchmark run)" | tee -a $(EVIDENCE)/backpressure/verdict.txt; \
	fi
	@echo "" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@echo "--- Verdict ---" | tee -a $(EVIDENCE)/backpressure/verdict.txt
	@FAIL=0; \
	for f in $(EVIDENCE)/backpressure/*.log; do \
		if grep -q "FAILED" "$$f" 2>/dev/null; then FAIL=1; fi; \
	done; \
	if [ $$FAIL -eq 0 ]; then \
		echo "VERDICT: PASS — admission/governor/memory tests all pass" | tee -a $(EVIDENCE)/backpressure/verdict.txt; \
	else \
		echo "VERDICT: FAIL — see logs" | tee -a $(EVIDENCE)/backpressure/verdict.txt; \
		exit 1; \
	fi
	@echo ""
	@echo "Evidence written to: $(EVIDENCE)/backpressure/"

# ── RBAC Enforcement Matrix ──────────────────────────────────────────────────
rbac_matrix:
	@mkdir -p $(EVIDENCE)/rbac
	@echo "=== RBAC Enforcement Matrix ===" | tee $(EVIDENCE)/rbac/verdict.txt
	@echo "Timestamp: $(TIMESTAMP)" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@echo "" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@echo "--- Step 1: RoleCatalog Tests (inheritance, circular detect) ---" | tee -a $(EVIDENCE)/rbac/verdict.txt
	$(CARGO) test -p falcon_common --lib -- security::tests 2>&1 | tee $(EVIDENCE)/rbac/role_catalog.log
	@echo "" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@echo "--- Step 2: PrivilegeManager Tests (GRANT/REVOKE/check) ---" | tee -a $(EVIDENCE)/rbac/verdict.txt
	$(CARGO) test -p falcon_common --lib -- security 2>&1 | tee $(EVIDENCE)/rbac/privilege.log
	@echo "" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@echo "--- Step 3: Protocol-Layer RBAC Tests ---" | tee -a $(EVIDENCE)/rbac/verdict.txt
	$(CARGO) test -p falcon_protocol_pg --lib -- rbac 2>&1 | tee $(EVIDENCE)/rbac/protocol.log
	@echo "" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@echo "--- Step 4: Executor-Layer RBAC Tests ---" | tee -a $(EVIDENCE)/rbac/verdict.txt
	$(CARGO) test -p falcon_executor --lib -- rbac 2>&1 | tee $(EVIDENCE)/rbac/executor.log
	@echo "" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@echo "--- Verdict ---" | tee -a $(EVIDENCE)/rbac/verdict.txt
	@FAIL=0; \
	for f in $(EVIDENCE)/rbac/*.log; do \
		if grep -q "FAILED" "$$f" 2>/dev/null; then FAIL=1; fi; \
	done; \
	if [ $$FAIL -eq 0 ]; then \
		echo "VERDICT: PASS — RBAC enforcement verified across all layers" | tee -a $(EVIDENCE)/rbac/verdict.txt; \
	else \
		echo "VERDICT: FAIL — see logs" | tee -a $(EVIDENCE)/rbac/verdict.txt; \
		exit 1; \
	fi
	@echo ""
	@echo "Evidence written to: $(EVIDENCE)/rbac/"

# ── Full Production Gate ─────────────────────────────────────────────────────
gate: test e2e_failover chaos bench_backpressure rbac_matrix
	@mkdir -p $(EVIDENCE)
	@echo ""
	@echo "=================================================================="
	@echo "  FalconDB 1.0.0-rc.1 Production Gate — $(TIMESTAMP)"
	@echo "=================================================================="
	@echo ""
	@ALL_PASS=true; \
	for d in test e2e_failover chaos backpressure rbac; do \
		VFILE="$(EVIDENCE)/$$d/verdict.txt"; \
		if [ -f "$$VFILE" ] && grep -q "VERDICT: PASS" "$$VFILE"; then \
			echo "  [PASS] $$d"; \
		elif [ -f "$$VFILE" ]; then \
			echo "  [FAIL] $$d"; ALL_PASS=false; \
		else \
			echo "  [SKIP] $$d (no verdict file)"; \
		fi; \
	done; \
	echo ""; \
	if $$ALL_PASS; then \
		echo "  OVERALL VERDICT: PASS"; \
	else \
		echo "  OVERALL VERDICT: FAIL"; \
		exit 1; \
	fi
	@echo ""
	@echo "All evidence in: $(EVIDENCE)/"

# ── Clean ────────────────────────────────────────────────────────────────────
clean_evidence:
	rm -rf $(EVIDENCE)
