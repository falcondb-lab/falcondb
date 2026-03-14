-- FalconDB Benchmark — W1: Single-Table OLTP
-- 70% point SELECT, 20% UPDATE, 10% INSERT
--
-- This file defines the operations. The runner script (run_workload.sh)
-- executes these in a loop with concurrency control.
--
-- Usage: Called by scripts/run_workload.sh, not run directly.

-- ── Point SELECT (70% of ops) ──
-- Random account lookup by primary key
SELECT id, balance, name FROM accounts WHERE id = :random_id;

-- ── Point UPDATE (20% of ops) ──
-- Random balance update
UPDATE accounts SET balance = balance + :delta WHERE id = :random_id;

-- ── INSERT (10% of ops) ──
-- Insert new account (id range above pre-loaded data)
INSERT INTO accounts (id, balance, name) VALUES (:new_id, :balance, 'bench_' || :new_id);
