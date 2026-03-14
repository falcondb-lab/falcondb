-- FalconDB Benchmark — W5: Batch Insert Throughput
-- Measures raw single-row INSERT throughput into an append-only table.
--
-- Tests WAL write speed, index maintenance overhead, and memory
-- allocation under sustained insert pressure.
--
-- The target table is created fresh for each run by setup_batch.sql.
--
-- Usage: Called by scripts/run_workload.sh, not run directly.

-- ── Single-row insert ──
\set seq_id random(1, 2000000000)
\set amount random(1, 10000)
INSERT INTO bench_events (id, account_id, event_type, amount, created_at)
VALUES (:seq_id, random(1, 100000), 'transfer', :amount, CURRENT_TIMESTAMP);
