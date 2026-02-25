-- FalconDB Benchmark — W4: High-Contention Hot-Key Update
-- All threads compete to update a very small set of rows (10 hot keys).
--
-- Tests lock/latch contention, MVCC retry overhead, and concurrency
-- control under extreme skew. This is the workload most likely to
-- surface lock-manager bottlenecks and abort-rate differences.
--
-- Usage: Called by scripts/run_workload.sh, not run directly.

-- ── Hot-key update ──
-- Only 10 distinct keys → extreme contention at high thread counts
\set hot_id random(1, 10)
\set delta random(-50, 50)
UPDATE accounts SET balance = balance + :delta WHERE id = :hot_id;
