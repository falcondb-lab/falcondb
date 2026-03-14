-- FalconDB Benchmark — W2: Multi-Table Transaction
-- Full ACID transaction: read account → insert transfer → update balance
--
-- Each iteration:
--   BEGIN
--   SELECT balance FROM accounts WHERE id = :from_id
--   INSERT INTO transfers (from_id, to_id, amount, ts) VALUES (...)
--   UPDATE accounts SET balance = balance - :amount WHERE id = :from_id
--   UPDATE accounts SET balance = balance + :amount WHERE id = :to_id
--   COMMIT
--
-- Usage: Called by scripts/run_workload.sh, not run directly.

BEGIN;
SELECT balance FROM accounts WHERE id = :from_id;
INSERT INTO transfers (from_id, to_id, amount, ts) VALUES (:from_id, :to_id, :amount, CURRENT_TIMESTAMP);
UPDATE accounts SET balance = balance - :amount WHERE id = :from_id;
UPDATE accounts SET balance = balance + :amount WHERE id = :to_id;
COMMIT;
