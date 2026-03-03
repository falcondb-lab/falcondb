-- Workload B: Read-Write OLTP — Operations
-- 50% SELECT, 25% INSERT, 25% UPDATE
-- PK point lookups, no JOINs, short transactions.
-- pgbench custom script format.

\set uid random(1, 50000)
\set oid random(100001, 99999999)
\set amount random(50, 9999)
\set op random(1, 4)

-- op=1,2 → SELECT (50%)
-- op=3   → INSERT (25%)
-- op=4   → UPDATE (25%)

\if :op <= 2
SELECT user_id, username, balance, status FROM users WHERE user_id = :uid;
\elif :op = 3
INSERT INTO orders (order_id, user_id, amount) VALUES (:oid, :uid, :amount) ON CONFLICT DO NOTHING;
\else
UPDATE users SET balance = balance + :amount WHERE user_id = :uid;
\endif
