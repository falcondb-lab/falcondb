-- Workload A: Write-Heavy — Operations
-- 90% INSERT, 10% UPDATE. Each op = 1 transaction, 1 write.
-- Driven by run.sh via pgbench custom script format.

-- pgbench variables:
--   :client_id  — pgbench client number
--   :random_id  — random(1, 10000) for UPDATE target

\set order_id random(10001, 99999999)
\set customer_id random(1, 50000)
\set amount random(100, 99999)
\set update_target random(1, 10000)
\set op random(1, 10)

-- 90% INSERT (op 1-9), 10% UPDATE (op 10)
\if :op <= 9
INSERT INTO orders (order_id, customer_id, amount) VALUES (:order_id, :customer_id, :amount) ON CONFLICT DO NOTHING;
\else
UPDATE orders SET amount = :amount, status = 'updated' WHERE order_id = :update_target;
\endif
