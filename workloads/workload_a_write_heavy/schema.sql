-- Workload A: Write-Heavy — Schema
-- Single table, BIGINT PK, minimal columns to isolate WAL write path.

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id    BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount      BIGINT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Pre-seed 10K rows for UPDATE targets
INSERT INTO orders (order_id, customer_id, amount, status)
SELECT
    g,
    (g % 5000) + 1,
    100 + (g % 9900),
    'active'
FROM generate_series(1, 10000) AS g;
