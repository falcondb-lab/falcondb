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

-- Seed data is loaded by run.ps1 (batch INSERT VALUES)
-- FalconDB does not support INSERT...SELECT FROM generate_series
