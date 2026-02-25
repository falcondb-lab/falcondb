-- FalconDB Benchmark — Schema for W5 (Batch Insert)
-- Called before W5 runs; creates a fresh append-only table.

DROP TABLE IF EXISTS bench_events;

CREATE TABLE bench_events (
    id          INT PRIMARY KEY,
    account_id  INT NOT NULL,
    event_type  TEXT NOT NULL,
    amount      BIGINT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
