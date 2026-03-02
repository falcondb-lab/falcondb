-- Workload C: Durability & Crash — Schema
-- Intentionally simple: single table with monotonic IDs for gap detection.

DROP TABLE IF EXISTS commit_log;

CREATE TABLE commit_log (
    seq_id      BIGINT PRIMARY KEY,
    payload     TEXT NOT NULL,
    written_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
