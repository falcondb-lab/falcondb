-- FalconDB MES Schema (4 tables)

CREATE TABLE work_order (
    work_order_id    INTEGER      PRIMARY KEY,
    product_code     TEXT         NOT NULL,
    planned_qty      INTEGER      NOT NULL DEFAULT 0,
    completed_qty    INTEGER      NOT NULL DEFAULT 0,
    status           TEXT         NOT NULL DEFAULT 'CREATED',
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE operation (
    operation_id     INTEGER      PRIMARY KEY,
    work_order_id    INTEGER      NOT NULL,
    seq_no           INTEGER      NOT NULL,
    operation_name   TEXT         NOT NULL,
    status           TEXT         NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE operation_report (
    report_id        INTEGER      PRIMARY KEY,
    work_order_id    INTEGER      NOT NULL,
    operation_id     INTEGER      NOT NULL,
    report_qty       INTEGER      NOT NULL,
    reported_by      TEXT         NOT NULL,
    reported_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE work_order_state_log (
    event_id         INTEGER      PRIMARY KEY,
    work_order_id    INTEGER      NOT NULL,
    from_status      TEXT         NOT NULL,
    to_status        TEXT         NOT NULL,
    event_time       TIMESTAMP    NOT NULL DEFAULT NOW()
);
