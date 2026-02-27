-- ============================================================================
-- FalconDB MES 工单 & 工序执行核心系统 — 数据库 Schema
-- ============================================================================
-- 四张核心表：
--   1. work_order          — 生产工单
--   2. operation           — 工序定义（每张工单下的固定顺序工序）
--   3. operation_report    — 工序报工事实账本（append-only，灵魂表）
--   4. work_order_state_log — 工单状态变更历史（审计 / 追溯）
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- 1. 生产工单
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE work_order (
    work_order_id    BIGSERIAL    PRIMARY KEY,
    product_code     VARCHAR(64)  NOT NULL,
    planned_qty      INTEGER      NOT NULL CHECK (planned_qty > 0),
    completed_qty    INTEGER      NOT NULL DEFAULT 0 CHECK (completed_qty >= 0),
    status           VARCHAR(20)  NOT NULL DEFAULT 'CREATED'
                     CHECK (status IN ('CREATED', 'IN_PROGRESS', 'COMPLETED')),
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────────────────────
-- 2. 工序定义
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE operation (
    operation_id     BIGSERIAL    PRIMARY KEY,
    work_order_id    BIGINT       NOT NULL REFERENCES work_order(work_order_id),
    seq_no           INTEGER      NOT NULL CHECK (seq_no > 0),
    operation_name   VARCHAR(128) NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'RUNNING', 'DONE')),
    UNIQUE (work_order_id, seq_no)
);

CREATE INDEX idx_operation_wo ON operation(work_order_id, seq_no);

-- ────────────────────────────────────────────────────────────────────────────
-- 3. 工序报工事实账本（append-only — 系统灵魂表）
--    每一条记录 = 一次不可否认的生产事实
--    规则: 只允许 INSERT，不允许 UPDATE / DELETE
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE operation_report (
    report_id        BIGSERIAL    PRIMARY KEY,
    work_order_id    BIGINT       NOT NULL REFERENCES work_order(work_order_id),
    operation_id     BIGINT       NOT NULL REFERENCES operation(operation_id),
    report_qty       INTEGER      NOT NULL CHECK (report_qty > 0),
    reported_by      VARCHAR(64)  NOT NULL,
    reported_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_report_wo ON operation_report(work_order_id);
CREATE INDEX idx_report_op ON operation_report(operation_id);

-- ────────────────────────────────────────────────────────────────────────────
-- 4. 工单状态变更历史（审计 / 追溯 / 事故复盘）
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE work_order_state_log (
    event_id         BIGSERIAL    PRIMARY KEY,
    work_order_id    BIGINT       NOT NULL REFERENCES work_order(work_order_id),
    from_status      VARCHAR(20)  NOT NULL,
    to_status        VARCHAR(20)  NOT NULL,
    event_time       TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_state_log_wo ON work_order_state_log(work_order_id, event_time);
