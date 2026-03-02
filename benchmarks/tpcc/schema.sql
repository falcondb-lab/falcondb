-- ============================================================================
-- TPC-C Benchmark Schema for FalconDB / PostgreSQL
-- Based on TPC-C Standard Specification v5.11
-- ============================================================================
-- 9 tables: warehouse, district, customer, history, new_order, orders,
--           order_line, item, stock
-- ============================================================================

DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS item;

-- ── Item (100,000 rows, static, loaded once) ────────────────────────────────

CREATE TABLE item (
    i_id        INT         NOT NULL,
    i_im_id     INT         NOT NULL,
    i_name      TEXT        NOT NULL,
    i_price     BIGINT      NOT NULL,  -- cents (scaled ×100)
    i_data      TEXT        NOT NULL,
    PRIMARY KEY (i_id)
);

-- ── Warehouse (W rows, scales with warehouse count) ─────────────────────────

CREATE TABLE warehouse (
    w_id        INT         NOT NULL,
    w_name      TEXT        NOT NULL,
    w_street_1  TEXT        NOT NULL,
    w_street_2  TEXT        NOT NULL,
    w_city      TEXT        NOT NULL,
    w_state     TEXT        NOT NULL,
    w_zip       TEXT        NOT NULL,
    w_tax       BIGINT      NOT NULL,  -- scaled ×10000
    w_ytd       BIGINT      NOT NULL,  -- cents
    PRIMARY KEY (w_id)
);

-- ── District (10 per warehouse) ─────────────────────────────────────────────

CREATE TABLE district (
    d_id        INT         NOT NULL,
    d_w_id      INT         NOT NULL,
    d_name      TEXT        NOT NULL,
    d_street_1  TEXT        NOT NULL,
    d_street_2  TEXT        NOT NULL,
    d_city      TEXT        NOT NULL,
    d_state     TEXT        NOT NULL,
    d_zip       TEXT        NOT NULL,
    d_tax       BIGINT      NOT NULL,  -- scaled ×10000
    d_ytd       BIGINT      NOT NULL,  -- cents
    d_next_o_id INT         NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
);

-- ── Customer (3,000 per district = 30,000 per warehouse) ────────────────────

CREATE TABLE customer (
    c_id        INT         NOT NULL,
    c_d_id      INT         NOT NULL,
    c_w_id      INT         NOT NULL,
    c_first     TEXT        NOT NULL,
    c_middle    TEXT        NOT NULL,
    c_last      TEXT        NOT NULL,
    c_street_1  TEXT        NOT NULL,
    c_street_2  TEXT        NOT NULL,
    c_city      TEXT        NOT NULL,
    c_state     TEXT        NOT NULL,
    c_zip       TEXT        NOT NULL,
    c_phone     TEXT        NOT NULL,
    c_since     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_credit    TEXT        NOT NULL,  -- 'GC' or 'BC'
    c_credit_lim   BIGINT  NOT NULL,
    c_discount  BIGINT      NOT NULL,  -- scaled ×10000
    c_balance   BIGINT      NOT NULL,  -- cents
    c_ytd_payment  BIGINT   NOT NULL,
    c_payment_cnt  INT      NOT NULL,
    c_delivery_cnt INT      NOT NULL,
    c_data      TEXT        NOT NULL,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

-- ── History (1 per customer initially) ──────────────────────────────────────

CREATE TABLE history (
    h_c_id      INT         NOT NULL,
    h_c_d_id    INT         NOT NULL,
    h_c_w_id    INT         NOT NULL,
    h_d_id      INT         NOT NULL,
    h_w_id      INT         NOT NULL,
    h_date      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    h_amount    BIGINT      NOT NULL,  -- cents
    h_data      TEXT        NOT NULL
);

-- ── Orders (3,000 per district initially) ───────────────────────────────────

CREATE TABLE orders (
    o_id        INT         NOT NULL,
    o_d_id      INT         NOT NULL,
    o_w_id      INT         NOT NULL,
    o_c_id      INT         NOT NULL,
    o_entry_d   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    o_carrier_id INT,
    o_ol_cnt    INT         NOT NULL,
    o_all_local INT         NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

-- ── New-Order (900 per district initially — last 30% of orders) ─────────────

CREATE TABLE new_order (
    no_o_id     INT         NOT NULL,
    no_d_id     INT         NOT NULL,
    no_w_id     INT         NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

-- ── Order-Line (avg 10 per order) ───────────────────────────────────────────

CREATE TABLE order_line (
    ol_o_id     INT         NOT NULL,
    ol_d_id     INT         NOT NULL,
    ol_w_id     INT         NOT NULL,
    ol_number   INT         NOT NULL,
    ol_i_id     INT         NOT NULL,
    ol_supply_w_id INT      NOT NULL,
    ol_delivery_d  TIMESTAMP,
    ol_quantity INT         NOT NULL,
    ol_amount   BIGINT      NOT NULL,  -- cents
    ol_dist_info TEXT       NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

-- ── Stock (100,000 per warehouse) ───────────────────────────────────────────

CREATE TABLE stock (
    s_i_id      INT         NOT NULL,
    s_w_id      INT         NOT NULL,
    s_quantity  INT         NOT NULL,
    s_dist_01   TEXT        NOT NULL,
    s_dist_02   TEXT        NOT NULL,
    s_dist_03   TEXT        NOT NULL,
    s_dist_04   TEXT        NOT NULL,
    s_dist_05   TEXT        NOT NULL,
    s_dist_06   TEXT        NOT NULL,
    s_dist_07   TEXT        NOT NULL,
    s_dist_08   TEXT        NOT NULL,
    s_dist_09   TEXT        NOT NULL,
    s_dist_10   TEXT        NOT NULL,
    s_ytd       INT         NOT NULL,
    s_order_cnt INT         NOT NULL,
    s_remote_cnt INT        NOT NULL,
    s_data      TEXT        NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
);
