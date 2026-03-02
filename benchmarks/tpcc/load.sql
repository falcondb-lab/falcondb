-- ============================================================================
-- TPC-C Benchmark — Data Loading (Scale Factor = 1 warehouse)
-- Based on TPC-C Standard Specification v5.11 §4.3
-- ============================================================================
-- Adjust generate_series upper bounds for multi-warehouse:
--   W warehouses → item=100K (fixed), warehouse=W, district=10×W,
--   customer=30K×W, orders=30K×W, order_line=~300K×W, stock=100K×W
-- ============================================================================

-- ── Item: 100,000 rows (static, warehouse-independent) ──────────────────────

INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data)
SELECT
    g,
    (g * 7 + 13) % 10000 + 1,
    'item_' || g || '_' || substr(md5(g::text), 1, 8),
    (g % 9901) + 100,  -- 1.00–99.01 in cents
    CASE WHEN g % 10 = 0
         THEN 'ORIGINAL_' || substr(md5(g::text), 1, 30)
         ELSE 'data_' || substr(md5(g::text), 1, 40)
    END
FROM generate_series(1, 100000) AS g;

-- ── Warehouse: 1 row (scale factor = 1) ─────────────────────────────────────

INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
VALUES (1, 'Warehouse_1', '123 Main St', 'Suite 100', 'Anytown', 'CA', '90210', 1800, 30000000);

-- ── District: 10 per warehouse ──────────────────────────────────────────────

INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
SELECT
    g, 1,
    'District_' || g,
    g || '00 Oak Ave', 'Floor ' || g, 'Cityville', 'NY', '1000' || g,
    (g * 397) % 2001,  -- 0.0000–0.2000
    3000000,
    3001
FROM generate_series(1, 10) AS g;

-- ── Customer: 3,000 per district (30,000 total for 1 warehouse) ─────────────

INSERT INTO customer (
    c_id, c_d_id, c_w_id, c_first, c_middle, c_last,
    c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
    c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment,
    c_payment_cnt, c_delivery_cnt, c_data
)
SELECT
    ((g - 1) % 3000) + 1,
    ((g - 1) / 3000) + 1,
    1,
    'First_' || g,
    'OE',
    'Last_' || ((g - 1) % 1000),
    g || ' Elm St', 'Apt ' || (g % 100), 'Town_' || (g % 50),
    chr(65 + (g % 26)) || chr(65 + ((g + 7) % 26)),
    '0' || (10000 + g % 90000),
    '555-' || lpad((g % 10000)::text, 4, '0'),
    CASE WHEN g % 10 = 0 THEN 'BC' ELSE 'GC' END,
    5000000,
    (g % 5001),  -- 0.0000–0.5000
    -1000,
    1000,
    1, 0,
    'customer_data_' || substr(md5(g::text), 1, 40)
FROM generate_series(1, 30000) AS g;

-- ── History: 1 per customer (30,000 rows) ───────────────────────────────────

INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_data)
SELECT
    ((g - 1) % 3000) + 1,
    ((g - 1) / 3000) + 1,
    1,
    ((g - 1) / 3000) + 1,
    1,
    1000,
    'hist_' || g
FROM generate_series(1, 30000) AS g;

-- ── Orders: 3,000 per district (30,000 total) ──────────────────────────────

INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local)
SELECT
    ((g - 1) % 3000) + 1,
    ((g - 1) / 3000) + 1,
    1,
    ((g - 1) % 3000) + 1,  -- permuted in real TPC-C; sequential here for simplicity
    CASE WHEN (g - 1) % 3000 >= 2100 THEN NULL ELSE ((g - 1) % 10) + 1 END,
    5 + (g % 11),  -- 5–15 order lines
    1
FROM generate_series(1, 30000) AS g;

-- ── New-Order: last 900 per district (9,000 total) ──────────────────────────

INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
SELECT
    o_id, o_d_id, o_w_id
FROM orders
WHERE o_id > 2100;

-- ── Order-Line: avg 10 per order (using o_ol_cnt from orders) ───────────────
-- Simplified: 10 lines per order = 300,000 total

INSERT INTO order_line (
    ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
    ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info
)
SELECT
    o.o_id,
    o.o_d_id,
    o.o_w_id,
    ln,
    ((o.o_id * 7 + ln * 13 + o.o_d_id) % 100000) + 1,
    1,
    CASE WHEN o.o_id <= 2100 THEN CURRENT_TIMESTAMP ELSE NULL END,
    5,
    CASE WHEN o.o_id <= 2100 THEN 0 ELSE ((o.o_id * ln) % 999901) + 100 END,
    'dist_info_' || o.o_d_id || '_' || ln
FROM orders o
CROSS JOIN generate_series(1, 10) AS ln;

-- ── Stock: 100,000 per warehouse ────────────────────────────────────────────

INSERT INTO stock (
    s_i_id, s_w_id, s_quantity,
    s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
    s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10,
    s_ytd, s_order_cnt, s_remote_cnt, s_data
)
SELECT
    g, 1,
    10 + (g % 91),  -- 10–100
    'dist01_' || g, 'dist02_' || g, 'dist03_' || g, 'dist04_' || g, 'dist05_' || g,
    'dist06_' || g, 'dist07_' || g, 'dist08_' || g, 'dist09_' || g, 'dist10_' || g,
    0, 0, 0,
    CASE WHEN g % 10 = 0
         THEN 'ORIGINAL_' || substr(md5(g::text), 1, 30)
         ELSE 'stock_data_' || substr(md5(g::text), 1, 35)
    END
FROM generate_series(1, 100000) AS g;
