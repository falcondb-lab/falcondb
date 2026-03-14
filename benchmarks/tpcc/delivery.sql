-- ============================================================================
-- TPC-C Transaction Profile: Delivery (4% of mix)
-- TPC-C Standard §2.7
-- ============================================================================
-- Batch transaction: for each of 10 districts, finds the oldest new_order,
-- deletes it, updates the order's carrier, sums order_line amounts,
-- updates customer balance and delivery count.
-- ============================================================================

\set w_id 1
\set carrier_id random(1, 10)

BEGIN;

-- Process one district per pgbench invocation (district chosen randomly)
\set d_id random(1, 10)

-- Step 1: Find oldest undelivered order in this district
SELECT MIN(no_o_id) AS next_o_id FROM new_order
WHERE no_w_id = :w_id AND no_d_id = :d_id;

-- Step 2: Delete the new_order row
DELETE FROM new_order
WHERE no_w_id = :w_id AND no_d_id = :d_id
  AND no_o_id = (
      SELECT MIN(no_o_id) FROM new_order
      WHERE no_w_id = :w_id AND no_d_id = :d_id
  );

-- Step 3: Update order with carrier_id
UPDATE orders
SET o_carrier_id = :carrier_id
WHERE o_w_id = :w_id AND o_d_id = :d_id
  AND o_id = (
      SELECT MIN(o_id) FROM orders
      WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_carrier_id IS NULL
  );

-- Step 4: Update order_line delivery dates
UPDATE order_line
SET ol_delivery_d = CURRENT_TIMESTAMP
WHERE ol_w_id = :w_id AND ol_d_id = :d_id
  AND ol_o_id = (
      SELECT MIN(o_id) FROM orders
      WHERE o_w_id = :w_id AND o_d_id = :d_id
        AND o_carrier_id = :carrier_id
        AND o_id > 2100
  )
  AND ol_delivery_d IS NULL;

-- Step 5: Update customer balance with sum of delivered order line amounts
UPDATE customer
SET c_balance = c_balance + COALESCE((
    SELECT SUM(ol_amount) FROM order_line
    WHERE ol_w_id = :w_id AND ol_d_id = :d_id
      AND ol_o_id = (
          SELECT MIN(o_id) FROM orders
          WHERE o_w_id = :w_id AND o_d_id = :d_id
            AND o_carrier_id = :carrier_id AND o_id > 2100
      )
), 0),
    c_delivery_cnt = c_delivery_cnt + 1
WHERE c_w_id = :w_id AND c_d_id = :d_id
  AND c_id = (
      SELECT o_c_id FROM orders
      WHERE o_w_id = :w_id AND o_d_id = :d_id
        AND o_carrier_id = :carrier_id AND o_id > 2100
      ORDER BY o_id LIMIT 1
  );

COMMIT;
