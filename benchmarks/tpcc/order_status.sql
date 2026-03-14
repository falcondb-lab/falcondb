-- ============================================================================
-- TPC-C Transaction Profile: Order-Status (4% of mix)
-- TPC-C Standard §2.6
-- ============================================================================
-- Read-only: finds the customer's last order and its order lines.
-- ============================================================================

\set w_id 1
\set d_id random(1, 10)
\set c_id random(1, 3000)

-- Step 1: Read customer info
SELECT c_first, c_middle, c_last, c_balance
FROM customer WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

-- Step 2: Find last order for this customer
SELECT o_id, o_entry_d, o_carrier_id
FROM orders
WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :c_id
ORDER BY o_id DESC
LIMIT 1;

-- Step 3: Read order lines for the last order
SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
FROM order_line
WHERE ol_w_id = :w_id AND ol_d_id = :d_id
  AND ol_o_id = (
      SELECT MAX(o_id) FROM orders
      WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :c_id
  );
