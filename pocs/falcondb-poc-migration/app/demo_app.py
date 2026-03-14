#!/usr/bin/env python3
"""
FalconDB PoC #5 — Migration: Demo OLTP Application

A simple e-commerce application that uses standard PostgreSQL client (psycopg2).
The SAME code runs against PostgreSQL and FalconDB — only the config file changes.

Usage:
    python demo_app.py --config app.conf.postgres --workflow all
    python demo_app.py --config app.conf.falcon   --workflow all
    python demo_app.py --config app.conf.falcon   --workflow create_order
"""

import argparse
import configparser
import json
import sys
import time
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


def load_config(path):
    """Load database connection config from INI file."""
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return {
        "host": cfg.get("database", "host", fallback="127.0.0.1"),
        "port": cfg.getint("database", "port", fallback=5432),
        "dbname": cfg.get("database", "dbname", fallback="shop_demo"),
        "user": cfg.get("database", "user", fallback="postgres"),
        "password": cfg.get("database", "password", fallback=""),
    }


def connect(cfg):
    """Connect using standard psycopg2 — no FalconDB-specific code."""
    params = {
        "host": cfg["host"],
        "port": cfg["port"],
        "dbname": cfg["dbname"],
        "user": cfg["user"],
    }
    if cfg.get("password"):
        params["password"] = cfg["password"]
    return psycopg2.connect(**params)


# ── Workflow 1: Create Order ─────────────────────────────────────────────────

def workflow_create_order(conn):
    """Create a new order with items and payment. Uses a transaction."""
    with conn:
        with conn.cursor() as cur:
            # Create order
            cur.execute("""
                INSERT INTO orders (customer_id, status, total_amount, notes)
                VALUES (1, 'pending', 25000, 'PoC migration test order')
                RETURNING id, created_at;
            """)
            row = cur.fetchone()
            order_id = row[0]
            created = row[1]

            # Add items
            cur.execute("""
                INSERT INTO order_items (order_id, product, quantity, unit_price)
                VALUES (%s, 'Test Widget', 2, 10000),
                       (%s, 'Test Gadget', 1, 5000);
            """, (order_id, order_id))

            # Create payment
            cur.execute("""
                INSERT INTO payments (order_id, amount, method, status)
                VALUES (%s, 25000, 'card', 'pending')
                RETURNING id;
            """, (order_id,))
            payment_id = cur.fetchone()[0]

    return {
        "workflow": "create_order",
        "result": "PASS",
        "order_id": order_id,
        "payment_id": payment_id,
        "created_at": str(created),
    }


# ── Workflow 2: Update Order Status ──────────────────────────────────────────

def workflow_update_order(conn):
    """Update an order's status and payment. Uses a transaction."""
    with conn:
        with conn.cursor() as cur:
            # Find the most recent pending order
            cur.execute("""
                SELECT id FROM orders WHERE status = 'pending'
                ORDER BY created_at DESC LIMIT 1;
            """)
            row = cur.fetchone()
            if not row:
                return {"workflow": "update_order", "result": "SKIP", "reason": "no pending orders"}
            order_id = row[0]

            # Update order status
            cur.execute("""
                UPDATE orders SET status = 'completed', updated_at = NOW()
                WHERE id = %s;
            """, (order_id,))

            # Update payment status
            cur.execute("""
                UPDATE payments SET status = 'completed', paid_at = NOW()
                WHERE order_id = %s AND status = 'pending';
            """, (order_id,))

    return {
        "workflow": "update_order",
        "result": "PASS",
        "order_id": order_id,
    }


# ── Workflow 3: Query Order by PK ────────────────────────────────────────────

def workflow_query_order(conn):
    """Fetch a specific order with its items. Standard SELECT + JOIN."""
    with conn.cursor() as cur:
        # Get order
        cur.execute("""
            SELECT o.id, o.status, o.total_amount, o.created_at,
                   c.name AS customer_name, c.email
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            WHERE o.id = 1;
        """)
        order = cur.fetchone()
        if not order:
            return {"workflow": "query_order", "result": "FAIL", "reason": "order 1 not found"}

        # Get items
        cur.execute("""
            SELECT product, quantity, unit_price
            FROM order_items WHERE order_id = 1
            ORDER BY id;
        """)
        items = cur.fetchall()

    return {
        "workflow": "query_order",
        "result": "PASS",
        "order_id": order[0],
        "status": order[1],
        "total_amount": order[2],
        "customer_name": order[4],
        "item_count": len(items),
    }


# ── Workflow 4: List Recent Orders ───────────────────────────────────────────

def workflow_list_recent(conn):
    """List recent orders with ORDER BY + LIMIT. Common dashboard query."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT o.id, c.name, o.status, o.total_amount, o.created_at
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            ORDER BY o.created_at DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()

    return {
        "workflow": "list_recent_orders",
        "result": "PASS" if len(rows) > 0 else "FAIL",
        "count": len(rows),
        "orders": [
            {"id": r[0], "customer": r[1], "status": r[2], "amount": r[3]}
            for r in rows
        ],
    }


# ── Runner ───────────────────────────────────────────────────────────────────

WORKFLOWS = {
    "create_order": workflow_create_order,
    "update_order": workflow_update_order,
    "query_order": workflow_query_order,
    "list_recent_orders": workflow_list_recent,
}


def run_workflows(conn, names):
    """Run specified workflows and return results."""
    results = []
    for name in names:
        fn = WORKFLOWS.get(name)
        if not fn:
            results.append({"workflow": name, "result": "ERROR", "reason": "unknown workflow"})
            continue
        try:
            start = time.time()
            result = fn(conn)
            result["duration_ms"] = round((time.time() - start) * 1000, 1)
            results.append(result)
        except Exception as e:
            results.append({
                "workflow": name,
                "result": "FAIL",
                "error": str(e),
            })
    return results


def main():
    parser = argparse.ArgumentParser(description="FalconDB Migration PoC — Demo App")
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--workflow", default="all",
                        help="Workflow name or 'all' (comma-separated)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    cfg = load_config(args.config)

    target = f"{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    if not args.json:
        print(f"\n  Connecting to {target} (user: {cfg['user']})")

    try:
        conn = connect(cfg)
    except Exception as e:
        print(f"  ERROR: Cannot connect to {target}: {e}", file=sys.stderr)
        sys.exit(1)

    if not args.json:
        print(f"  Connected.\n")

    if args.workflow == "all":
        names = list(WORKFLOWS.keys())
    else:
        names = [w.strip() for w in args.workflow.split(",")]

    results = run_workflows(conn, names)
    conn.close()

    if args.json:
        output = {
            "target": target,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "results": results,
        }
        print(json.dumps(output, indent=2, default=str))
    else:
        all_pass = True
        for r in results:
            status = r.get("result", "?")
            icon = "+" if status == "PASS" else ("~" if status == "SKIP" else "x")
            color = "PASS" if status == "PASS" else status
            dur = r.get("duration_ms", "")
            dur_str = f" ({dur}ms)" if dur else ""
            print(f"  [{icon}] {r['workflow']}: {color}{dur_str}")
            if status not in ("PASS", "SKIP"):
                all_pass = False
                if "error" in r:
                    print(f"      Error: {r['error']}")
        print("")
        if all_pass:
            print("  All workflows passed.")
        else:
            print("  Some workflows failed.")
            sys.exit(1)


if __name__ == "__main__":
    main()
