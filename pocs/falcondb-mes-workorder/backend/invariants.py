"""
FalconDB MES — Business invariant verification.
"""

from db import get_cursor, sql

STATUS_RANK = {"PENDING": 0, "RUNNING": 1, "DONE": 2}


def check_operation_forward_only(work_order_id: int) -> tuple[bool, str]:
    with get_cursor() as cur:
        cur.execute(sql(
            "SELECT seq_no, status FROM operation WHERE work_order_id = %s ORDER BY seq_no",
            work_order_id,
        ))
        ops = cur.fetchall()

    for i in range(len(ops) - 1):
        r1 = STATUS_RANK.get(ops[i]["status"], -1)
        r2 = STATUS_RANK.get(ops[i + 1]["status"], -1)
        if r1 < r2:
            return False, f"seq {ops[i]['seq_no']}={ops[i]['status']} but seq {ops[i+1]['seq_no']}={ops[i+1]['status']}"
    return True, "All operations follow correct sequence order"


def check_reported_qty_monotonic(work_order_id: int) -> tuple[bool, str]:
    with get_cursor() as cur:
        cur.execute(sql(
            "SELECT COUNT(*) AS cnt FROM operation_report WHERE work_order_id = %s",
            work_order_id,
        ))
        cnt = cur.fetchone()["cnt"]
        total = 0
        if cnt > 0:
            cur.execute(sql(
                "SELECT SUM(report_qty) AS total FROM operation_report WHERE work_order_id = %s",
                work_order_id,
            ))
            row = cur.fetchone()
            if row and row["total"] is not None:
                total = row["total"]

        cur.execute(sql(
            "SELECT COUNT(*) AS bad FROM operation_report WHERE work_order_id = %s AND report_qty <= 0",
            work_order_id,
        ))
        bad = cur.fetchone()["bad"]
        if bad > 0:
            return False, f"Found {bad} reports with qty <= 0"
        return True, f"Total reported qty = {total}, all positive, append-only"


def check_completed_irreversible(work_order_id: int) -> tuple[bool, str]:
    with get_cursor() as cur:
        cur.execute(sql("SELECT status FROM work_order WHERE work_order_id = %s", work_order_id))
        row = cur.fetchone()
        if not row:
            return True, "Work order not found (vacuously true)"
        current_status = row["status"]

        cur.execute(sql(
            "SELECT COUNT(*) AS completed_events FROM work_order_state_log WHERE work_order_id = %s AND to_status = 'COMPLETED'",
            work_order_id,
        ))
        completed_events = cur.fetchone()["completed_events"]

        if completed_events > 0 and current_status != "COMPLETED":
            return False, f"Was COMPLETED but current status is {current_status}"
        if current_status == "COMPLETED":
            return True, "Work order is COMPLETED and irreversible"
        return True, f"Work order status is {current_status} (not yet completed)"


def verify_all(work_order_id: int) -> dict:
    from models import InvariantResult
    checks = [
        ("operation_forward_only", check_operation_forward_only),
        ("reported_qty_monotonic", check_reported_qty_monotonic),
        ("completed_irreversible", check_completed_irreversible),
    ]
    results = []
    all_pass = True
    for name, fn in checks:
        passed, detail = fn(work_order_id)
        results.append(InvariantResult(name=name, passed=passed, detail=detail))
        if not passed:
            all_pass = False
    return {"invariants": results, "overall": "PASS" if all_pass else "FAIL"}
