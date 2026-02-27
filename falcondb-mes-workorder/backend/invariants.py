"""
FalconDB MES — Business invariant verification.

Four invariants that must hold at ALL times, including after failover:

  1. Operation status only moves forward (PENDING → RUNNING → DONE)
  2. Cumulative reported quantity is monotonically non-decreasing
  3. COMPLETED work orders cannot revert
  4. All of the above hold after failover (verified by re-running 1-3)
"""

from db import read_cursor


def check_operation_forward_only(work_order_id: int) -> tuple[bool, str]:
    """Invariant 1: No operation has gone backward in status."""
    with read_cursor() as cur:
        # Check for any operation whose status is inconsistent with sequence:
        # A DONE op must not have a later-seq op in RUNNING, and no DONE op
        # should appear before a PENDING op in the sequence.
        cur.execute("""
            SELECT o1.operation_id, o1.seq_no, o1.status AS s1,
                   o2.operation_id AS o2_id, o2.seq_no AS seq2, o2.status AS s2
            FROM operation o1
            JOIN operation o2 ON o1.work_order_id = o2.work_order_id
            WHERE o1.work_order_id = %s
              AND o1.seq_no < o2.seq_no
              AND (
                  (o1.status = 'PENDING' AND o2.status IN ('RUNNING', 'DONE'))
               OR (o1.status = 'RUNNING' AND o2.status = 'DONE')
              )
        """, (work_order_id,))
        violations = cur.fetchall()
        if violations:
            detail = "; ".join(
                f"seq {v['seq_no']}={v['s1']} but seq {v['seq2']}={v['s2']}"
                for v in violations
            )
            return False, f"Order violation: {detail}"
        return True, "All operations follow correct sequence order"


def check_reported_qty_monotonic(work_order_id: int) -> tuple[bool, str]:
    """Invariant 2: SUM(report_qty) can only increase (append-only table)."""
    with read_cursor() as cur:
        cur.execute("""
            SELECT COALESCE(SUM(report_qty), 0) AS total
            FROM operation_report
            WHERE work_order_id = %s
        """, (work_order_id,))
        total = cur.fetchone()["total"]

        # Since the table is append-only, we verify no negative qty exists
        cur.execute("""
            SELECT COUNT(*) AS bad
            FROM operation_report
            WHERE work_order_id = %s AND report_qty <= 0
        """, (work_order_id,))
        bad = cur.fetchone()["bad"]
        if bad > 0:
            return False, f"Found {bad} reports with qty <= 0"
        return True, f"Total reported qty = {total}, all positive, append-only"


def check_completed_irreversible(work_order_id: int) -> tuple[bool, str]:
    """Invariant 3: A COMPLETED work order never reverts."""
    with read_cursor() as cur:
        cur.execute("""
            SELECT status FROM work_order WHERE work_order_id = %s
        """, (work_order_id,))
        row = cur.fetchone()
        if not row:
            return True, "Work order not found (vacuously true)"

        current_status = row["status"]

        # Check state log: if COMPLETED ever appeared, current must be COMPLETED
        cur.execute("""
            SELECT COUNT(*) AS completed_events
            FROM work_order_state_log
            WHERE work_order_id = %s AND to_status = 'COMPLETED'
        """, (work_order_id,))
        completed_events = cur.fetchone()["completed_events"]

        if completed_events > 0 and current_status != "COMPLETED":
            return False, (
                f"Work order was COMPLETED ({completed_events} events) "
                f"but current status is {current_status}"
            )
        if current_status == "COMPLETED":
            return True, "Work order is COMPLETED and irreversible"
        return True, f"Work order status is {current_status} (not yet completed)"


def verify_all(work_order_id: int) -> dict:
    """Run all invariants and return a structured report."""
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

    return {
        "invariants": results,
        "overall": "PASS" if all_pass else "FAIL",
    }
