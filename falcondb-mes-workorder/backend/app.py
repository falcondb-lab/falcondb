"""
FalconDB MES — 工单 & 工序执行核心系统 REST API

Manufacturing Execution System: Work Order + Operation Reporting.
FalconDB is the sole business database. Every write is an explicit,
synchronous transaction. What the database confirms is production truth.

Endpoints:
  POST   /api/work-orders                  — Create work order + operations
  GET    /api/work-orders                  — List all work orders
  GET    /api/work-orders/{id}             — Get work order detail
  GET    /api/work-orders/{id}/operations  — List operations for a work order
  POST   /api/operations/{id}/start        — Start an operation (PENDING → RUNNING)
  POST   /api/operations/{id}/report       — Report production (报工)
  POST   /api/operations/{id}/complete     — Complete an operation (RUNNING → DONE)
  POST   /api/work-orders/{id}/complete    — Complete a work order
  GET    /api/work-orders/{id}/reports     — List all reports for a work order
  GET    /api/work-orders/{id}/state-log   — Audit trail for a work order
  GET    /api/work-orders/{id}/verify      — Run business invariant verification
  GET    /api/health                       — Health check
"""

from fastapi import FastAPI, HTTPException
from models import (
    WorkOrderCreate, WorkOrderOut, OperationOut,
    ReportCreate, ReportOut, StateLogOut, VerificationReport,
)
from db import transaction, read_cursor
import invariants

app = FastAPI(
    title="FalconDB MES — 工单执行系统",
    description="Manufacturing Execution System backed by FalconDB's Deterministic Commit Guarantee",
    version="1.0.0",
)


# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    """Database connectivity check."""
    try:
        with read_cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")


# ── Work Order CRUD ──────────────────────────────────────────────────────────

@app.post("/api/work-orders", response_model=WorkOrderOut, status_code=201)
def create_work_order(req: WorkOrderCreate):
    """Create a work order with its operations in a single transaction.

    Business rule: operations are created in the order provided.
    Each gets a sequential seq_no (1, 2, 3, ...).
    """
    with transaction() as (conn, cur):
        cur.execute(
            """INSERT INTO work_order (product_code, planned_qty)
               VALUES (%s, %s) RETURNING *""",
            (req.product_code, req.planned_qty),
        )
        wo = cur.fetchone()

        for i, op_name in enumerate(req.operations, start=1):
            cur.execute(
                """INSERT INTO operation (work_order_id, seq_no, operation_name)
                   VALUES (%s, %s, %s)""",
                (wo["work_order_id"], i, op_name),
            )

        # Log state creation
        cur.execute(
            """INSERT INTO work_order_state_log (work_order_id, from_status, to_status)
               VALUES (%s, '', 'CREATED')""",
            (wo["work_order_id"],),
        )

    return WorkOrderOut(**wo)


@app.get("/api/work-orders", response_model=list[WorkOrderOut])
def list_work_orders():
    with read_cursor() as cur:
        cur.execute("SELECT * FROM work_order ORDER BY work_order_id")
        return [WorkOrderOut(**r) for r in cur.fetchall()]


@app.get("/api/work-orders/{work_order_id}", response_model=WorkOrderOut)
def get_work_order(work_order_id: int):
    with read_cursor() as cur:
        cur.execute("SELECT * FROM work_order WHERE work_order_id = %s", (work_order_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Work order not found")
        return WorkOrderOut(**row)


@app.get("/api/work-orders/{work_order_id}/operations", response_model=list[OperationOut])
def list_operations(work_order_id: int):
    with read_cursor() as cur:
        cur.execute(
            "SELECT * FROM operation WHERE work_order_id = %s ORDER BY seq_no",
            (work_order_id,),
        )
        return [OperationOut(**r) for r in cur.fetchall()]


# ── Operation Lifecycle ──────────────────────────────────────────────────────

@app.post("/api/operations/{operation_id}/start", response_model=OperationOut)
def start_operation(operation_id: int):
    """Transition operation PENDING → RUNNING.

    Business rules enforced inside the transaction:
      - Operation must be PENDING.
      - All prior operations (lower seq_no) must be DONE.
      - Work order transitions CREATED → IN_PROGRESS on first start.
    """
    with transaction() as (conn, cur):
        # Lock the operation row
        cur.execute(
            "SELECT * FROM operation WHERE operation_id = %s FOR UPDATE",
            (operation_id,),
        )
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, "Operation not found")
        if op["status"] != "PENDING":
            raise HTTPException(
                409, f"Cannot start: operation is {op['status']}, expected PENDING"
            )

        # Check all prior operations are DONE
        cur.execute(
            """SELECT COUNT(*) AS pending
               FROM operation
               WHERE work_order_id = %s AND seq_no < %s AND status != 'DONE'""",
            (op["work_order_id"], op["seq_no"]),
        )
        if cur.fetchone()["pending"] > 0:
            raise HTTPException(
                409, "Cannot start: prior operations not yet completed"
            )

        # Transition operation
        cur.execute(
            """UPDATE operation SET status = 'RUNNING'
               WHERE operation_id = %s RETURNING *""",
            (operation_id,),
        )
        updated_op = cur.fetchone()

        # If work order is CREATED, transition to IN_PROGRESS
        cur.execute(
            "SELECT status FROM work_order WHERE work_order_id = %s FOR UPDATE",
            (op["work_order_id"],),
        )
        wo = cur.fetchone()
        if wo["status"] == "CREATED":
            cur.execute(
                "UPDATE work_order SET status = 'IN_PROGRESS' WHERE work_order_id = %s",
                (op["work_order_id"],),
            )
            cur.execute(
                """INSERT INTO work_order_state_log
                   (work_order_id, from_status, to_status)
                   VALUES (%s, 'CREATED', 'IN_PROGRESS')""",
                (op["work_order_id"],),
            )

    return OperationOut(**updated_op)


@app.post("/api/operations/{operation_id}/report", response_model=ReportOut)
def report_operation(operation_id: int, req: ReportCreate):
    """Submit a production report (报工).

    This is the soul of the system. Each report is an immutable production fact:
      "Operator X completed Y units at time T on operation Z."

    Business rules enforced inside the transaction:
      - Operation must be RUNNING.
      - report_qty must be > 0.
      - The report is INSERT-only (append to fact ledger).
      - work_order.completed_qty is updated atomically.
    """
    with transaction() as (conn, cur):
        # Lock operation
        cur.execute(
            "SELECT * FROM operation WHERE operation_id = %s FOR UPDATE",
            (operation_id,),
        )
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, "Operation not found")
        if op["status"] != "RUNNING":
            raise HTTPException(
                409, f"Cannot report: operation is {op['status']}, must be RUNNING"
            )

        # INSERT the immutable production fact
        cur.execute(
            """INSERT INTO operation_report
               (work_order_id, operation_id, report_qty, reported_by)
               VALUES (%s, %s, %s, %s) RETURNING *""",
            (op["work_order_id"], operation_id, req.report_qty, req.reported_by),
        )
        report = cur.fetchone()

        # Update work order completed_qty atomically
        cur.execute(
            """UPDATE work_order
               SET completed_qty = completed_qty + %s
               WHERE work_order_id = %s""",
            (req.report_qty, op["work_order_id"]),
        )

    return ReportOut(**report)


@app.post("/api/operations/{operation_id}/complete", response_model=OperationOut)
def complete_operation(operation_id: int):
    """Transition operation RUNNING → DONE.

    Business rule: operation must be RUNNING.
    Once DONE, it cannot go back.
    """
    with transaction() as (conn, cur):
        cur.execute(
            "SELECT * FROM operation WHERE operation_id = %s FOR UPDATE",
            (operation_id,),
        )
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, "Operation not found")
        if op["status"] != "RUNNING":
            raise HTTPException(
                409, f"Cannot complete: operation is {op['status']}, must be RUNNING"
            )

        cur.execute(
            """UPDATE operation SET status = 'DONE'
               WHERE operation_id = %s RETURNING *""",
            (operation_id,),
        )
        updated = cur.fetchone()

    return OperationOut(**updated)


# ── Work Order Completion ────────────────────────────────────────────────────

@app.post("/api/work-orders/{work_order_id}/complete", response_model=WorkOrderOut)
def complete_work_order(work_order_id: int):
    """Transition work order IN_PROGRESS → COMPLETED.

    Business rules:
      - All operations must be DONE.
      - Work order must be IN_PROGRESS.
      - Once COMPLETED, it cannot revert (invariant 3).
    """
    with transaction() as (conn, cur):
        cur.execute(
            "SELECT * FROM work_order WHERE work_order_id = %s FOR UPDATE",
            (work_order_id,),
        )
        wo = cur.fetchone()
        if not wo:
            raise HTTPException(404, "Work order not found")
        if wo["status"] != "IN_PROGRESS":
            raise HTTPException(
                409, f"Cannot complete: status is {wo['status']}, must be IN_PROGRESS"
            )

        # Verify all operations are DONE
        cur.execute(
            """SELECT COUNT(*) AS not_done
               FROM operation
               WHERE work_order_id = %s AND status != 'DONE'""",
            (work_order_id,),
        )
        if cur.fetchone()["not_done"] > 0:
            raise HTTPException(409, "Cannot complete: not all operations are DONE")

        cur.execute(
            """UPDATE work_order SET status = 'COMPLETED'
               WHERE work_order_id = %s RETURNING *""",
            (work_order_id,),
        )
        updated = cur.fetchone()

        cur.execute(
            """INSERT INTO work_order_state_log
               (work_order_id, from_status, to_status)
               VALUES (%s, 'IN_PROGRESS', 'COMPLETED')""",
            (work_order_id,),
        )

    return WorkOrderOut(**updated)


# ── Query Endpoints ──────────────────────────────────────────────────────────

@app.get("/api/work-orders/{work_order_id}/reports", response_model=list[ReportOut])
def list_reports(work_order_id: int):
    with read_cursor() as cur:
        cur.execute(
            """SELECT * FROM operation_report
               WHERE work_order_id = %s ORDER BY report_id""",
            (work_order_id,),
        )
        return [ReportOut(**r) for r in cur.fetchall()]


@app.get("/api/work-orders/{work_order_id}/state-log", response_model=list[StateLogOut])
def state_log(work_order_id: int):
    with read_cursor() as cur:
        cur.execute(
            """SELECT * FROM work_order_state_log
               WHERE work_order_id = %s ORDER BY event_id""",
            (work_order_id,),
        )
        return [StateLogOut(**r) for r in cur.fetchall()]


# ── Verification ─────────────────────────────────────────────────────────────

@app.get("/api/work-orders/{work_order_id}/verify", response_model=VerificationReport)
def verify_work_order(work_order_id: int):
    """Run all business invariants and return a structured PASS/FAIL report.

    This is the endpoint that proves FalconDB's DCG:
      After a kill -9, after failover, after anything —
      call this endpoint and see PASS.
    """
    with read_cursor() as cur:
        cur.execute("SELECT * FROM work_order WHERE work_order_id = %s", (work_order_id,))
        wo = cur.fetchone()
        if not wo:
            raise HTTPException(404, "Work order not found")

        cur.execute(
            "SELECT COALESCE(SUM(report_qty), 0) AS total FROM operation_report WHERE work_order_id = %s",
            (work_order_id,),
        )
        total_reported = cur.fetchone()["total"]

    result = invariants.verify_all(work_order_id)

    return VerificationReport(
        work_order_id=work_order_id,
        planned_qty=wo["planned_qty"],
        completed_qty=wo["completed_qty"],
        total_reported_qty=total_reported,
        status=wo["status"],
        **result,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False, log_level="info")
