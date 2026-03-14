"""
FalconDB MES — 工单 & 工序执行核心系统 REST API
"""

from datetime import datetime
from fastapi import FastAPI, HTTPException
from models import (
    WorkOrderCreate, WorkOrderOut, OperationOut,
    ReportCreate, ReportOut, StateLogOut, VerificationReport,
)
from db import get_cursor, sql
import invariants

app = FastAPI(title="FalconDB MES", version="1.0.0")


@app.get("/api/health")
def health():
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")


@app.post("/api/work-orders", response_model=WorkOrderOut, status_code=201)
def create_work_order(req: WorkOrderCreate):
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM work_order")
        wo_id = cur.fetchone()["cnt"] + 1
        cur.execute(sql(
            "INSERT INTO work_order (work_order_id, product_code, planned_qty) VALUES (%s, %s, %s)",
            wo_id, req.product_code, req.planned_qty,
        ))

        cur.execute("SELECT COUNT(*) AS cnt FROM operation")
        op_base = cur.fetchone()["cnt"]
        for i, op_name in enumerate(req.operations, start=1):
            cur.execute(sql(
                "INSERT INTO operation (operation_id, work_order_id, seq_no, operation_name) VALUES (%s, %s, %s, %s)",
                op_base + i, wo_id, i, op_name,
            ))

        cur.execute("SELECT COUNT(*) AS cnt FROM work_order_state_log")
        ev_id = cur.fetchone()["cnt"] + 1
        cur.execute(sql(
            "INSERT INTO work_order_state_log (event_id, work_order_id, from_status, to_status) VALUES (%s, %s, %s, %s)",
            ev_id, wo_id, "", "CREATED",
        ))

    return WorkOrderOut(
        work_order_id=wo_id, product_code=req.product_code,
        planned_qty=req.planned_qty, completed_qty=0,
        status="CREATED", created_at=datetime.now(),
    )


@app.get("/api/work-orders", response_model=list[WorkOrderOut])
def list_work_orders():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM work_order ORDER BY work_order_id")
        return [WorkOrderOut(**r) for r in cur.fetchall()]


@app.get("/api/work-orders/{work_order_id}", response_model=WorkOrderOut)
def get_work_order(work_order_id: int):
    with get_cursor() as cur:
        cur.execute(sql("SELECT * FROM work_order WHERE work_order_id = %s", work_order_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Work order not found")
        return WorkOrderOut(**row)


@app.get("/api/work-orders/{work_order_id}/operations", response_model=list[OperationOut])
def list_operations(work_order_id: int):
    with get_cursor() as cur:
        cur.execute(sql(
            "SELECT * FROM operation WHERE work_order_id = %s ORDER BY seq_no",
            work_order_id,
        ))
        return [OperationOut(**r) for r in cur.fetchall()]


@app.post("/api/operations/{operation_id}/start", response_model=OperationOut)
def start_operation(operation_id: int):
    with get_cursor() as cur:
        cur.execute(sql("SELECT * FROM operation WHERE operation_id = %s", operation_id))
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, "Operation not found")
        if op["status"] != "PENDING":
            raise HTTPException(409, f"Cannot start: operation is {op['status']}")

        cur.execute(sql(
            "SELECT COUNT(*) AS pending FROM operation WHERE work_order_id = %s AND seq_no < %s AND status != 'DONE'",
            op["work_order_id"], op["seq_no"],
        ))
        if cur.fetchone()["pending"] > 0:
            raise HTTPException(409, "Cannot start: prior operations not yet completed")

        cur.execute(sql("UPDATE operation SET status = 'RUNNING' WHERE operation_id = %s", operation_id))

        cur.execute(sql("SELECT status FROM work_order WHERE work_order_id = %s", op["work_order_id"]))
        wo = cur.fetchone()
        if wo and wo["status"] == "CREATED":
            cur.execute(sql("UPDATE work_order SET status = 'IN_PROGRESS' WHERE work_order_id = %s", op["work_order_id"]))
            cur.execute("SELECT COUNT(*) AS cnt FROM work_order_state_log")
            ev_id = cur.fetchone()["cnt"] + 1
            cur.execute(sql(
                "INSERT INTO work_order_state_log (event_id, work_order_id, from_status, to_status) VALUES (%s, %s, 'CREATED', 'IN_PROGRESS')",
                ev_id, op["work_order_id"],
            ))

    return OperationOut(
        operation_id=operation_id, work_order_id=op["work_order_id"],
        seq_no=op["seq_no"], operation_name=op["operation_name"], status="RUNNING",
    )


@app.post("/api/operations/{operation_id}/report", response_model=ReportOut)
def report_operation(operation_id: int, req: ReportCreate):
    with get_cursor() as cur:
        cur.execute(sql("SELECT * FROM operation WHERE operation_id = %s", operation_id))
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, "Operation not found")
        if op["status"] != "RUNNING":
            raise HTTPException(409, f"Cannot report: operation is {op['status']}, must be RUNNING")

        cur.execute("SELECT COUNT(*) AS cnt FROM operation_report")
        rpt_id = cur.fetchone()["cnt"] + 1
        now = datetime.now()
        cur.execute(sql(
            "INSERT INTO operation_report (report_id, work_order_id, operation_id, report_qty, reported_by) VALUES (%s, %s, %s, %s, %s)",
            rpt_id, op["work_order_id"], operation_id, req.report_qty, req.reported_by,
        ))

        cur.execute(sql(
            "UPDATE work_order SET completed_qty = completed_qty + %s WHERE work_order_id = %s",
            req.report_qty, op["work_order_id"],
        ))

    return ReportOut(
        report_id=rpt_id, work_order_id=op["work_order_id"],
        operation_id=operation_id, report_qty=req.report_qty,
        reported_by=req.reported_by, reported_at=now,
    )


@app.post("/api/operations/{operation_id}/complete", response_model=OperationOut)
def complete_operation(operation_id: int):
    with get_cursor() as cur:
        cur.execute(sql("SELECT * FROM operation WHERE operation_id = %s", operation_id))
        op = cur.fetchone()
        if not op:
            raise HTTPException(404, "Operation not found")
        if op["status"] != "RUNNING":
            raise HTTPException(409, f"Cannot complete: operation is {op['status']}, must be RUNNING")

        cur.execute(sql("UPDATE operation SET status = 'DONE' WHERE operation_id = %s", operation_id))

    return OperationOut(
        operation_id=operation_id, work_order_id=op["work_order_id"],
        seq_no=op["seq_no"], operation_name=op["operation_name"], status="DONE",
    )


@app.post("/api/work-orders/{work_order_id}/complete", response_model=WorkOrderOut)
def complete_work_order(work_order_id: int):
    with get_cursor() as cur:
        cur.execute(sql("SELECT * FROM work_order WHERE work_order_id = %s", work_order_id))
        wo = cur.fetchone()
        if not wo:
            raise HTTPException(404, "Work order not found")
        if wo["status"] != "IN_PROGRESS":
            raise HTTPException(409, f"Cannot complete: status is {wo['status']}, must be IN_PROGRESS")

        cur.execute(sql(
            "SELECT COUNT(*) AS not_done FROM operation WHERE work_order_id = %s AND status != 'DONE'",
            work_order_id,
        ))
        if cur.fetchone()["not_done"] > 0:
            raise HTTPException(409, "Cannot complete: not all operations are DONE")

        cur.execute(sql("UPDATE work_order SET status = 'COMPLETED' WHERE work_order_id = %s", work_order_id))

        cur.execute("SELECT COUNT(*) AS cnt FROM work_order_state_log")
        ev_id = cur.fetchone()["cnt"] + 1
        cur.execute(sql(
            "INSERT INTO work_order_state_log (event_id, work_order_id, from_status, to_status) VALUES (%s, %s, 'IN_PROGRESS', 'COMPLETED')",
            ev_id, work_order_id,
        ))

    return WorkOrderOut(
        work_order_id=work_order_id, product_code=wo["product_code"],
        planned_qty=wo["planned_qty"], completed_qty=wo["completed_qty"],
        status="COMPLETED", created_at=wo["created_at"],
    )


@app.get("/api/work-orders/{work_order_id}/reports", response_model=list[ReportOut])
def list_reports(work_order_id: int):
    with get_cursor() as cur:
        cur.execute(sql(
            "SELECT * FROM operation_report WHERE work_order_id = %s ORDER BY report_id",
            work_order_id,
        ))
        return [ReportOut(**r) for r in cur.fetchall()]


@app.get("/api/work-orders/{work_order_id}/state-log", response_model=list[StateLogOut])
def state_log(work_order_id: int):
    with get_cursor() as cur:
        cur.execute(sql(
            "SELECT * FROM work_order_state_log WHERE work_order_id = %s ORDER BY event_id",
            work_order_id,
        ))
        return [StateLogOut(**r) for r in cur.fetchall()]


@app.get("/api/work-orders/{work_order_id}/verify", response_model=VerificationReport)
def verify_work_order(work_order_id: int):
    with get_cursor() as cur:
        cur.execute(sql("SELECT * FROM work_order WHERE work_order_id = %s", work_order_id))
        wo = cur.fetchone()
        if not wo:
            raise HTTPException(404, "Work order not found")

        cur.execute(sql(
            "SELECT COUNT(*) AS cnt FROM operation_report WHERE work_order_id = %s",
            work_order_id,
        ))
        report_count = cur.fetchone()["cnt"]

        total_reported = 0
        if report_count > 0:
            cur.execute(sql(
                "SELECT SUM(report_qty) AS total FROM operation_report WHERE work_order_id = %s",
                work_order_id,
            ))
            row = cur.fetchone()
            if row and row["total"] is not None:
                total_reported = row["total"]

    result = invariants.verify_all(work_order_id)

    return VerificationReport(
        work_order_id=work_order_id,
        planned_qty=wo["planned_qty"],
        completed_qty=wo["completed_qty"],
        total_reported_qty=total_reported,
        status=wo["status"],
        **result,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False, log_level="info")
