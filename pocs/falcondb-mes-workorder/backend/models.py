"""
FalconDB MES — Pydantic request/response models.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


# ── Work Order ───────────────────────────────────────────────────────────────

class WorkOrderCreate(BaseModel):
    product_code: str = Field(..., min_length=1, max_length=64)
    planned_qty: int = Field(..., gt=0)
    operations: list[str] = Field(
        ...,
        min_length=1,
        description="Ordered list of operation names, e.g. ['cutting', 'welding', 'assembly', 'inspection']",
    )


class WorkOrderOut(BaseModel):
    work_order_id: int
    product_code: str
    planned_qty: int
    completed_qty: int
    status: str
    created_at: datetime


# ── Operation ────────────────────────────────────────────────────────────────

class OperationOut(BaseModel):
    operation_id: int
    work_order_id: int
    seq_no: int
    operation_name: str
    status: str


# ── Operation Report (报工) ──────────────────────────────────────────────────

class ReportCreate(BaseModel):
    report_qty: int = Field(..., gt=0)
    reported_by: str = Field(..., min_length=1, max_length=64)


class ReportOut(BaseModel):
    report_id: int
    work_order_id: int
    operation_id: int
    report_qty: int
    reported_by: str
    reported_at: datetime


# ── State Log ────────────────────────────────────────────────────────────────

class StateLogOut(BaseModel):
    event_id: int
    work_order_id: int
    from_status: str
    to_status: str
    event_time: datetime


# ── Verification ─────────────────────────────────────────────────────────────

class InvariantResult(BaseModel):
    name: str
    passed: bool
    detail: str


class VerificationReport(BaseModel):
    work_order_id: int
    planned_qty: int
    completed_qty: int
    total_reported_qty: int
    status: str
    invariants: list[InvariantResult]
    overall: str  # "PASS" or "FAIL"
