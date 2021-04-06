from datetime import datetime
from enum import Enum
from typing import Optional, Dict, TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from datafeeds import db
from datafeeds.orm import ModelMixin, Base


if TYPE_CHECKING:
    from datafeeds.models.meter import Meter
    from datafeeds.models.bill import Bill

NOT_ENROLLED = "notEnrolled"
OPS_BILL_AUDIT = "opsbillaudit"


class WorkflowState(Enum):
    pending = "pending"
    quarantined = "quarantined"
    review = "review"
    done = "done"


class AuditVerdict(Enum):
    failed = "failed"
    warning = "warning"
    passed = "passed"
    error = "error"


class BillAuditEvent(ModelMixin, Base):
    __tablename__ = "bill_audit_event"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    occurred = sa.Column(sa.DateTime)
    source = sa.Column(sa.Unicode)
    description = sa.Column(sa.Unicode)
    meta = sa.Column(sa.JSON)
    audit = sa.Column(
        sa.BigInteger, sa.ForeignKey("bill_audit.oid"), nullable=False
    )  # Unenforced foreign key.

    audit_obj = relationship("BillAudit", back_populates="events")

    @staticmethod
    def generate(
        audit: "BillAudit", description: str, meta: Dict = None
    ) -> "BillAuditEvent":
        return BillAuditEvent(
            occurred=datetime.utcnow(),
            source="datafeeds",
            description=description,
            meta=meta,
            audit_obj=audit,
        )


class BillAudit(ModelMixin, Base):
    __tablename__ = "bill_audit"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    workflow_state = sa.Column(sa.Enum(WorkflowState))
    audit_verdict = sa.Column(sa.Enum(AuditVerdict))
    audit_issues = sa.Column(sa.JSON)
    audit_errors = sa.Column(sa.JSON)
    modified = sa.Column(sa.DateTime)
    latest_audit = sa.Column(sa.DateTime)
    bill = sa.Column(sa.BigInteger)
    # bill_service, bill_initial, account_hex, and account_name and building_name
    # are cached on the bill audit model for easier querying
    bill_service = sa.Column(sa.BigInteger)
    bill_initial = sa.Column(sa.Date)
    account_hex = sa.Column(sa.Unicode)
    account_name = sa.Column(sa.Unicode)
    building_name = sa.Column(sa.Unicode)
    utility = sa.Column(sa.Unicode)

    events = relationship("BillAuditEvent", back_populates="audit_obj")

    @staticmethod
    def find(b: "Bill") -> Optional["BillAudit"]:
        return db.session.query(BillAudit).filter(BillAudit.bill == b.oid).first()

    @staticmethod
    def initialize_bill_audit_workflow(
        b: Optional["Bill"], meter: "Meter"
    ) -> Optional["BillAudit"]:
        """
        Generate a new audit workflow for a bill, if one does not already exist.
        """
        if b is None:
            return None

        audit = BillAudit.find(b)

        if audit is None:
            audit = BillAudit.generate(b, meter)
            db.session.add(audit)

            event = BillAuditEvent.generate(audit, "Initialized bill audit.")
            audit.events = [event]
            db.session.add(event)
            db.session.flush()
            return audit

        return None

    @staticmethod
    def generate(bill: "Bill", meter: "Meter") -> "BillAudit":
        account = meter.first_snapmeter_account

        return BillAudit(
            workflow_state=WorkflowState.pending,
            audit_verdict=None,
            audit_issues=None,
            modified=datetime.now(),
            latest_audit=None,
            bill=bill.oid,
            bill_service=bill.service,
            bill_initial=bill.initial,
            account_hex=account.hex_id if account is not None else None,
            account_name=account.name if account is not None else None,
            building_name=meter.building.name if meter is not None else None,
            utility=meter.utility_service.utility if meter is not None else None,
        )
