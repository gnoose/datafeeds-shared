"""Bill

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""
from datetime import datetime
from sqlalchemy import JSON, func

from datafeeds.orm import ModelMixin, Base
from datafeeds.common.typing import BillingDatum

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from datafeeds import db
from datafeeds.models.utility_service import TND_ONLY, GENERATION_ONLY, UTILITY_BUNDLED

PARTIAL_BILL_PROVIDER_TYPES = [TND_ONLY, GENERATION_ONLY, UTILITY_BUNDLED]


class Bill(ModelMixin, Base):
    __tablename__ = "bill"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    attachments = sa.Column(JSON)
    closing = sa.Column(sa.Date)
    cost = sa.Column(sa.Float)
    initial = sa.Column(sa.Date)
    items = sa.Column(JSON)
    manual = sa.Column(sa.Boolean)
    modified = sa.Column(sa.DateTime)
    peak = sa.Column(sa.Float)
    service = sa.Column(sa.BigInteger)
    used = sa.Column(sa.Float)
    notes = sa.Column(sa.Unicode)
    visible = sa.Column(sa.Boolean, nullable=False, default=True)


class PartialBill(ModelMixin, Base):
    __tablename__ = "partial_bill"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    service = sa.Column(
        sa.BigInteger, sa.ForeignKey("utility_service.oid"), nullable=False
    )
    service_obj = relationship("UtilityService")
    attachments = sa.Column(JSON)
    closing = sa.Column(sa.Date)
    cost = sa.Column(sa.Float)
    initial = sa.Column(sa.Date)
    items = sa.Column(JSON)
    manual = sa.Column(sa.Boolean)
    modified = sa.Column(sa.DateTime)
    peak = sa.Column(sa.Float)
    used = sa.Column(sa.Float)
    notes = sa.Column(sa.Unicode)
    visible = sa.Column(sa.Boolean, nullable=False, default=True)
    # Date added to the db
    created = sa.Column(sa.DateTime, default=func.now())
    # Type of partial bill - is this a generation bill or a T&D bill?
    provider_type = sa.Column(sa.Enum(*PARTIAL_BILL_PROVIDER_TYPES))
    # Whether partial bill has been fully matched.  Its cost has been fully absorbed into totalized Bills.
    matched = sa.Column(sa.Boolean, nullable=False, default=False)
    # If the partial bill has been superseded by a newer bill, store its oid here.
    superseded_by = sa.Column(
        sa.BigInteger, sa.ForeignKey("partial_bill.oid"), nullable=True
    )

    @classmethod
    def generate(
        cls, service: int, provider_type: str, bill: BillingDatum
    ) -> "PartialBill":
        """Generates a partial bill for the service from the BillingDatum.
        """
        attachments = bill.attachments or []
        if attachments and bill.attachments[0] is None:
            attachments = []

        partial_bill = PartialBill(
            initial=bill.start,
            closing=bill.end,
            cost=bill.cost,
            used=bill.used,
            peak=bill.peak,
            modified=datetime.now(),
            manual=False,
            items=[
                {
                    "description": item.description,
                    "quantity": item.quantity,
                    "rate": item.rate,
                    "total": item.total,
                    "kind": item.kind,
                    "unit": item.unit,
                }
                for item in (bill.items or [])
            ],
            attachments=[
                {
                    "key": attachment.key,
                    "kind": attachment.kind,
                    "format": attachment.format,
                }
                for attachment in attachments
            ],
            service=service,
            provider_type=provider_type,
        )
        db.session.add(partial_bill)
        db.session.flush()
        return partial_bill

    def differs(self, other: BillingDatum) -> bool:
        """
        Compare a pending partial bill with the current partial bill
        to see if the key fields differ.

        Used to determine if the current partial bill should be replaced.
        """
        return (
            self.peak != other.peak
            or self.cost != other.cost
            or self.used != other.used
        )

    def matches(self, other: BillingDatum) -> bool:
        """
        Returns True if the key fields between the given partial bill
        and pending billing datum match.
        """
        return (
            self.peak == other.peak
            and self.cost == other.cost
            and self.used == other.used
            and self.initial == other.start
            and self.closing == other.end
        )

    def supersede(self, replacement: "PartialBill"):
        """
        Replace the current partial bill with a new partial bill.
        Mark the current partial bill with superseded_by and update its date modified.
        """
        self.superseded_by = replacement.oid
        self.modified = datetime.utcnow()
        db.session.add(self)
        db.session.flush()
