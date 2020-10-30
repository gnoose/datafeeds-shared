"""Bill

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""
from typing import Dict, List, Union
from datetime import datetime
from sqlalchemy import JSON

from datafeeds.orm import ModelMixin, Base
from datafeeds.common.typing import (
    BillingDatum,
    AttachmentEntry,
    BillingDatumItemsEntry,
)

import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from datafeeds import db
from datafeeds.models.utility_service import (
    TND_ONLY,
    GENERATION_ONLY,
    UTILITY_BUNDLED,
    UtilityService,
)

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
    modified = sa.Column(sa.DateTime, default=func.now(), onupdate=func.now())
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
    tariff = sa.Column(sa.Unicode)
    # Date added to the db
    created = sa.Column(sa.DateTime, default=func.now())
    # Type of partial bill - is this a generation bill or a T&D bill?
    provider_type = sa.Column(sa.Enum(*PARTIAL_BILL_PROVIDER_TYPES))
    # If the partial bill has been superseded by a newer bill, store its oid here.
    superseded_by = sa.Column(
        sa.BigInteger, sa.ForeignKey("partial_bill.oid"), nullable=True
    )
    # service_id, tariff, utility, and utility_account_id are stored
    # here to preserve history
    service_id = sa.Column(sa.Unicode)
    utility = sa.Column(sa.Unicode)
    utility_account_id = sa.Column(sa.Unicode)

    # Utility's version of the tariff
    utility_code = sa.Column(sa.Unicode)

    utility_service = relationship("UtilityService")

    @classmethod
    def generate(
        cls, service: UtilityService, provider_type: str, bill: BillingDatum
    ) -> "PartialBill":
        """Generates a partial bill for the service from the BillingDatum.

        Caches the service_id, utility_account_id, and utility from the UtilityService record
        on the partial bill for record-keeping.

        If the utility code was scraped (the utility's version of the tariff), stash this on the partial as well.
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
            created=datetime.utcnow(),
            modified=datetime.utcnow(),
            manual=False,
            items=cls.map_line_items(bill.items),
            attachments=cls.map_attachments(attachments),
            service=service.oid,
            provider_type=provider_type,
            service_id=service.gen_service_id
            if provider_type == GENERATION_ONLY
            else service.service_id,
            utility_account_id=service.gen_utility_account_id
            if provider_type == GENERATION_ONLY
            else service.utility_account_id,
            utility=service.gen_utility
            if provider_type == GENERATION_ONLY
            else service.utility,
            utility_code=bill.utility_code or None,
        )
        db.session.add(partial_bill)
        db.session.flush()
        return partial_bill

    @staticmethod
    def map_attachments(attachments: List[AttachmentEntry]) -> List[Dict[str, str]]:
        return [
            {
                "key": attachment.key,
                "kind": attachment.kind,
                "format": attachment.format,
            }
            for attachment in attachments
        ]

    @staticmethod
    def map_line_items(
        items: List[BillingDatumItemsEntry],
    ) -> List[Dict[str, Union[str, float]]]:
        return [
            {
                "description": item.description,
                "quantity": item.quantity,
                "rate": item.rate,
                "total": item.total,
                "kind": item.kind,
                "unit": item.unit,
            }
            for item in (items or [])
        ]

    def differs(self, other: BillingDatum) -> bool:
        """
        Compare a pending partial bill with the current partial bill
        to see if the key fields differ.

        Used to determine if the current partial bill should be replaced.
        Not considering service_id, utility_account_id, or utility in whether
        to supersede the partial bill, because these attributes are not scraped,
        but pulled from the current utility_service.
        """
        return (
            self.peak != other.peak
            or self.cost != other.cost
            or self.used != other.used
            or self.attachments != (self.map_attachments(other.attachments or []))
            or self.items != (self.map_line_items(other.items or []))
            or self.utility_code != other.utility_code
        )

    def matches(self, other: BillingDatum) -> bool:
        """
        Returns True if the key fields between the given partial bill
        and pending billing datum match.

        Used to determine if a partial bill is already staged to be created
        during a scraper run.
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
