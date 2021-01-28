"""Bill

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""
from enum import Enum
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
from datafeeds.models.utility_service import UtilityService


class PartialBillProviderType(Enum):
    TND_ONLY = "tnd-only"
    GENERATION_ONLY = "generation-only"

    # Usually the Enum names are used in the database, but we already have values with dashes in the
    # database. These can't be used as class member names.
    @classmethod
    def values(cls):
        return [f.value for f in PartialBillProviderType]


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
    provider_type = sa.Column(sa.Enum(*PartialBillProviderType.values()))
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
    third_party_expected = sa.Column(sa.Boolean)

    utility_service = relationship("UtilityService")

    @classmethod
    def generate(
        cls,
        service: UtilityService,
        provider_type: PartialBillProviderType,
        bill: BillingDatum,
    ) -> "PartialBill":
        """Generates a partial bill for the service from the BillingDatum.

        Caches the service_id, utility_account_id, and utility from the UtilityService record
        on the partial bill for record-keeping.

        If the utility code was scraped (the utility's version of the tariff), stash this on the partial as well.
        """
        attachments = bill.attachments or []
        if attachments and bill.attachments[0] is None:
            attachments = []

        # If service_id was scraped, use this, otherwise, pull from the UtilityService record.
        service_id = bill.service_id
        if service_id is None:
            service_id = (
                service.gen_service_id
                if provider_type == PartialBillProviderType.GENERATION_ONLY
                else service.service_id
            )

        # If utility was scraped, use this, otherwise, pull from the UtilityService record.
        utility = bill.utility
        if utility is None:
            utility = (
                service.gen_utility
                if provider_type == PartialBillProviderType.GENERATION_ONLY
                else service.utility
            )

        # If utility account id was scraped, use this, otherwise, pull from the UtilityService record.
        utility_account_id = bill.utility_account_id
        if utility_account_id is None:
            if (
                provider_type == PartialBillProviderType.GENERATION_ONLY
                and service.gen_utility_account_id
            ):
                utility_account_id = service.gen_utility_account_id
            else:
                utility_account_id = service.utility_account_id

        partial_bill = PartialBill(
            initial=bill.start,
            closing=bill.end,
            cost=round(bill.cost, 2),
            used=round(bill.used, 4) if bill.used else bill.used,
            peak=round(bill.peak, 4) if bill.peak else bill.peak,
            created=datetime.utcnow(),
            modified=datetime.utcnow(),
            manual=False,
            items=cls.map_line_items(bill.items),
            attachments=cls.map_attachments(attachments),
            service=service.oid,
            provider_type=provider_type.value,
            service_id=service_id,
            utility_account_id=utility_account_id,
            utility=utility,
            utility_code=bill.utility_code or None,
            third_party_expected=bill.third_party_expected,
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

    @staticmethod
    def sort_items(
        items: List[Dict[str, Union[str, float]]]
    ) -> List[Dict[str, Union[str, float]]]:
        """Sort line items by description and total for better comparison"""
        try:
            return sorted(items, key=lambda k: (k["description"], k["total"]))
        except KeyError:
            return items

    def differs(self, other: BillingDatum) -> bool:
        """
        Compare a pending partial bill with the current partial bill
        to see if the key fields differ. Used to determine if the current partial bill should be replaced.

        In some cases, we check to make sure an incoming attribute is being scraped,
        before comparing the current value to existing value. For example, say we have a partial bill
        where we can't scrape the service_id, so we just populate it with the UtilityService.service_id by default.
        The next time this meter is scraped, the scraped service_id will still be None, but will differ
        from the current service_id cached from the service. This would cause us to keep superseding the
        existing partial bill, even though the scraped information was not changing.
        """
        return (
            self.peak != other.peak
            or self.cost != other.cost
            or self.used != other.used
            or self.attachments != (self.map_attachments(other.attachments or []))
            or self.sort_items(self.items or [])
            != self.sort_items((self.map_line_items(other.items or [])))
            or (
                other.utility_code is not None
                and self.utility_code != other.utility_code
            )
            or (other.service_id is not None and self.service_id != other.service_id)
            or (other.utility is not None and self.utility != other.utility)
            or (
                other.utility_account_id is not None
                and self.utility_account_id != other.utility_account_id
            )
            or self.third_party_expected != other.third_party_expected
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
