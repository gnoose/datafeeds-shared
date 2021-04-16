"""Bill

This module covers tables managed by webapps that describe bills and partial bills..
Except for unit tests, analytics should treat these tables as Read Only.

Bills and Partial Bills are created in both Webapps and Datafeeds.  Webapps Bill
Creation Code has been copied and adapted here.
"""
import logging
from enum import Enum
from typing import Dict, List, Union, NamedTuple, Optional, Tuple, Any, TYPE_CHECKING
from datetime import datetime, timedelta

import numpy as np
from sqlalchemy import JSON
from sqlalchemy.ext.associationproxy import association_proxy

from datafeeds.models.billaudit import BillAudit
from datafeeds.models.meter import ProductEnrollment
from datafeeds.orm import ModelMixin, Base
from datafeeds.common.typing import (
    BillingDatum,
    AttachmentEntry,
    BillingDatumItemsEntry,
    BillingData,
    NoFutureBillsError,
)

import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref

from datafeeds import db
from datafeeds.models.utility_service import UtilityService

log = logging.getLogger(__name__)


if TYPE_CHECKING:
    from datafeeds.models.meter import Meter


class InvalidBillError(Exception):
    pass


def check_no_future_bills(uncommitted: List["Bill"]):
    """
    Requirement: incoming bills cannot end in the future.
    """
    today = datetime.today().date()
    for bill in uncommitted:
        if bill.closing > today:
            raise NoFutureBillsError()


def validate_incoming_bills(
    uncommitted: List["Bill"], service_oid: int
) -> List["Bill"]:
    """Verifies incoming bills don't overlap and sorts chronologically."""
    to_add: List[Bill] = []
    for b in sorted(uncommitted, key=lambda x: x.initial):
        for other in to_add:
            if b.overlaps(other):
                message = (
                    "New bills must not overlap. %s-%s vs. %s-%s for service %s."
                    % (
                        b.initial,
                        b.closing,
                        other.initial,
                        other.closing,
                        service_oid,
                    )
                )
                raise InvalidBillError(message)
        to_add.append(b)
    return to_add


class IncomingBillSummary(NamedTuple):
    bill: "Bill"
    new: Optional[bool] = False
    duplicate: Optional["Bill"] = None
    update: Optional["Bill"] = None
    overlaps: Optional[List["Bill"]] = []
    skip: Optional[bool] = False


def process_incoming_bills(
    service: int, uncommitted: List["Bill"], source: Optional[str] = None
) -> List["IncomingBillSummary"]:
    """Pre-processes incoming bills.  Called by Bill._add_service_bills."""
    incoming_bills = validate_incoming_bills(uncommitted, service)
    check_no_future_bills(uncommitted)

    existing = (
        db.session.query(Bill)
        .filter(service == Bill.service)
        .order_by(Bill.initial.asc())
        .all()
    )

    existing_initial = np.array([b.initial for b in existing])
    existing_closing = np.array([b.closing for b in existing])

    summary = []
    for bill in incoming_bills:
        bill.source = source

        duplicate = None
        update = None
        overlaps = []

        same_date_index = np.where(
            ((bill.initial == existing_initial) & (bill.closing == existing_closing))
        )[0]

        if same_date_index.size == 1:
            existing_bill = existing[same_date_index[0]]
            # Webapps does not have the bad_usage_override check.
            skip = not existing_bill.safe_override(
                bill
            ) or existing_bill.bad_usage_override(bill)

            if existing_bill.values_match(bill):
                duplicate = existing_bill
            else:
                update = existing_bill
        else:
            overlapping_indices = np.where(
                (bill.initial <= existing_closing) & (existing_initial <= bill.closing)
            )
            overlaps = [existing[i] for i in overlapping_indices[0]]
            skip = any([not overlap.safe_override(bill) for overlap in overlaps])

        bs = IncomingBillSummary(
            bill=bill,
            new=not (duplicate or update or overlaps),
            duplicate=duplicate,
            update=update,
            overlaps=overlaps,
            skip=skip,
        )
        summary.append(bs)
    return summary


def create_bill_audit_records(
    meter: "Meter", incoming_summary: List[IncomingBillSummary]
):
    """
    For meters enrolled in bill audit, create bill audit records if they don't exist for new/updated bills.

    Hide newly-created bills from the customer.
    """
    enrolled = (
        db.session.query(ProductEnrollment)
        .filter_by(meter=meter.oid, product="opsbillaudit", status="active")
        .first()
    )

    if not enrolled:
        return

    for summary in incoming_summary:
        if summary.duplicate or summary.skip:
            continue

        bill = summary.update if summary.update else summary.bill
        audit = BillAudit.initialize_bill_audit_workflow(bill, meter)
        if audit and summary.new:
            # If we create a new audit workflow, we need to make a decision about whether to render
            # the bill as "visible" (meaning, visible to a customer). The current thinking is, we
            # don't want to hide an existing bill that got updated. Therefore, we want to only
            # hide newly created bills.
            bill.visible = False
            db.session.add(bill)
            log.info(
                "Hiding incoming bill on service %s (%s - %s : $%s).",
                bill.service,
                bill.initial,
                bill.closing,
                bill.cost,
            )


def extract_bill_attributes(bill: "Bill") -> Tuple:
    return bill.service, bill.initial, bill.closing, bill.cost


class PartialBillProviderType(Enum):
    TND_ONLY = "tnd-only"
    GENERATION_ONLY = "generation-only"

    # Usually the Enum names are used in the database, but we already have values with dashes in the
    # database. These can't be used as class member names.
    @classmethod
    def values(cls):
        return [f.value for f in PartialBillProviderType]


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
    utility_code = sa.Column(sa.Unicode, nullable=True)
    gen_utility_code = sa.Column(sa.Unicode, nullable=True)
    # Used to indicate that we believe we've received all partial
    # bill components, where applicable.
    has_all_charges = sa.Column(sa.Boolean, nullable=True)

    # Used to indicate that analytics has synthesized a "cost curve" plot for this bill and stored it in a
    # designated S3 bucket for display on the frontend.
    has_cost_curve = sa.Column(sa.Boolean, nullable=True)

    created = sa.Column(sa.DateTime, default=func.now(), nullable=True)
    source = sa.Column(sa.Unicode, default="webapps", nullable=True)
    tnd_cost = sa.Column(sa.Float, nullable=True)  # Intermediate T&D costs
    gen_cost = sa.Column(sa.Float, nullable=True)  # Intermediate generation costs

    # TODO update SQL file

    @staticmethod
    def add_bills(
        uncommitted: List["Bill"], source: Optional[str] = None
    ) -> Optional[Tuple[List["Bill"], List[Dict[str, Any]]]]:
        return Bill._add_service_bills(uncommitted, source)

    @staticmethod
    def _add_service_bills(
        uncommitted: List["Bill"], source: Optional[str] = None
    ) -> Optional[Tuple[List["Bill"], List[Dict[str, Any]]]]:
        """Reconciles incoming bills with existing bills in the database.

        :return: A list of Bills that were created or updated, and a list of change records to send to Elasticsearch.
        """
        from datafeeds.models import Meter

        if not uncommitted or not uncommitted[0].service:
            return None

        service_oid = uncommitted[0].service
        incoming_summary = process_incoming_bills(service_oid, uncommitted, source)

        insertable: List[Bill] = []
        meter = (
            db.session.query(Meter)
            .filter(Meter.kind != "sub", Meter.service == service_oid)
            .first()
        )
        extra = meter.build_log_extra if meter else {}

        for incoming in incoming_summary:
            incoming_attrs = extract_bill_attributes(incoming.bill)

            if incoming.new:
                db.session.add(incoming.bill)
                extra["evt"] = {"name": {"create"}}
                log.info(
                    "Creating bill for service %s (%s - %s : $%s).",
                    *incoming_attrs,
                    extra=extra,
                )
                insertable.append(incoming.bill)
            elif incoming.skip:
                extra["evt"] = {"name": {"skip overlap"}}
                log.info(
                    "Skipping new bill for service %s (%s - %s : $%s), cannot override existing.",
                    *incoming_attrs,
                    extra=extra,
                )
            elif incoming.duplicate:
                extra["evt"] = {"name": {"match"}}
                log.info(
                    "Skipping new bill for service %s (%s - %s : $%s), matches current bill.",
                    *incoming_attrs,
                    extra=extra,
                )
            elif incoming.update:
                incoming.update.copy_values(incoming.bill)
                extra["evt"] = {"name": {"update"}}
                log.info(
                    "Updating bill for service %s (%s - %s : $%s).",
                    *incoming_attrs,
                    extra=extra,
                )
                insertable.append(incoming.update)

            else:
                for existing in incoming.overlaps:
                    db.session.delete(existing)
                    extra["evt"] = {"name": {"delete"}}
                    log.info(
                        "Deleting bill for service %s (%s - %s : $%s), overlapped by a more recent bill.",
                        *extract_bill_attributes(existing),
                        extra=extra,
                    )
                db.session.add(incoming.bill)
                insertable.append(incoming.bill)
                extra["evt"] = {"name": {"create"}}
                log.info(
                    "Creating bill for service %s (%s - %s : $%s).",
                    *incoming_attrs,
                    extra=extra,
                )

        db.session.flush()
        if meter:
            create_bill_audit_records(meter=meter, incoming_summary=incoming_summary)
        return insertable, []

    def overlaps(self, other: "Bill") -> bool:
        if not isinstance(other, Bill):
            return False

        return (
            other.initial <= self.initial <= other.closing
            or other.initial <= self.closing <= other.closing
            or self.initial <= other.initial <= self.closing
        )

    def values_match(self, other: "Bill") -> bool:
        """Returns True if the attributes and values on "other" are a subset of attributes on the current bill."""
        other_dict = other.__dict__.copy()
        current_dict = self.__dict__.copy()
        other_dict.pop("_sa_instance_state", None)
        other_dict.pop("created", None)  # Just in case this was set
        other_dict.pop("modified", None)  # Just in case this was set
        current_dict.pop("_sa_instance_state", None)
        if not getattr(other, "attachments"):
            other_dict.pop("attachments", None)
        if not getattr(other, "items"):
            other_dict.pop("line_items", None)
        return other_dict.items() <= current_dict.items()

    def copy_values(self, other: "Bill"):
        incoming_fields = other.__dict__.copy()
        incoming_fields.pop("_sa_instance_state", None)
        incoming_fields.pop("attachments", None)
        incoming_fields.pop("items", None)
        incoming_fields.pop("modified", None)
        incoming_fields.pop("created", None)
        for field in incoming_fields:
            setattr(self, field, getattr(other, field))
        # copy attachments and items only if they add info
        for field in ["attachments", "items"]:
            if getattr(other, field):
                setattr(self, field, getattr(other, field))
        db.session.add(self)

    def bad_usage_override(self, replacement: "Bill") -> bool:
        # Cost of incoming bill is non-zero but use is zero. Comparison bill should have the same dates.
        return replacement.cost != 0 and replacement.used == 0 and self.used != 0

    def safe_override(self, replacement: "Bill") -> bool:
        """Returns whether the current bill can be overridden with the replacement bill. The replacement is assumed
        to overlap.

        - Incoming manual bills can override anything
        - Incoming regular bills cannot override bills from billing streams.
        - Otherwise, scraped bills can be replaced.
        - Manual bills can additional be overridden with a "totalized" bill from billing streams if key fields match.
             We allow this because a lot of manual bills exist where missing charges had to be added by hand.
             Billing streams may be able to pull in the missing charges, so replacing the manual
             bill with the "totalized" one will give us a more detailed breakdown of the charges.
        """
        if replacement.manual:
            # Incoming manual bills can replace any existing
            return True

        if len(self.active_partial_bills) > 0 and not getattr(
            replacement, "cached_partial", None
        ):
            # Regular incoming bills shouldn't override bills that were summed in billing streams. For example, a bill
            # that was created from summing SMD T&D charges with CCA charges, shouldn't later be overridden with a
            # regular bill from SMD.
            return False

        if not self.manual:
            # Otherwise, "scraped" bills can be overriden - order of these conditionals is important here.
            return True

        if getattr(
            replacement, "cached_partial", None
        ):  # Indicator that incoming bill is from "billing streams"
            # Carefully override manual bills with totalized bills where key fields match
            safe = bool(
                round(self.used or 0) == round(replacement.used or 0)
                and self.cost
                and round(self.cost)
                in [
                    round(replacement.tnd_cost or 0),
                    round(replacement.gen_cost or 0),
                    round(replacement.cost or 0),
                ]
            )

            if safe:
                log.info(
                    "Overriding manual bill for service %s (%s - %s : $%s, Used: %s). Replacing with stitched bill.",
                    self.service,
                    self.initial,
                    self.closing,
                    self.cost,
                    self.used,
                )
            return safe
        return False

    @property
    def active_partial_bills(self) -> List[PartialBill]:
        """Returns Partial Bills linked to the Bill that are active (as we keep older ones around for
        record-keeping).

        Partial Bill must be "visible" and Partial Bill must not have been superseded by another, more
        current Partial Bill.
        """
        return list(
            filter(lambda pb: pb.visible and not pb.superseded_by, self.partial_bills)
        )

    partial_bills = association_proxy(
        "partial_bill_links",
        "partial_bill_obj",
        creator=lambda pb: PartialBillLink(partial_bill_obj=pb),  # type: ignore
    )


class PartialBillLink(ModelMixin, Base):
    """Linking table connecting bills to partial bills, and vice versa.

    Bills are comprised of multiple partial bills. Because dates won't always line up, partial bills
    can likewise belong to multiple bills.
    """

    __tablename__ = "partial_bill_link"

    oid = sa.Column(sa.Integer, primary_key=True)
    bill = sa.Column(sa.BigInteger, sa.ForeignKey("bill.oid"))
    partial_bill = sa.Column(sa.BigInteger, sa.ForeignKey("partial_bill.oid"))
    created = sa.Column(sa.DateTime, default=func.now())

    bill_obj = relationship(
        Bill, backref=backref("partial_bill_links", cascade="all,delete")
    )
    partial_bill_obj = relationship(
        PartialBill, backref=backref("partial_bill_links", cascade="all,delete")
    )


def snap_first_start(
    billing_data: BillingData, existing: Union[List[Bill], List[PartialBill]]
) -> BillingData:
    """
    Adjusts the start date of the first Billing Datum, such that its start date
    is equal to the end date of an existing partial bill/bill. This helps us
    "snap" the start date into the existing bill timeline.

    One day is added to the start date, if applicable.
    """
    if not billing_data:
        return billing_data

    list.sort(billing_data, key=lambda b: b.start)
    incoming = billing_data[0]

    start = incoming.start
    if isinstance(start, datetime):
        start = start.date()

    for existing_bill in existing:
        if start == existing_bill.closing:
            new_bill_start = incoming.start + timedelta(days=1)
            billing_data[0] = incoming._replace(start=new_bill_start)
            log.info(
                "Snapped the start date of the first new bill to {}".format(
                    new_bill_start
                )
            )

            if incoming.end - new_bill_start < timedelta(days=1):
                raise Exception(
                    "Snapping start date would create bill ({} - {}) because of existing "
                    "bill ({} - {}).".format(
                        new_bill_start,
                        incoming.end,
                        existing_bill.initial,
                        existing_bill.closing,
                    )
                )
            return billing_data
    return billing_data
