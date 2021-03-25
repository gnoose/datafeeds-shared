"""
This module contains models of data transmitted to us by PG&E via ShareMyData.
Datafeeds should treat these models as read-only.
"""
import logging
from datetime import datetime, date, timedelta

import attr
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB  # type: ignore
from sqlalchemy import func, orm

from typing import Dict, Any, Optional, List

from datafeeds import db
from datafeeds.common.typing import BillingDatum
from datafeeds.common.typing import BillingDatumItemsEntry
from datafeeds.models import UtilityService
from datafeeds.orm import ModelMixin, Base

log = logging.getLogger(__name__)


class GreenButtonProvider(ModelMixin, Base):
    __tablename__ = "green_button_provider"

    oid = sa.Column(sa.Integer, primary_key=True)

    # The Platform utility plugin string, eg: "utility:pge"
    utility = sa.Column(sa.Unicode, nullable=False)

    # The authorized third-party provider, eg: "gridium"
    identifier = sa.Column(sa.Unicode, nullable=False)


class Artifact(ModelMixin, Base):
    """An artifact captures a single XML file generated by the Share My Data service."""

    __tablename__ = "smd_artifact"

    oid = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)

    provider_oid = sa.Column(
        "provider",
        sa.BigInteger,
        sa.ForeignKey("green_button_provider.oid"),
        nullable=False,
    )
    provider = orm.relationship("GreenButtonProvider", lazy="joined")

    # Filename should capture where the file is stored in S3. (ex. s3://my-bucket/some-dir/abcd-12345.xml)
    filename = sa.Column(sa.Unicode, nullable=False)
    # URL is the PG&E URL used to retrieve the document
    url = sa.Column(sa.Unicode)

    created = sa.Column(
        sa.DateTime, default=func.now()
    )  # When was record was added to our system?
    published = sa.Column(sa.DateTime)  # When did PG&E create this data?


@attr.s(frozen=True)
class LineItem:
    note: str = attr.ib()
    unit: str = attr.ib()
    amount: float = attr.ib()  # Dollars
    quantity: float = attr.ib()  # Amount of the commodity that was used.

    @staticmethod
    def from_json(record: dict) -> Optional["LineItem"]:
        """Deserialize a line item from a JSON record."""
        fields = ["note", "unit", "amount", "quantity"]
        if any(f not in record for f in fields):
            return None

        return LineItem(**record)

    @property
    def kind(self) -> str:
        if "demand" in self.note.lower():
            return "demand"
        elif "energy charge" in self.note.lower():
            return "use"

        return "other"

    def to_billing_datum_items_entry(self) -> Optional[BillingDatumItemsEntry]:
        """Translate this line item to datafeeds' intermediate representation of a bill line item."""

        # Line item data from SMD is not guaranteed to be clean; this is a best-effort
        # method of converting line items for debugging purposes.
        # pylint: disable=no-member
        try:
            if self.unit.lower() in ("w", "wh"):
                quantity = float(self.quantity) / 1000.0
                unit = "k" + self.unit
            else:
                quantity = float(self.quantity)
                unit = self.unit

            return BillingDatumItemsEntry(
                description=self.note,
                quantity=quantity,
                rate=None,  # Not available.
                total=self.amount,
                kind=self.kind,
                unit=unit,
            )
        except:  # noqa: E722
            return None
        # pylint: enable=no-member


class Bill(ModelMixin, Base):

    """A bill record sent to us from PG&E via Share My Data."""

    __tablename__ = "smd_bill"

    oid = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)

    identifier = sa.Column(sa.Unicode)

    subscription = sa.Column(sa.Unicode, nullable=False)
    usage_point = sa.Column(sa.Unicode, nullable=False)

    start = sa.Column(sa.DateTime)
    duration = sa.Column(sa.Interval)  # Interval is the SQL equivalent of a datetime.

    used = sa.Column(sa.Float)
    used_unit = sa.Column(sa.Unicode)

    # Costs are in dollars.
    cost = sa.Column(sa.Float)
    cost_additional = sa.Column(sa.Float)
    _line_items = sa.Column(
        "line_items", JSONB
    )  # This is a list of dictionaries, one per line item.

    # Utility's version of the tariff - not our Gridium tariff
    tariff = sa.Column(sa.Unicode)

    # Metadata

    # This is the self URL presented in the customer agreement that generated this record.
    self_url = sa.Column("self_url", sa.Unicode, nullable=False)

    # # These fields tell us which XML file generated this record.
    artifact_oid = sa.Column(
        "artifact",
        sa.BigInteger,
        sa.ForeignKey("smd_artifact.oid", ondelete="CASCADE"),
        nullable=False,
    )
    artifact = orm.relationship("Artifact", lazy="joined")

    created = sa.Column(
        sa.DateTime, default=func.now()
    )  # When was record was added to our system?
    published = sa.Column(sa.DateTime)  # When did PG&E create this data?

    @property
    def safe_published(self) -> datetime:
        """Return the time of publication, or the Unix epoch start if none is available.

        This property is useful for sorting by publication time, where Nones cause issues.
        """
        if self.published is not None:
            return self.published

        return datetime(1970, 1, 1)

    def __str__(self):
        return (
            "<SMD Bill subscription: %s, usage_point: %s, dates: %s - %s cost: %s>"
            % (
                self.subscription,
                self.usage_point,
                self.start,
                self.start + self.duration,
                self.cost,
            )
        )

    def as_dict(self) -> Dict[str, Any]:
        return dict(
            identifier=self.identifier,
            subscription=self.subscription,
            usage_point=self.usage_point,
            start=self.start,
            duration=self.duration,
            used=self.used,
            used_unit=self.used_unit,
            cost=self.cost,
            cost_additional=self.cost_additional,
            line_items=self.line_items,
            tariff=self.tariff,
            self_url=self.self_url,
            published=self.published,
        )

    def __eq__(self, other):
        return isinstance(other, Bill) and self.as_dict() == other.as_dict()

    # The initial and closing properties are a bit confusing. SMD bills appear to be offset by PG&E paper bills
    # by one day. For example, if the paper bill says Feb 1 to Feb 28, then the SMD bill will show the bill as starting
    # Jan 31. We would prefer that Gridium bills match the PDF dates, so we adjust the start date here, while
    # guaranteeing that for all bills initial <= closing.

    @property
    def initial(self) -> date:
        return min((self.start + timedelta(days=1)).date(), self.closing)

    @property
    def closing(self) -> date:
        return (self.start + self.duration).date()

    @property
    def line_items(self) -> List[LineItem]:
        if self._line_items is None:
            return []

        line_items: List[LineItem] = [LineItem.from_json(x) for x in self._line_items]
        return [x for x in line_items if x is not None]

    @property
    def is_nem(self) -> Optional[bool]:
        """Returns True if we think the meter is an NEM meter.

        Not guaranteed to get all NEM meters, but 95% of meters with a reverse flow channel
        are also NEM. NEM tariffs are sometimes present as well.
        """
        has_nem_tariff = self.tariff and "NEM" in self.tariff
        if has_nem_tariff:
            return has_nem_tariff

        has_reverse_flow_channel = (
            db.session.query(IntervalData)
            .filter(
                IntervalData.usage_point == self.usage_point,
                IntervalData.reading_type_oid == ReadingType.oid,
                ReadingType.flow_direction == "reverse",
                self.start <= IntervalData.start,
                IntervalData.start <= self.start + self.duration,
            )
            .first()
        )

        return has_reverse_flow_channel

    @property
    def is_partial(self) -> Optional[bool]:
        """Returns True if third party indicators found in line items.

        Return None (unknown) for gas meters and NEM meters.
        """
        if self.used_unit == "therm":
            return None

        indicators_found = any(
            line.get("note", "").lower()
            in ("generation credit", "power cost incentive adjustment", "pcia")
            for line in self._line_items or []
        )

        if self.is_nem and not indicators_found:
            # NEM meters often have sparse line items.  An NEM meter with no third party indicators
            # could still be on a CCA.
            return None

        return indicators_found

    def overlaps(self, other: "Bill") -> bool:
        if not isinstance(other, Bill):
            return False

        return (
            other.initial <= self.initial <= other.closing
            or other.initial <= self.closing <= other.closing
            or self.initial <= other.initial <= self.closing
        )

    @staticmethod
    def unify_bills(data: List["Bill"]) -> List["Bill"]:
        """Convert the input list of ShareMyData bills to a chronological list.

        Later publication dates have higher precedence. Bills must not overlap, so a
        bill will displace any overlapping bill of lower precedence.
        """

        chronological_bills = sorted(
            data, key=lambda b: (b.safe_published, b.start), reverse=True
        )
        results: List["Bill"] = []

        for b in chronological_bills:
            if any(other.overlaps(b) for other in results):
                continue
            results.append(b)

        return list(reversed(results))

    @staticmethod
    def adjust_single_day_bills(data: List["Bill"]) -> List["Bill"]:
        """Adjusts the list of chronological bills so there are no one-day bills.
        We expect that the start and end dates are not the same.
        """
        # Sort by start, as incoming data is sorted by published
        sort_by_start = sorted(data, key=lambda b: b.start)
        results: List["Bill"] = []

        prev = None
        for b in sort_by_start:
            if prev and prev.closing == b.initial:
                b.start += timedelta(days=1)
                b.duration -= timedelta(days=1)

            if b.duration == timedelta(1):
                # SMD bill initial will be shifted one day forward, so a one-day duration
                # needs to be two days, so the start and end differ.
                b.duration = timedelta(2)

            prev = b
            results.append(b)
        return results

    def customer_info(self) -> Optional["CustomerInfo"]:
        """Returns the corresponding CustomerInfo record.

        Attempts to find a Customer Info record whose "published date" directly precedes the Bill's start on the
        same usage_point at the same address, with a buffer of 45 days.

        If no match is found, returns the earliest Customer Info record for this usage point.
        """
        service_address = (
            db.session.query(CustomerInfo.customer_name)
            .filter(
                CustomerInfo.usage_point == self.usage_point,
                CustomerInfo.subscription == self.subscription,
            )
            .order_by(CustomerInfo.published.asc())
            .first()
        )

        if self.start and service_address:
            preceding_record = (
                db.session.query(CustomerInfo)
                .filter(
                    CustomerInfo.usage_point == self.usage_point,
                    CustomerInfo.customer_name == service_address[0],
                    CustomerInfo.published <= self.start + timedelta(days=45),
                )
                .order_by(CustomerInfo.published.desc())
                .limit(1)
            ).first()
            if preceding_record is None:
                # return first record
                first = (
                    db.session.query(CustomerInfo)
                    .filter(
                        CustomerInfo.usage_point == self.usage_point,
                        CustomerInfo.customer_name == service_address[0],
                    )
                    .order_by(CustomerInfo.published.asc())
                    .limit(1)
                    .first()
                )
                log.debug(
                    "Returning first SMD Customer Info record: SAID %s, Published %s, Bill Start %s.",
                    first.service_id,
                    first.published,
                    self.start,
                )
                return first
            log.debug(
                "Returning preceding SMD Customer Info record: SAID %s, Published %s, Bill Start %s.",
                preceding_record.service_id,
                preceding_record.published,
                self.start,
            )
            return preceding_record
        else:
            log.debug(
                "Cannot locate SMD Customer Info Record: Address %s, Bill Start %s.",
                service_address,
                self.start,
            )
        return None

    def to_billing_datum(
        self, service: Optional[UtilityService] = None
    ) -> BillingDatum:
        line_items = [li.to_billing_datum_items_entry() for li in self.line_items]

        customer_info = self.customer_info()

        utility_account_id = None
        service_id = None

        if customer_info:
            service_id = customer_info.service_id

            utility_account_id = customer_info.customer_account_id
            if (
                service
                and utility_account_id
                and utility_account_id in service.utility_account_id
            ):
                utility_account_id = service.utility_account_id

        return BillingDatum(
            start=self.initial,
            end=self.closing,
            statement=self.closing,
            cost=self.cost,
            peak=self.peak,
            used=round(self.used / 1000)
            if self.used_unit.lower() == "wh"
            else self.used,
            items=[li for li in line_items if li is not None],
            attachments=None,
            utility_code=self.tariff,
            utility="utility:pge",
            utility_account_id=utility_account_id,
            service_id=service_id,
            third_party_expected=self.is_partial,
        )

    @property
    def peak(self) -> Optional[float]:
        largest = None
        factor = 1.0

        for item in self.line_items:
            if (
                item.kind == "demand"
                and isinstance(item.quantity, float)
                and (largest is None or largest <= item.quantity)
            ):
                factor = (
                    0.001 if item.unit.lower() == "w" else 1.0
                )  # Convert to kW if necessary.
                largest = item.quantity

        return round(largest * factor) if largest else None


class CustomerInfo(ModelMixin, Base):
    __tablename__ = "smd_customer_info"

    oid = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)

    #####################################
    # Green Button Customer Information #
    #####################################

    subscription = sa.Column(sa.Unicode, nullable=False)
    customer_name = sa.Column(sa.Unicode)
    customer_account_id = sa.Column(sa.Unicode)

    # Obfuscated SAID identifier. The "identifier" of the customer agreement in PG&E SMD documentation.
    usage_point = sa.Column(sa.Unicode, nullable=False)

    # PG&E SAID (utility_service.service_id, "name" of the customer agreement in PG&E SMD documentation.)
    service_id = sa.Column(sa.Unicode, nullable=False)

    street1 = sa.Column(sa.Unicode)
    street2 = sa.Column(sa.Unicode)
    city = sa.Column(sa.Unicode)
    state = sa.Column(sa.Unicode)
    zipcode = sa.Column(sa.Unicode)

    # This is the first time we should expect data for this meter (in UTC).
    service_start = sa.Column(sa.DateTime)
    status = sa.Column(sa.Unicode)

    # This is a serial number associated with the physical meter. (Required for rate changes.)
    # This is a list of serial numbers.
    # meter_serials = sa.Column(JSONB)

    #################################
    # Green Button Staging Metadata #
    #################################

    # This is the self URL presented in the customer agreement that generated this record.
    self_url = sa.Column("self_url", sa.Unicode, nullable=False)

    # These fields tell us which XML file generated this record.
    artifact_oid = sa.Column(
        "artifact",
        sa.BigInteger,
        sa.ForeignKey("smd_artifact.oid", ondelete="CASCADE"),
        nullable=False,
    )
    artifact = orm.relationship("Artifact", lazy="joined")

    created = sa.Column(
        sa.DateTime, default=func.now()
    )  # When was this record was added to our system?
    published = sa.Column(sa.DateTime)  # When did PG&E create this data?

    def __str__(self):
        return "<CustomerInfo Subscription: %s, Usage Point: %s, SAID: %s>" % (
            self.subscription,
            self.usage_point,
            self.service_id,
        )

    def as_dict(self):
        return dict(
            subscription=self.subscription,
            customer_name=self.customer_name,
            customer_account_id=self.customer_account_id,
            usage_point=self.usage_point,
            service_id=self.service_id,
            street1=self.street1,
            street2=self.street2,
            city=self.city,
            state=self.state,
            zipcode=self.zipcode,
            service_start=self.service_start,
            status=self.status,
            meter_serials=self.meter_serials,
            self_url=self.self_url,
            published=self.published,
        )

    def __eq__(self, other):
        return isinstance(other, CustomerInfo) and self.as_dict() == other.as_dict()

    @classmethod
    def from_self_url(cls, self_url: str) -> Optional["CustomerInfo"]:
        return (
            db.session.query(CustomerInfo)
            .filter(CustomerInfo.self_url == self_url)
            .order_by(sa.desc(CustomerInfo.created))
            .first()
        )


class IntervalData(ModelMixin, Base):
    __tablename__ = "smd_interval_data"

    oid = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)

    subscription = sa.Column(sa.Unicode, nullable=False)
    usage_point = sa.Column(sa.Unicode, nullable=False)

    start = sa.Column(sa.DateTime)
    duration = sa.Column(sa.Interval)  # Interval is the SQL equivalent of a timedelta.

    reading_type_oid = sa.Column(
        "reading_type",
        sa.BigInteger,
        sa.ForeignKey("smd_reading_type.oid"),
        nullable=False,
    )

    reading_type = orm.relationship("ReadingType", lazy="joined")

    readings = sa.Column(JSONB)

    # This is the self URL for the interval block that generated this record.
    self_url = sa.Column("self_url", sa.Unicode, nullable=False)

    # These fields tell us which XML file generated this record.
    artifact_oid = sa.Column(
        "artifact",
        sa.BigInteger,
        sa.ForeignKey("smd_artifact.oid", ondelete="CASCADE"),
        nullable=False,
    )
    artifact = orm.relationship("Artifact", lazy="joined")

    created = sa.Column(
        sa.DateTime, default=func.now()
    )  # When was record was added to our system?
    published = sa.Column(sa.DateTime)  # When did PG&E create this data?

    @property
    def safe_published(self) -> datetime:
        """Return the time of publication, or the Unix epoch start if none is available.

        This property is useful for sorting by publication time, where Nones cause issues.
        """
        if self.published is not None:
            return self.published

        return datetime(1970, 1, 1)

    def as_dict(self) -> Dict[str, Any]:
        return dict(
            subscription=self.subscription,
            usage_point=self.usage_point,
            start=self.start,
            duration=self.duration,
            reading_type=self.reading_type.as_dict(),
            readings=self.readings,
            self_url=self.self_url,
            published=self.published,
        )


class ReadingType(ModelMixin, Base):
    __tablename__ = "smd_reading_type"

    oid = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)

    identifier = sa.Column(sa.Unicode)
    accumulation_behaviour = sa.Column(sa.Unicode)
    commodity = sa.Column(sa.Unicode)
    flow_direction = sa.Column(sa.Unicode)
    kind = sa.Column(sa.Unicode)
    unit_of_measure = sa.Column(sa.Unicode)
    interval_length = sa.Column(sa.Integer)
    power_of_ten_multiplier = sa.Column(sa.Integer)

    # These fields tell us which XML file generated this record.
    artifact_oid = sa.Column(
        "artifact",
        sa.BigInteger,
        sa.ForeignKey("smd_artifact.oid", ondelete="CASCADE"),
        nullable=False,
    )
    artifact = orm.relationship("Artifact", lazy="joined")

    # This is the self URL presented in the customer agreement that generated this record.
    self_url = sa.Column("self_url", sa.Unicode, nullable=False)

    created = sa.Column(
        sa.DateTime, default=func.now()
    )  # When was record was added to our system?
    published = sa.Column(sa.DateTime)  # When did PG&E create this data?

    def __str__(self):
        return "<ReadingType id: %s, commodity: %s, flow: %s, interval: %s>" % (
            self.identifier,
            self.commodity,
            self.flow_direction,
            self.interval_length,
        )

    def as_dict(self) -> Dict[str, Any]:
        return dict(
            identifier=self.identifier,
            accumulation_behaviour=self.accumulation_behaviour,
            commodity=self.commodity,
            flow_direction=self.flow_direction,
            kind=self.kind,
            unit_of_measure=self.unit_of_measure,
            interval_length=self.interval_length,
            power_of_ten_multiplier=self.power_of_ten_multiplier,
            self_url=self.self_url,
            published=self.published,
        )
