""" Meter

This module covers tables managed by webapps/platform that describe Meters, their interval data, and groups.
Except for unit tests, analytics should treat these tables as Read Only.
"""


import json
import math
from datetime import date, datetime, timedelta
from enum import Enum
from typing import List, Tuple, Optional, Dict

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from datafeeds import db
from datafeeds.orm import ModelMixin, Base
from datafeeds.models.utility_service import UtilityService


class MeterReading(ModelMixin, Base):
    __tablename__ = "meter_reading"

    oid = sa.Column(sa.BigInteger, primary_key=True)

    # foreign key for SQLAlchemy: does not actually exist in database
    meter = sa.Column(sa.BigInteger, sa.ForeignKey("meter.oid"))

    occurred = sa.Column(sa.Date)
    readings = sa.Column(sa.JSON)
    frozen = sa.Column(sa.Boolean)

    meter_obj = relationship("Meter", back_populates="readings")


class MeterFlowDirection(Enum):
    """Flow directions for a meter."""

    forward = "forward"
    reverse = "reverse"


class Meter(ModelMixin, Base):
    __tablename__ = "meter"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    billing = sa.Column(sa.Unicode)
    # foreign key for SQLAlchemy: does not actually exist in database
    # _building = sa.Column("building", sa.BigInteger, sa.ForeignKey("building.oid"))
    commodity = sa.Column(sa.Unicode)
    interval = sa.Column(sa.Integer)
    kind = sa.Column(sa.Unicode)
    name = sa.Column(sa.Unicode)
    number = sa.Column(sa.Unicode)
    parent = sa.Column(sa.BigInteger)
    point = sa.Column(sa.Unicode)
    # foreign key for SQLAlchemy: does not actually exist in database
    service = sa.Column(sa.BigInteger, sa.ForeignKey("utility_service.oid"))
    system = sa.Column(sa.BigInteger)
    direction = sa.Column(
        sa.Enum(*[f.value for f in MeterFlowDirection]), default="forward"
    )

    # building = relationship("Building", back_populates="meters")
    utility_service = relationship("UtilityService")
    readings = relationship("MeterReading", back_populates="meter_obj")

    snapmeter_account_meter = relationship("SnapmeterAccountMeter")

    def __init__(
        self,
        name,
        building=None,
        kind="main",
        interval=15,
        commodity="kw",
        direction="forward",
        utility_service=None,
        parent=None,
    ):
        self.oid = Meter.get_new_oid()

        if building:
            self.building = building

        self.name = name
        self.kind = kind
        self.interval = interval
        self.commodity = commodity
        self.direction = direction

        if utility_service is not None:
            self.utility_service = utility_service

        if parent is not None:
            self.parent = parent

    @classmethod
    def readings_sign(cls, direction):
        return -1 if direction == "reverse" else 1

    @property
    def unit_label(self):
        label = {"kw": "kW", "therms": "thm", "ccf": "ccf"}
        return label.get(self.commodity, self.commodity)

    @property
    def submeters(self):
        return db.session.query(Meter).filter(Meter.parent == self.oid)

    @property
    def timezone(self):
        """timezone from building, with default"""
        default_tz = "America/Los_Angeles"
        if not self.building:
            return default_tz
        return self.building.timezone or default_tz

    @property
    def bills_range(self):
        """return min initial/max closing bill dates meter as date properties

        usage: meter.bills_range.first_initial, meter.bills_range.first_closing,
        meter.bills_range.last_initial, meter.bills_range.last_closing
        """
        query = """
            select min(initial) first_initial, min(closing) first_closing,
                max(initial) last_initial, max(closing) last_closing
            from bill b, meter m
            where b.service=m.service and m.oid=:meter
        """
        return db.session.execute(query, {"meter": self.oid}).first()

    @classmethod
    def _replace_nan(cls, number):
        if number and math.isnan(number):
            return None

        return number

    @classmethod
    def _readings_query(cls):
        return """
            SELECT
                mr.meter meter_id,
                mr.occurred occurred,
                mr.readings::text,
                m.commodity,
                m.interval,
                m.direction
            FROM meter_reading mr, meter m
            WHERE
                m.oid = mr.meter AND
                m.kind <> 'totalized' AND
                m.oid IN :meters AND
                mr.occurred >= :from_date AND
                mr.occurred < :to_date
            union
            SELECT
                submeter.parent meter_id,
                mr.occurred occurred,
                mr.readings::text,
                m.commodity,
                m.interval,
                submeter.direction
            FROM meter_reading mr, meter m, meter submeter
            WHERE
                submeter.parent = m.oid AND
                submeter.oid = mr.meter AND
                m.oid IN :meters AND
                m.kind = 'totalized' AND
                mr.occurred >= :from_date AND
                mr.occurred < :to_date
            ORDER BY meter_id, occurred ASC
        """

    @classmethod
    def signed_readings(cls, readings_row, direction="forward"):
        """set the sign for an array of reading values (from meter_reading)

        use this before exporting or summing meter reading values; values from the database
        are always positive
        """
        rval = []
        sign = Meter.readings_sign(direction)
        readings = (
            json.loads(readings_row) if isinstance(readings_row, str) else readings_row
        )
        for val in readings:
            val = Meter._replace_nan(val)
            rval.append(sign * val if val else val)
        return rval

    def interval_data(
        self, start_dt: datetime, end_dt: datetime
    ) -> List[Tuple[datetime, Optional[float]]]:
        """Return ordered interval data between the two input datetimes.

        The intervals in this query are half-open. For example, when
        this method is called for interval data from 2018-10-01 to
        2018-10-03, we return all intervals in 10-01 and 10-02, but
        not midnight of 10-03.
        """

        # Meter readings are by day so expand our window to make sure
        # we capture all relevant intervals.
        start_d = start_dt.date()
        end_d = end_dt.date() + timedelta(days=1)

        query = db.session.execute(
            self._readings_query(),
            {"meters": (self.oid,), "from_date": start_d, "to_date": end_d},
        )
        # can return multiple rows per date for totalized meter
        by_date: Dict[date, list] = {}

        def add_readings(v1, v2):
            return None if v1 is None and v2 is None else (v1 or 0) + (v2 or 0)

        for row in query:
            readings = Meter.signed_readings(row.readings, row.direction)
            if row.occurred in by_date:
                by_date[row.occurred] = list(
                    map(add_readings, by_date[row.occurred], readings)
                )
            else:
                by_date[row.occurred] = readings

        if not by_date:
            return []

        intervals_per_day = int(24 * 60 / self.interval)
        num_intervals = int((end_d - start_d).total_seconds() / (self.interval * 60))
        start = datetime(start_d.year, start_d.month, start_d.day)
        step = timedelta(minutes=self.interval)
        labels = [start + k * step for k in range(0, num_intervals)]

        time_series: List[Optional[float]] = []
        day = start_d
        while day < end_d:
            if day in by_date:
                time_series += by_date[day]
            else:
                time_series += [None for _ in range(0, intervals_per_day)]

            day += timedelta(days=1)

        # Filter once more at the interval level.
        return [
            (dt, v) for (dt, v) in zip(labels, time_series) if start_dt <= dt < end_dt
        ]

    @property
    def readings_range(self):
        """return min/max reading dates (not datetime) for meter as date properties

        usage: meter.readings_range.min_date, meter.readings_range.max_date
        """
        query = """
            select min(occurred) min_date, max(occurred) max_date
            from meter_reading
            where meter=:meter
        """
        return db.session.execute(query, {"meter": self.oid}).first()

    @property
    def utility_account_id(self) -> str:
        query = db.session.query(UtilityService).filter_by(oid=self.service)
        service = query.first()
        if service:
            return service.utility_account_id
        return None

    @property
    def service_id(self) -> str:
        return self.utility_service.service_id
