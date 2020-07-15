from datetime import date, datetime, timedelta
from typing import Dict, List, Set
import unittest

from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.common.exceptions import InvalidMeterDataException
from datafeeds.models import Meter
from datafeeds.models.meter import MeterReading


class SaveReadingsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        meter = Meter(
            commodity="kw",
            interval=15,
            kind="main",
            name="Test Meter 1-%s" % datetime.now().strftime("%s"),
        )
        db.session.add(meter)
        db.session.flush()
        self.meter_id = meter.oid
        # create readings for a week ago
        dt = date.today() - timedelta(days=14)
        for idx in range(7):
            db.session.add(
                MeterReading(
                    meter=meter.oid,
                    occurred=dt,
                    readings=[1.0] * 96,
                    frozen=False,
                    modified=datetime(dt.year, dt.month, dt.day),
                )
            )
            dt += timedelta(days=1)
        db.session.flush()

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_parse_readings(self):
        """Parse meter readings in scraper format into MeterReading objects."""
        dt = (datetime.today() - timedelta(days=3)).date()
        # data size doesn't match meter interval
        readings: Dict[str, List[float]] = {dt.strftime("%Y-%m-%d"): [1.0] * 24}
        with self.assertRaises(InvalidMeterDataException):
            MeterReading.from_json(self.meter_id, readings)
        # values are not floats
        readings: Dict[str, List[float]] = {dt.strftime("%Y-%m-%d"): ["1.0" * 96]}
        with self.assertRaises(InvalidMeterDataException):
            MeterReading.from_json(self.meter_id, readings)
        # valid data
        valid_dates: Set[date] = set()
        for idx in range(3):
            valid_dates.add(dt)
            readings[dt.strftime("%Y-%m-%d")] = [2.0] * 96
            dt += timedelta(days=1)
        # empty data ignored
        readings[dt.strftime("%Y-%m-%d")] = []
        readings[(dt + timedelta(days=1)).strftime("%Y-%m-%d")] = [None] * 96
        meter_readings: List[MeterReading] = MeterReading.from_json(
            self.meter_id, readings
        )
        self.assertEqual(3, len(meter_readings))
        for row in meter_readings:
            self.assertIn(row.occurred, valid_dates)
            self.assertEqual([2.0] * 96, row.readings)
            self.assertFalse(row.frozen)

    def test_new_readings(self):
        """New readings can be added."""
        query = db.session.query(MeterReading).filter_by(meter=self.meter_id)
        self.assertEqual(7, query.count(), "7 readings from setup")
        dt = (datetime.today() - timedelta(days=3)).date()
        start_dt = dt
        readings: Dict[str, List[float]] = {}
        for idx in range(3):
            readings[dt.strftime("%Y-%m-%d")] = [2.0] * 96
            dt += timedelta(days=1)
        MeterReading.merge_readings(MeterReading.from_json(self.meter_id, readings))
        db.session.flush()
        self.assertEqual(10, query.count(), "10 readings after save")
        for row in query.filter(MeterReading.occurred < start_dt):
            self.assertEqual(
                96.0,
                sum(row.readings),
                "existing readings for %s unchanged" % row.occurred,
            )
            self.assertEqual(row.occurred, row.modified.date())
        for row in query.filter(MeterReading.occurred >= start_dt):
            self.assertEqual(
                96.0 * 2, sum(row.readings), "new readings added for %s" % row.occurred
            )
            self.assertEqual(date.today(), row.modified.date())

    def test_merge_readings(self):
        """New readings will be merged with old readings."""
        readings: Dict[str, List[float]] = {}
        # replace full row
        full_dt = date.today() - timedelta(days=10)
        readings[full_dt.strftime("%Y-%m-%d")] = [2.0] * 96
        # replace partial row
        partial_dt = date.today() - timedelta(days=9)
        readings[partial_dt.strftime("%Y-%m-%d")] = [None] * 90 + [2.0] * 6
        MeterReading.merge_readings(MeterReading.from_json(self.meter_id, readings))
        db.session.flush()
        query = db.session.query(MeterReading).filter_by(meter=self.meter_id)
        self.assertEqual(7, query.count(), "7 readings from setup")
        for row in query:
            if row.occurred == full_dt:
                self.assertEqual(
                    96.0 * 2, sum(row.readings), "%s fully replaced" % row.occurred
                )
                self.assertEqual(date.today(), row.modified.date())
            elif row.occurred == partial_dt:
                self.assertEqual(
                    90.0 + 12.0,
                    sum(row.readings),
                    "missing values for %s don't replace non-null values"
                    % row.occurred,
                )
                self.assertEqual(date.today(), row.modified.date())
            else:
                self.assertEqual(96.0, sum(row.readings))
                self.assertEqual(row.occurred, row.modified.date())

    def test_frozen_readings(self):
        """New readings will not replace frozen readings."""
        query = (
            db.session.query(MeterReading)
            .filter_by(meter=self.meter_id)
            .order_by(MeterReading.occurred)
        )
        reading = query.first()
        reading.frozen = True
        # if unchanged, SQLAlchemy will default to now
        reading.modified = datetime(
            reading.occurred.year, reading.occurred.month, reading.occurred.day, 1
        )
        db.session.add(reading)
        frozen_dt = reading.occurred
        dt = reading.occurred
        readings: Dict[str, List[float]] = {}
        for idx in range(3):
            readings[dt.strftime("%Y-%m-%d")] = [2.0] * 96
            dt += timedelta(days=1)
        latest_dt = dt
        MeterReading.merge_readings(MeterReading.from_json(self.meter_id, readings))
        db.session.flush()
        # first (frozen) reading unchanged
        reading = query.filter(MeterReading.occurred == frozen_dt).first()
        self.assertEqual(96.0, sum(reading.readings), "frozen data not updated")
        self.assertEqual(
            reading.occurred, reading.modified.date(), "frozen modified not updated"
        )
        # other 2 readings updated
        for row in query.filter(MeterReading.occurred > reading.occurred):
            if row.occurred < latest_dt:
                self.assertEqual(
                    96.0 * 2,
                    sum(row.readings),
                    "readings for %s updated" % row.occurred,
                )
                self.assertEqual(date.today(), row.modified.date())
            else:
                self.assertEqual(
                    96.0, sum(row.readings), "readings for %s unchanged" % row.occurred
                )
                self.assertEqual(row.occurred, row.modified.date())
        self.assertEqual(7, query.count(), "7 readings from setup")
