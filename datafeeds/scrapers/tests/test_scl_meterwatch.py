import json
from datetime import date, timedelta
from unittest import TestCase

from datafeeds.common import Timeline
from datafeeds.common.support import Credentials, DateRange
from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.models import Meter
from datafeeds.models.meter import MeterReading
from datafeeds.scrapers.scl_meterwatch import (
    MeterDataPage,
    SCLMeterWatchConfiguration,
    SCLMeterWatchScraper,
)


class SCLTestDates(TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def setUp(self):
        super().setUp()
        self.meter = Meter(interval=15, commodity="kw", name="Test meter")
        db.session.add(self.meter)
        db.session.flush()

    def test_last_reading_date(self):
        page = MeterDataPage(None, None)
        start_date = date(2020, 5, 1)
        updated_start_date = page.start_date_from_readings(self.meter.oid, start_date)
        self.assertEqual(
            start_date, updated_start_date, "no readings uses requested start date"
        )
        # create a reading record older than start date
        reading_date = start_date - timedelta(days=3)
        db.session.add(
            MeterReading(
                meter=self.meter.oid, occurred=reading_date, readings=[1.0] * 96
            )
        )
        db.session.flush()
        updated_start_date = page.start_date_from_readings(self.meter.oid, start_date)
        self.assertEqual(
            reading_date,
            updated_start_date,
            "use oldest reading date when older than start date",
        )
        reading_date = start_date + timedelta(days=3)
        # create a reading record newer than start date
        db.session.add(
            MeterReading(
                meter=self.meter.oid, occurred=reading_date, readings=[1.0] * 96
            )
        )
        db.session.flush()
        updated_start_date = page.start_date_from_readings(self.meter.oid, start_date)
        self.assertEqual(
            start_date,
            updated_start_date,
            "use requested start date when newer readings exist",
        )

    def test_fall_daylight_savings(self):
        """Test Fall DST values are not double counted"""

        date_range = DateRange(date(2020, 11, 1), date(2020, 11, 1))
        timeline = Timeline(date_range.start_date, date_range.end_date, 15)
        scraper = SCLMeterWatchScraper(
            Credentials(None, None),
            date_range,
            SCLMeterWatchConfiguration(meter_numbers=["803441"], meter=self.meter),
        )
        scraper._process_csv(
            "datafeeds/scrapers/tests/fixtures/scl_meterwatch_dst.csv", timeline
        )
        with open(
            "datafeeds/scrapers/tests/fixtures/scl_meterwatch_dst_expected.json"
        ) as f:
            expected = json.loads(f.read())

        self.assertEqual(expected, timeline.serialize())
