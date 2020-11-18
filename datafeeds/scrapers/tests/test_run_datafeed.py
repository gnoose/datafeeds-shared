import csv
import os
from datetime import datetime, timedelta, date
import uuid
from unittest import TestCase

from datafeeds import db, config
from datafeeds.common import (
    Configuration,
    BaseApiScraper,
    test_utils,
    Results,
    BillingDatum,
    Timeline,
)
from datafeeds.common.batch import run_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterMeterDataSource
from datafeeds.models.bill import PartialBill, PartialBillProviderType
from datafeeds.models.meter import MeterReading


class TestConfiguration(Configuration):
    def __init__(
        self,
        service_id: str,
        gen_service_id: str,
        scrape_bills: bool = False,
        scrape_partial_bills: bool = False,
        scrape_readings: bool = False,
    ):
        super().__init__(
            scrape_bills=scrape_bills,
            scrape_partial_bills=scrape_partial_bills,
            scrape_readings=scrape_readings,
        )
        self.service_id = service_id
        self.gen_service_id = gen_service_id


class TestPartialBillScraper(BaseApiScraper):
    """A scraper that returns a generation bill."""

    def _execute(self):
        return Results(
            generation_bills=[
                BillingDatum(
                    start=self.end_date - timedelta(days=30),
                    end=self.end_date,
                    statement=self.end_date,
                    cost=100,
                    used=25,
                    peak=10,
                    items=None,
                    attachments=None,
                    utility_code=None,
                )
            ]
        )


class TestBillScraper(BaseApiScraper):
    """A scraper that returns a bundled bill."""

    def _execute(self):
        return Results(
            bills=[
                BillingDatum(
                    start=self.end_date - timedelta(days=30),
                    end=self.end_date,
                    statement=self.end_date,
                    cost=100,
                    used=25,
                    peak=10,
                    items=None,
                    attachments=None,
                    utility_code=None,
                )
            ]
        )


class TestIntervalScraper(BaseApiScraper):
    """A scraper that returns interval data."""

    def _execute(self):
        timeline = Timeline(self.start_date, self.end_date)
        dt = datetime(self.end_date.year, self.start_date.month, self.start_date.day)
        for idx in range(96):
            timeline.insert(dt + timedelta(minutes=15 * idx), 1.0)
        return Results(readings=timeline.serialize())


class TestEndToEnd(TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        db.session.begin(subtransactions=True)
        (account, meters) = test_utils.create_meters()
        self.account = account
        self.meter = meters[0]
        test_utils.add_datasources(account, meters, "test-scraper")
        us = self.meter.utility_service
        us.gen_service_id = datetime.now().strftime("%f")
        db.session.add(us)
        db.session.flush()

    def tearDown(self):
        db.session.rollback()
        db.session.remove()

    def test_run_datafeed_partial(self):
        """Run a test scraper that creates partial bills."""
        us = self.meter.utility_service
        configuration = TestConfiguration(
            us.service_id, us.gen_service_id, scrape_partial_bills=True
        )
        meter_ds = (
            db.session.query(SnapmeterMeterDataSource)
            .filter_by(meter=self.meter)
            .first()
        )
        params = {
            "data_start": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "data_end": datetime.now().strftime("%Y-%m-%d"),
        }
        rval = run_datafeed(
            TestPartialBillScraper,
            self.account,
            self.meter,
            meter_ds,
            params,
            configuration=configuration,
            task_id=uuid.uuid4().hex,
        )
        self.assertEqual(Status.SUCCEEDED, rval)
        # created a partial bill
        bills = db.session.query(PartialBill).filter_by(service=us.oid)
        self.assertEqual(1, bills.count())
        bill = bills.first()
        self.assertEqual(date.today(), bill.closing)
        self.assertEqual(100, bill.cost)
        self.assertEqual(25, bill.used)
        self.assertEqual(
            PartialBillProviderType.GENERATION_ONLY.value, bill.provider_type
        )

    def test_run_datafeed_bill(self):
        """Run a test scraper that scrapes a bill."""
        us = self.meter.utility_service
        configuration = TestConfiguration(
            us.service_id, us.gen_service_id, scrape_bills=True
        )
        meter_ds = (
            db.session.query(SnapmeterMeterDataSource)
            .filter_by(meter=self.meter)
            .first()
        )
        params = {
            "data_start": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "data_end": datetime.now().strftime("%Y-%m-%d"),
        }
        rval = run_datafeed(
            TestBillScraper,
            self.account,
            self.meter,
            meter_ds,
            params,
            configuration=configuration,
            task_id=uuid.uuid4().hex,
        )
        self.assertEqual(Status.SUCCEEDED, rval)
        # writes bill data to csv
        path = os.path.join(config.WORKING_DIRECTORY, "bills.csv")
        with open(path) as f:
            reader = csv.reader(f)
            rows = [row for row in reader]
        self.assertEqual(2, len(rows))
        # header row
        self.assertEqual(
            ["Service ID", "Start", "End", "Cost", "Used", "Peak"], rows[0]
        )
        # data row
        self.assertEqual(
            [
                str(us.service_id),
                (date.today() - timedelta(days=30)).strftime("%Y-%m-%d"),
                date.today().strftime("%Y-%m-%d"),
                "100",
                "25",
                "10",
            ],
            rows[1],
        )

    def test_run_datafeed_interval(self):
        """Run a test scraper that creates interval data."""
        us = self.meter.utility_service
        configuration = TestConfiguration(
            us.service_id, us.gen_service_id, scrape_readings=True
        )
        meter_ds = (
            db.session.query(SnapmeterMeterDataSource)
            .filter_by(meter=self.meter)
            .first()
        )
        start_dt = datetime.now() - timedelta(days=7)
        params = {
            "data_start": start_dt.strftime("%Y-%m-%d"),
            "data_end": datetime.now().strftime("%Y-%m-%d"),
        }
        rval = run_datafeed(
            TestIntervalScraper,
            self.account,
            self.meter,
            meter_ds,
            params,
            configuration=configuration,
            task_id=uuid.uuid4().hex,
        )
        self.assertEqual(Status.SUCCEEDED, rval)
        # created readings
        readings = [
            row
            for row in db.session.query(MeterReading).filter_by(meter=self.meter.oid)
        ]
        self.assertEqual(1, len(readings))
        self.assertEqual(start_dt.date(), readings[0].occurred)
        self.assertEqual([1.0] * 96, readings[0].readings)
