from datetime import date, timedelta
from unittest import TestCase

from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.models import Meter
from datafeeds.models.meter import MeterReading
from datafeeds.scrapers.scl_meterwatch import MeterDataPage


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


# #
#
# """
#     Run this to launch the scl-meterwatch scraper:
#
#     $ export PYTHONPATH=$(pwd)
#     $ python datafeeds/scrapers/tests/test_scl_meterwatch.py service_id start end username
#     password
# """
#
#
# def test_scraper(
#     service_id: str, start_date: date, end_date: date, username: str, password: str
# ):
#     meter = db.session.query(Meter).first()
#     configuration = SCLMeterWatchConfiguration(
#         meter_numbers=[service_id], meter_oid=meter.oid
#     )
#     credentials = Credentials(username, password)
#     scraper = SCLMeterWatchScraper(
#         credentials, DateRange(start_date, end_date), configuration
#     )
#     scraper.start()
#     scraper.scrape(
#         readings_handler=print, bills_handler=None,
#     )
#     scraper.stop()
#
#
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("service_id", type=str)
#     parser.add_argument("start", type=str)
#     parser.add_argument("end", type=str)
#     parser.add_argument("username", type=str)
#     parser.add_argument("password", type=str)
#     args = parser.parse_args()
#     test_scraper(
#         args.service_id,
#         date_parser.parse(args.start).date(),
#         date_parser.parse(args.end).date(),
#         args.username,
#         args.password,
#     )
