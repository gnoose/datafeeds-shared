from datetime import datetime, timedelta
import os
import unittest
from unittest import mock

from datafeeds.common.support import Configuration, DateRange
from datafeeds.scrapers import nautilus

TEST_DATE_FORMAT = "%Y-%m-%d"
script_dir = os.path.dirname(__file__)


class SetupBase(unittest.TestCase):
    def setUp(self):
        self.driver = mock.Mock()
        self.start_date = self._build_datetime("2019-01-01")
        self.end_date = self._build_datetime("2019-12-31")

    @staticmethod
    def _build_datetime(desired_date):
        return datetime.strptime(desired_date, TEST_DATE_FORMAT)


class TestCSVParser(SetupBase):
    def setUp(self):
        super().setUp()
        self.filepath = os.path.join(script_dir, "data/nautilus/works.csv")
        meter_number = "2"
        self.csv_parser = nautilus.CSVParser(self.filepath, meter_number)

    def test_csv_str_to_date(self):
        """date strings are converted to datetimes"""
        date_string = "2/4/2020 5:30 AM"
        test_date = self.csv_parser.csv_str_to_date(date_string)
        self.assertEqual(test_date, datetime(2020, 2, 4, 5, 30))

    def test_process_csv(self):
        results = self.csv_parser.process_csv()
        self.assertEqual(len(results), 161)
        self.assertEqual(results[0], (datetime(2020, 2, 2, 0, 0), 0.014))

    def test_units(self):
        """Processing fails with wrong units in header. kW is okay, but not kWh"""
        self.filepath = os.path.join(script_dir, "data/nautilus/bad-units.csv")
        self.meter_number = "2"
        self.csv_parser = nautilus.CSVParser(self.filepath)

        with self.assertRaises(nautilus.NautilusException):
            self.csv_parser.process_csv()

    def test_meter_numbers(self):
        """Determining number of meters fails with incorrectly formatted headers"""
        self.filepath = os.path.join(script_dir, "data/nautilus/bad-meternames.csv")
        self.meter_number = "2"
        self.csv_parser = nautilus.CSVParser(self.filepath)

        with self.assertRaises(nautilus.NautilusException):
            self.csv_parser.process_csv()


class TestSitePage(SetupBase):
    def test_string_to_date(self):
        self.status_page = nautilus.SitePage
        date_format = "%b %d, %Y"
        date_string = "Jan 28, 2020"
        self.assertEqual(
            self.status_page.string_to_date(date_string, date_format),
            datetime(2020, 1, 28).date(),
        )


class TestNautilusScraper(SetupBase):
    def setUp(self):
        super().setUp()
        config = Configuration()
        config.account_id = "s12345"
        config.meter_id = "2a566973506457484a43554b772b71553d-1"
        self.scraper = nautilus.NautilusScraper(
            credentials=None,
            date_range=DateRange(self.start_date.date(), self.end_date.date()),
            configuration=config,
        )

    def test_adjust_start_and_end_dates(self):
        self.scraper.install_date = self.end_date.date()
        self.scraper.end_date = datetime.min.date()
        # Start date moved to install date
        self.scraper.adjust_start_and_end_dates()
        self.assertEqual(self.scraper.start_date, self.end_date.date())
        # End date earlier than start date, so moved to one day past start
        self.assertEqual(
            self.scraper.end_date, self.scraper.start_date + timedelta(days=1)
        )

    def test_construct_site_url(self):
        self.scraper.construct_site_url()
        self.assertEqual(
            self.scraper.site_url,
            "http://s12345.mini.alsoenergy.com/Dashboard/2a566973506457484a43554b772b71553d",
        )
