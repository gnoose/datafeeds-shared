from datetime import datetime, timedelta
import os
import unittest
from unittest import mock

from datafeeds.common.support import Configuration, DateRange
from datafeeds.scrapers import powertrack

TEST_DATE_FORMAT = "%Y-%m-%d"


class SetupBase(unittest.TestCase):
    def setUp(self):
        self.driver = mock.Mock()
        self.start_date = self._build_datetime("2019-01-01")
        self.end_date = self._build_datetime("2019-12-31")

    def _build_datetime(self, desired_date):
        return datetime.strptime(desired_date, TEST_DATE_FORMAT)


class TestCSVParser(SetupBase):
    def setUp(self):
        super().setUp()
        script_dir = os.path.dirname(__file__)
        self.filepath = os.path.join(script_dir, "data/powertrack/example.csv")
        self.csv_parser = powertrack.CSVParser(self.filepath)

    def test_csv_str_to_date(self):
        """date strings are converted to datetimes"""
        date_string = "2020-01-31 09:09:00"
        test_date = self.csv_parser.csv_str_to_date(date_string)
        self.assertEqual(test_date, datetime(2020, 1, 31, 9, 9))

    def test_process_csv(self):
        results = self.csv_parser.process_csv()
        self.assertEqual(len(results), 239)
        self.assertEqual(results[0], (datetime(2020, 10, 13, 0, 0), 0.0))


class TestSiteStatusPage(SetupBase):
    def test_string_to_date(self):
        self.status_page = powertrack.SiteStatusPage
        date_format = "%b %d, %Y"
        date_string = "Jan 28, 2020"
        self.assertEqual(
            self.status_page.string_to_date(date_string, date_format),
            datetime(2020, 1, 28).date(),
        )


class TestPowerTrackScraper(SetupBase):
    def setUp(self):
        super().setUp()
        config = Configuration()
        self.scraper = powertrack.PowerTrackScraper(
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


if __name__ == "__main__":
    unittest.main()
