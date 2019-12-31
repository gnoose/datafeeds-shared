import unittest
from unittest import mock

from datetime import date, timedelta
from datafeeds.common.support import DateRange, Configuration

from datafeeds.scrapers import bloom_interval


class SetupBase(unittest.TestCase):
    def setUp(self):
        self.driver = mock.Mock()
        self.start_date = date(2019, 1, 1)
        self.end_date = date(2019, 12, 1)


class TestBloomScraper(SetupBase):
    def setUp(self):
        super().setUp()
        config = Configuration()
        config.site_name = "Xilinx - San Jose"
        self.scraper = bloom_interval.BloomScraper(credentials=None, date_range=DateRange(
            self.start_date, self.end_date), configuration=config
        )

    def test_check_start_and_end_dates(self):
        default_date = 2008
        self.scraper.start_date = self.start_date
        self.scraper.end_date = self.start_date - timedelta(days=2)
        # End date moved to after start date
        self.scraper.adjust_start_and_end_dates(default_date)
        self.assertEqual(self.scraper.end_date, self.start_date + timedelta(days=1))

        self.scraper.end_date = date.today() + timedelta(days=2)
        self.scraper.adjust_start_and_end_dates(default_date)
        # End date past today so moved to today
        self.assertEqual(self.scraper.end_date, date.today())

        # Move start date if earlier than earliest date
        self.scraper.start_date = date(default_date - 1, 1, 1)
        self.scraper.adjust_start_and_end_dates(default_date)
        self.assertEqual(self.scraper.start_date, date(default_date, 1, 1))
