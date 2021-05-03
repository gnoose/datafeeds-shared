import argparse
import logging
import unittest
import functools as ft

from datetime import date
from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.poway_water import (
    PowayWaterConfiguration,
    PowayWaterScraper,
    parse_poway_pdf,
)

logging.getLogger("pdfminer").setLevel(logging.WARNING)


class PowayTestCase(unittest.TestCase):
    def test_poway_pdf1(self):
        """Verify that we can extract cost, use, and dates from City of Poway PDF."""
        bill = parse_poway_pdf(
            "datafeeds/scrapers/tests/fixtures/poway1.pdf", "15808710-01"
        )
        self.assertEqual(date(2020, 12, 15), bill.start)
        self.assertEqual(
            date(2021, 2, 17), bill.end, "end read date -1 to prevent overlaps"
        )
        self.assertEqual(date(2021, 2, 25), bill.statement)
        self.assertAlmostEqual(3256.34, bill.cost, 2)
        self.assertAlmostEqual(742, bill.used, 2)

    def test_poway_pdf2(self):
        """Verify that we can extract cost, use, and dates from City of Poway PDF and exclude previous balance."""
        bill = parse_poway_pdf(
            "datafeeds/scrapers/tests/fixtures/poway2.pdf", "15808710-01"
        )
        self.assertEqual(date(2020, 8, 14), bill.start)
        self.assertEqual(
            date(2020, 10, 19), bill.end, "end read date -1 to prevent overlaps"
        )
        self.assertEqual(date(2020, 10, 29), bill.statement)
        self.assertAlmostEqual(10000, bill.cost, 2, "excludes balance forward")
        self.assertAlmostEqual(2403, bill.used, 2)


def test_upload_bills(meter_oid, meter_number, task_id, bills):
    print("Bill results:\n")
    for bill in bills:
        print(
            "%s\t%s\t%.2f\t%.2f\t%s"
            % (
                bill.start.strftime("%Y-%m-%d"),
                bill.end.strftime("%Y-%m-%d"),
                bill.cost,
                bill.used,
                bill.peak,
            )
        )


def test_scraper(
    username: str, password: str, account_id: str, start_date: date, end_date: date
):
    configuration = PowayWaterConfiguration(account_id)
    credentials = Credentials(username, password)
    scraper = PowayWaterScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        bills_handler=ft.partial(test_upload_bills, -1, account_id, None),
        partial_bills_handler=None,
        readings_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()


"""
    Run this to launch the Poway water billing scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_poway_water.py account_id start end username password
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("account_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    start_dt = date_parser.parse(args.start).date()
    end_dt = date_parser.parse(args.end).date()
    test_scraper(args.username, args.password, args.account_id, start_dt, end_dt)
