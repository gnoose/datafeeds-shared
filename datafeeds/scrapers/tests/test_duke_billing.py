import argparse
from datetime import date
import functools as ft
import logging
import unittest

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.duke import pdf_parser
from datafeeds.parsers import pdfparser
from datafeeds.scrapers.duke.billing import DukeBillingConfiguration, DukeBillingScraper

logging.getLogger("pdfminer").setLevel(logging.WARNING)


class DukeBillTestCase(unittest.TestCase):
    def test_parse_new_pdf1(self):
        """Verify that we can extract cost, use, and demand from June 2020+ version of PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_new_1.pdf")
        data = pdf_parser.parse_new_pdf(text)
        self.assertEqual(date(2020, 5, 10), data.start)
        self.assertEqual(date(2020, 6, 9), data.end)
        self.assertAlmostEqual(123113.66, data.cost, 2)
        self.assertAlmostEqual(1806000.0, data.used, 2)
        self.assertAlmostEqual(3840.0, data.peak, 2)

    def test_parse_new_pdf2(self):
        """Verify that we can extract cost, use, and demand from another new-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_new_2.pdf")
        data = pdf_parser.parse_new_pdf(text)
        self.assertEqual(date(2020, 4, 21), data.start)
        self.assertEqual(date(2020, 5, 25), data.end)
        self.assertAlmostEqual(29.89, data.cost, 2)
        self.assertAlmostEqual(155, data.used, 2)
        self.assertIsNone(data.peak)

    def test_parse_new_pdf3(self):
        """Verify that we can extract cost, use, and demand from another new-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_new_3.pdf")
        data = pdf_parser.parse_new_pdf(text)
        self.assertEqual(date(2020, 4, 21), data.start)
        self.assertEqual(date(2020, 5, 23), data.end)
        self.assertAlmostEqual(3048.76, data.cost, 2)
        self.assertAlmostEqual(38320, data.used, 2)
        self.assertAlmostEqual(86, data.peak)

    def test_parse_old_pdf1(self):
        """Verify that we can extract cost, use, and demand from version of PDF prior to June 2020.

        This bill includes a prior balance due line item that should be excluded from bill total.
        """
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_1.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2020, 4, 10), data.start)
        self.assertEqual(date(2020, 5, 9), data.end)
        self.assertAlmostEqual(89972.41, data.cost, 2)
        self.assertAlmostEqual(1312000.0, data.used, 2)
        self.assertAlmostEqual(3000.0, data.peak, 2)

    def test_parse_old_pdf2(self):
        """Verify that we can extract cost, use, and demand from another old-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_2.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2018, 10, 29), data.start)
        self.assertEqual(date(2018, 12, 3), data.end)
        self.assertAlmostEqual(685.16, data.cost, 2)
        self.assertAlmostEqual(5581.0, data.used, 2)
        self.assertAlmostEqual(30.0, data.peak, 2)

    def test_parse_old_pdf3(self):
        """Verify that we can extract cost, use, and demand from another old-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_3.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2020, 3, 20), data.start)
        self.assertEqual(date(2020, 4, 20), data.end)
        self.assertAlmostEqual(29.89, data.cost, 2)
        self.assertAlmostEqual(155, data.used, 2)
        self.assertIsNone(data.peak)

    def test_parse_old_pdf4(self):
        """Verify that we can extract dates from an old-format PDF over a year boundary."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_4.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2019, 11, 27), data.start)
        self.assertEqual(date(2019, 12, 30), data.end)
        self.assertAlmostEqual(19.7, data.cost, 2)
        self.assertAlmostEqual(156, data.used, 2)
        self.assertIsNone(data.peak)


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
    configuration = DukeBillingConfiguration("utility:duke-carolinas-nc", account_id)
    credentials = Credentials(username, password)
    scraper = DukeBillingScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        bills_handler=ft.partial(
            test_upload_bills, -1, account_id, None, "duke-energy-billing"
        ),
        partial_bills_handler=None,
        readings_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()


"""
    Run this to launch the Duke billing scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_duke_billing.py account_id start end username password
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
    """
    expected values for date range 2020-11-01 2021-01-15

account number 1781374757
start       end         cost       used     peak
2020-12-01  2020-12-31  325155.49  5410000  7944
2020-11-01  2020-11-30  297289.73  3730000  7500
2020-10-01  2020-10-31  294845.40  3406000  7500

account number 1655779178
start       end         cost    used  peak
2020-12-04  2021-01-05  149.24  1020  30
2020-11-04  2020-12-03   84.35   488  30
2020-10-05  2020-11-04   70.20   304  30
    """
