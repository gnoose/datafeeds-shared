import unittest
import os

from datetime import date

from datafeeds.common import BillingDatum
from datafeeds.common.typing import make_billing_pdf_attachment
from datafeeds.scrapers import portland_bizportal as pb


TEST_DIR = os.path.split(__file__)[0]


class TestPortlandBizportalScraper(unittest.TestCase):
    def test_extract_bill_period(self):
        """We can parse the start and end of the billing period from a bill PDF."""
        filename = os.path.join(
            TEST_DIR, "fixtures", "test_portland_bizportal_bill.pdf"
        )
        start_dt, end_dt = pb.extract_bill_period(filename)
        self.assertEqual(date(2018, 1, 7), start_dt)
        self.assertEqual(date(2018, 2, 6), end_dt)

    def test_adjust_bill_dates(self):
        bills = [
            BillingDatum(
                start=date(2018, 4, 8),
                end=date(2018, 5, 9),
                statement=date(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment(
                    "f1", "test", date(2018, 5, 9), "portland-ge", "123"
                ),
                utility_code=None,
            ),
            BillingDatum(
                start=date(2018, 5, 9),
                end=date(2018, 6, 8),
                statement=date(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment(
                    "f2", "test", date(2018, 6, 8), "portland-ge", "123"
                ),
                utility_code=None,
            ),
            BillingDatum(
                start=date(2018, 6, 8),
                end=date(2018, 7, 12),
                statement=date(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment(
                    "f3", "test", date(2018, 7, 12), "portland-ge", "123"
                ),
                utility_code=None,
            ),
        ]

        expected = [
            BillingDatum(
                start=date(2018, 4, 8),
                end=date(2018, 5, 9),
                statement=date(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment(
                    "f1", "test", date(2018, 5, 9), "portland-ge", "123"
                ),
                utility_code=None,
            ),
            BillingDatum(
                start=date(2018, 5, 10),
                end=date(2018, 6, 8),
                statement=date(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment(
                    "f2", "test", date(2018, 6, 8), "portland-ge", "123"
                ),
                utility_code=None,
            ),
            BillingDatum(
                start=date(2018, 6, 9),
                end=date(2018, 7, 12),
                statement=date(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment(
                    "f3", "test", date(2018, 7, 12), "portland-ge", "123"
                ),
                utility_code=None,
            ),
        ]

        actual = pb._adjust_bill_dates(bills)
        self.assertEqual(expected, actual)

    def test_extract_bill_data(self):
        filename = os.path.join(
            TEST_DIR, "fixtures", "test_portland_bizportal_bill_2.pdf"
        )
        bill = pb.extract_bill_data(filename, 1, "utility", 2)
        # BillingDatum(start=datetime.date(2020, 7, 9),
        #              end=datetime.date(2020, 8, 9),
        #              statement=datetime.date(2020, 8, 9),
        #              cost='4165.08',
        #              used='42438',
        #              peak=152.0,
        #              items=[],
        #              attachments=[],
        #              utility_code=None)
        self.assertEqual(bill.start, date(2020, 7, 9))
        self.assertEqual(bill.end, date(2020, 8, 9))
        self.assertEqual(bill.cost, "4165.08")
        self.assertEqual(bill.used, "42438")
        self.assertEqual(bill.peak, 152.0)
