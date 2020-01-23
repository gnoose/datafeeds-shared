import unittest
import os

from datetime import datetime

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
        self.assertEqual(datetime(2018, 1, 7), start_dt)
        self.assertEqual(datetime(2018, 2, 6), end_dt)

    def test_adjust_bill_dates(self):
        bills = [
            BillingDatum(
                start=datetime(2018, 4, 8),
                end=datetime(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f1"),
            ),
            BillingDatum(
                start=datetime(2018, 5, 9),
                end=datetime(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f2"),
            ),
            BillingDatum(
                start=datetime(2018, 6, 8),
                end=datetime(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f3"),
            ),
        ]

        expected = [
            BillingDatum(
                start=datetime(2018, 4, 8),
                end=datetime(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f1"),
            ),
            BillingDatum(
                start=datetime(2018, 5, 10),
                end=datetime(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f2"),
            ),
            BillingDatum(
                start=datetime(2018, 6, 9),
                end=datetime(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f3"),
            ),
        ]

        actual = pb._adjust_bill_dates(bills)
        self.assertEqual(expected, actual)

    def test_unify_bills_1(self):
        bills = [
            BillingDatum(
                start=datetime(2018, 4, 8),
                end=datetime(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f1"),
            ),
            BillingDatum(
                start=datetime(2018, 5, 9),
                end=datetime(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f2"),
            ),
            BillingDatum(
                start=datetime(2018, 6, 8),
                end=datetime(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f3"),
            ),
        ]

        expected = [
            BillingDatum(
                start=datetime(2018, 4, 8),
                end=datetime(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f1"),
            ),
            BillingDatum(
                start=datetime(2018, 5, 10),
                end=datetime(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f2"),
            ),
            BillingDatum(
                start=datetime(2018, 6, 9),
                end=datetime(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f3"),
            ),
        ]

        actual = pb._unify_bill_history(bills, [])
        self.assertEqual(expected, actual)

    def test_unify_bills_2(self):
        pdf_bills = [
            BillingDatum(
                start=datetime(2018, 3, 1),
                end=datetime(2018, 4, 8),
                cost=1234.5,
                used=100000,
                peak=0,
                items=None,
                attachments="f0",
            ),
            BillingDatum(
                start=datetime(2018, 4, 8),
                end=datetime(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f1"),
            ),
            BillingDatum(
                start=datetime(2018, 5, 9),
                end=datetime(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f2"),
            ),
            BillingDatum(
                start=datetime(2018, 6, 8),
                end=datetime(2018, 7, 12),
                cost=17529.34,
                used=96439,
                peak=0,
                items=None,
                attachments=make_billing_pdf_attachment("f3"),
            ),
        ]

        csv_bills = [
            BillingDatum(
                start=datetime(2018, 4, 8),
                end=datetime(2018, 5, 8),
                cost=9770.53,
                used=101175,
                peak=379,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=datetime(2018, 5, 9),
                end=datetime(2018, 6, 7),
                cost=8210.58,
                used=80084,
                peak=264,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=datetime(2018, 6, 8),
                end=datetime(2018, 7, 11),
                cost=9318.76,
                used=96439,
                peak=278,
                items=None,
                attachments=None,
            ),
        ]

        expected = [
            BillingDatum(
                start=datetime(2018, 3, 1),
                end=datetime(2018, 4, 8),
                cost=1234.5,
                used=100000,
                peak=0,
                items=None,
                attachments="f0",
            ),
            BillingDatum(
                start=datetime(2018, 4, 9),
                end=datetime(2018, 5, 9),
                cost=9770.53,
                used=101175,
                peak=379,
                items=None,
                attachments=make_billing_pdf_attachment("f1"),
            ),
            BillingDatum(
                start=datetime(2018, 5, 10),
                end=datetime(2018, 6, 8),
                cost=8210.58,
                used=80084,
                peak=264,
                items=None,
                attachments=make_billing_pdf_attachment("f2"),
            ),
            BillingDatum(
                start=datetime(2018, 6, 9),
                end=datetime(2018, 7, 12),
                cost=9318.76,
                used=96439,
                peak=278,
                items=None,
                attachments=make_billing_pdf_attachment("f3"),
            ),
        ]

        actual = pb._unify_bill_history(pdf_bills, csv_bills)
        self.assertEqual(expected, actual)
