import io
from datetime import date
from unittest import TestCase
import os

from datafeeds.common import BillingDatum
from datafeeds.scrapers.keller.parsers import parse_bill_pdf

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


# Note: In the tests below division by 748.052 allows us to convert gallons to CCF while keeping the ability
# to cross reference bill PDFs.


class TestKellerParser(TestCase):
    def setUp(self) -> None:
        bills = ["city_of_keller_%02d.pdf" % ii for ii in range(1, 10)]
        self.content = []
        for fname in bills:
            with open(os.path.join(TEST_DIR, "fixtures", fname), "rb") as f:
                self.content.append(io.BytesIO(f.read()))

    def test_bill_parse_01(self):
        actual = parse_bill_pdf(self.content[0])
        expected = BillingDatum(
            start=date(2019, 5, 21),
            end=date(2019, 6, 21),
            statement=date(2019, 6, 21),
            cost=344.85,
            used=15100 / 748.052,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_02(self):
        actual = parse_bill_pdf(self.content[1])
        expected = BillingDatum(
            start=date(2019, 4, 22),
            end=date(2019, 5, 21),
            statement=date(2019, 5, 21),
            cost=1014.22,
            used=73800 / 748.052,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_03(self):
        actual = parse_bill_pdf(self.content[2])
        expected = BillingDatum(
            start=date(2019, 3, 20),
            end=date(2019, 4, 22),
            statement=date(2019, 4, 22),
            cost=1017.75,
            used=74100 / 748.052,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_04(self):
        actual = parse_bill_pdf(self.content[3])
        expected = BillingDatum(
            start=date(2019, 5, 21),
            end=date(2019, 6, 21),
            statement=date(2019, 6, 21),
            cost=332.96,
            used=43500 / 748.052,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_05(self):
        actual = parse_bill_pdf(self.content[4])
        expected = BillingDatum(
            start=date(2019, 4, 22),
            end=date(2019, 5, 21),
            statement=date(2019, 5, 21),
            cost=71.44,
            used=0.0,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_06(self):
        actual = parse_bill_pdf(self.content[5])
        expected = BillingDatum(
            start=date(2019, 3, 20),
            end=date(2019, 4, 22),
            statement=date(2019, 4, 22),
            cost=325.54,
            used=42500 / 748.052,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_07(self):
        actual = parse_bill_pdf(self.content[6])
        expected = BillingDatum(
            start=date(2019, 8, 21),
            end=date(2019, 9, 20),
            statement=date(2019, 9, 20),
            cost=102.51,
            used=0.0,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_08(self):
        actual = parse_bill_pdf(self.content[7])
        expected = BillingDatum(
            start=date(2019, 8, 21),
            end=date(2019, 9, 20),
            statement=date(2019, 9, 20),
            cost=52.70,
            used=0.0,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_09(self):
        actual = parse_bill_pdf(self.content[8])
        expected = BillingDatum(
            start=date(2019, 8, 21),
            end=date(2019, 9, 19),
            statement=date(2019, 9, 19),
            cost=1270.05,
            used=104400.0 / 748.052,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual(expected, actual)
