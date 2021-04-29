from datetime import date
import io
import logging
import os
from unittest import TestCase

from datafeeds.common.typing import BillingDatum
from datafeeds.parsers import pacific_power

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


log = logging.getLogger(__name__)
# Tests 01-04 and 08 confirm that we can process current Pacific Power bills.
# Tests 05-06 confirm that we can process historical Pacific Power bills.


class TestPacificPowerParser(TestCase):
    def setUp(self) -> None:
        files = ["pacific_power_test_%02d.pdf" % x for x in range(1, 8)]
        self.data = {}
        for filename in files:
            with open(f"datafeeds/scrapers/tests/fixtures/{filename}", "rb") as f:
                data = f.read()
            self.data[filename] = io.BytesIO(data)

    def test_bill_parse_01(self):
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_01.pdf"], "13714552"
        )
        expected = BillingDatum(
            start=date(2019, 6, 1),
            end=date(2019, 7, 1),
            statement=date(2019, 7, 10),
            cost=850402.64,
            used=11300000 + 9143000,
            peak=29960,
            items=None,
            attachments=None,
            utility_code="Schedule 748",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_02(self):
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_01.pdf"], "66887643"
        )
        expected = BillingDatum(
            start=date(2019, 6, 3),
            end=date(2019, 7, 2),
            statement=date(2019, 7, 10),
            cost=732.86,
            used=6972.0,
            peak=26.0,
            items=None,
            attachments=None,
            utility_code="Schedule 28",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_03(self):
        """If the statement date cannot be determined, default to the bill's end-date."""
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_02.pdf"], "78534175"
        )
        expected = BillingDatum(
            start=date(2019, 2, 16),
            end=date(2019, 3, 15),
            statement=date(2019, 3, 15),
            cost=469.03,
            used=4471.0,
            peak=12.0,
            items=None,
            attachments=None,
            utility_code="Schedule 23",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_04(self):
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_03.pdf"], "78585187"
        )
        expected = BillingDatum(
            start=date(2019, 5, 31),
            end=date(2019, 6, 30),
            statement=date(2019, 7, 18),
            cost=185.86,
            used=0.0,
            peak=5.0,
            items=None,
            attachments=None,
            utility_code="Schedule 28",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_05(self):
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_04.pdf"], "13714552"
        )
        expected = BillingDatum(
            start=date(2017, 2, 1),
            end=date(2017, 3, 1),
            statement=date(2017, 3, 9),
            cost=780622.38,
            used=8271000.0 + 6061000.0,
            peak=35767.0,
            items=None,
            attachments=None,
            utility_code="Schedule 748",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_06(self):
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_05.pdf"], "66887643"
        )
        expected = BillingDatum(
            start=date(2017, 7, 3),
            end=date(2017, 8, 2),
            statement=date(2017, 8, 7),
            cost=222.02,
            used=1846,
            peak=11,
            items=None,
            attachments=None,
            utility_code="Schedule 23",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_07(self):
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_06.pdf"], "13714552"
        )
        expected = BillingDatum(
            start=date(2017, 12, 1),
            end=date(2018, 1, 1),
            statement=date(2018, 1, 12),
            cost=914721.55,
            used=10616000.0 + 9113000.0,
            peak=27278.0,
            items=None,
            attachments=None,
            utility_code="Schedule 748",
        )
        self.assertEqual(expected, actual)

    def test_bill_parse_08(self):
        """Verify utility code extraction from a multi-meter bill."""
        actual = pacific_power.parse_bill_pdf(
            self.data["pacific_power_test_07.pdf"], "38650731"
        )
        expected = BillingDatum(
            start=date(2021, 1, 31),
            end=date(2021, 2, 28),
            statement=date(2021, 3, 12),
            cost=152127.79,
            used=21609000.0,
            peak=34904.0,
            items=None,
            attachments=None,
            utility_code="Schedule 848",
        )
        self.assertEqual(expected, actual)
