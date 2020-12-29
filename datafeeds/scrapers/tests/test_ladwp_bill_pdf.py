import argparse
from datetime import date
import functools as ft
from unittest import TestCase
from unittest.mock import patch

from datafeeds.common import BillingDatum
from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.ladwp_bill_pdf import (
    LADWPBillPdfConfiguration,
    LADWPBillPdfScraper,
    parse_pdf,
)

"""
    Run this to launch the LADWP scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_ladwp_billing.py \\
          service_id start end username password

    Run this to test the PDF parsing:

    $ python -m unittest datafeeds/scrapers/tests/test_ladwp_billing.py
"""


class TestLADWPParser(TestCase):
    @patch("datafeeds.scrapers.ladwp_bill_pdf.notify_rebill")
    def test_single_account(self, _notify):
        """Parser can extract data from a single-account bill."""
        filename = "datafeeds/scrapers/tests/fixtures/ladwp-single.pdf"
        expected = BillingDatum(
            start=date(2020, 9, 10),
            end=date(2020, 10, 12),
            statement=date(2020, 10, 13),
            cost=115955.98,
            used=571680,
            peak=1215.36,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "PMY2V00231-00001054", "kw"))

    @patch("datafeeds.scrapers.ladwp_bill_pdf.notify_rebill")
    def test_multi_account(self, _notify):
        """Parser can extract data from a multi-account bill."""
        filename = "datafeeds/scrapers/tests/fixtures/ladwp-multi.pdf"
        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=1175.67,
            used=4788,
            peak=13.68,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "PMY00209-00014118", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=1223.64,
            used=5467,
            peak=13.03,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "APMYD00209-00069098", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=882.04,
            used=3520,
            peak=10.22,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "APMYD00209-00064175", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=2174.42,
            used=9498,
            peak=27.83,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "APMYD00209-00064176", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=900.52,
            used=2772,
            peak=7.92,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "PMY00209-00014142", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=1410.76,
            used=6217,
            peak=11.92,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "APMYD00209-00069100", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=384.29,
            used=1062,
            peak=2.03,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "APMYD00209-00064174", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=621.40,
            used=2316,
            peak=11.38,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "PMY00209-00028877", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=27440.74,
            used=111744,
            peak=426.24,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "1APMYV00277-00006259", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=1221.66,
            used=5220,
            peak=12.24,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "PMY00209-00016347", "kw"))

        expected = BillingDatum(
            start=date(2020, 9, 2),
            end=date(2020, 10, 1),
            statement=date(2020, 10, 5),
            cost=1004.06,
            used=3888,
            peak=18,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "PMY00209-00014123", "kw"))
        # uses peaks_2 pattern
        filename = "datafeeds/scrapers/tests/fixtures/ladwp-202004.pdf"
        expected = BillingDatum(
            start=date(2020, 3, 6),
            end=date(2020, 4, 6),
            statement=date(2020, 4, 7),
            cost=2566.24,
            used=12505,
            peak=37.72,
            items=None,
            attachments=None,
            utility_code=None,
        )
        self.assertEqual([expected], parse_pdf(filename, "APMYD00209-00063954", "kw"))

    @patch("datafeeds.scrapers.ladwp_bill_pdf.notify_rebill")
    def test_rebill(self, notify):
        """Parser can extract data from a bill with corrections."""
        self.maxDiff = None
        filename = "datafeeds/scrapers/tests/fixtures/ladwp-rebill.pdf"
        # electricity
        expected = [
            BillingDatum(
                start=date(2020, 3, 25),
                end=date(2020, 7, 21),
                statement=date(2020, 9, 24),
                cost=70308.37,
                used=297840,
                peak=338.4,
                items=None,
                attachments=None,
                utility_code=None,
            ),
            BillingDatum(
                start=date(2020, 7, 23),
                end=date(2020, 9, 20),
                statement=date(2020, 9, 24),
                cost=46004.04,
                used=200640.0,
                peak=295.2,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(expected, parse_pdf(filename, "APMV00477-00001024", "kw"))
        notify.assert_called_once_with("APMV00477-00001024", date(2020, 9, 24))
        # water
        expected = [
            BillingDatum(
                start=date(2020, 5, 27),
                end=date(2020, 6, 23),
                statement=date(2020, 9, 24),
                cost=534.08,
                used=95,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
            BillingDatum(
                start=date(2020, 6, 24),
                end=date(2020, 6, 30),
                statement=date(2020, 9, 24),
                cost=134.16,
                used=23.89655,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
            BillingDatum(
                start=date(2020, 7, 1),
                end=date(2020, 7, 22),
                statement=date(2020, 9, 24),
                cost=469.08,
                used=75.10345,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
            BillingDatum(
                start=date(2020, 7, 23),
                end=date(2020, 8, 20),
                statement=date(2020, 9, 24),
                cost=686.89,
                used=107,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
            BillingDatum(
                start=date(2020, 8, 21),
                end=date(2020, 9, 20),
                statement=date(2020, 9, 24),
                cost=1031.67,
                used=150,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected, parse_pdf(filename, "2463041637", "ccf"),
        )
        # fire
        expected = [
            BillingDatum(
                start=date(2020, 5, 26),
                end=date(2020, 9, 20),
                statement=date(2020, 9, 24),
                cost=466.97,
                used=0,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected, parse_pdf(filename, "2463041281", "ccf"),
        )

    def test_alt_regexp(self):
        """Parser can extract data from a bill with alternate set of regular expressions."""
        pattern = "datafeeds/scrapers/tests/fixtures/ladwp-%s.pdf"
        expected = [
            BillingDatum(
                start=date(2020, 9, 2),
                end=date(2020, 10, 1),
                statement=date(2020, 10, 5),
                cost=386.52,
                used=840,
                peak=6,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected, parse_pdf(pattern % "202010", "PMY00219-00010473", "kw")
        )
        expected = [
            BillingDatum(
                start=date(2020, 6, 5),
                end=date(2020, 7, 5),
                statement=date(2020, 7, 7),
                cost=357.98,
                used=720,
                peak=4.8,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected, parse_pdf(pattern % "202007", "PMY00219-00010473", "kw")
        )
        expected = [
            BillingDatum(
                start=date(2019, 12, 12),
                end=date(2020, 1, 13),
                statement=date(2020, 1, 14),
                cost=47.48,
                used=0,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected, parse_pdf(pattern % "202001", "00106-00095149", "kw")
        )

    def test_water_sewer(self):
        """Parser can extract water data from a bill with water and sewer data."""
        expected = [
            BillingDatum(
                start=date(2020, 9, 28),
                end=date(2020, 10, 28),
                statement=date(2020, 10, 29),
                cost=2523.12,
                used=411,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected,
            parse_pdf(
                "datafeeds/scrapers/tests/fixtures/ladwp-water-202010.pdf",
                "9479723015",
                "ccf",
            ),
        )

    def test_fire_only(self):
        """Parser can extract fire service data from a bill with only fire service data."""
        expected = [
            BillingDatum(
                start=date(2020, 8, 28),
                end=date(2020, 9, 28),
                statement=date(2020, 9, 29),
                cost=118.72,
                used=0,
                peak=None,
                items=None,
                attachments=None,
                utility_code=None,
            ),
        ]
        self.assertEqual(
            expected,
            parse_pdf(
                "datafeeds/scrapers/tests/fixtures/ladwp-fire-202009.pdf",
                "3631146704",
                "ccf",
            ),
        )


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
    meter_number: str, start_date: date, end_date: date, username: str, password: str,
):
    configuration = LADWPBillPdfConfiguration(
        meter_number=meter_number,
        utility_account_id=meter_number,
        commodity="False",
        account_name=None,
    )
    credentials = Credentials(username, password)
    scraper = LADWPBillPdfScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        bills_handler=ft.partial(test_upload_bills, -1, meter_number, None),
        partial_bills_handler=None,
        readings_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("meter_number", type=str)
    parser.add_argument("commodity", type=str)
    args = parser.parse_args()
    bills = parse_pdf(args.filename, args.meter_number, args.commodity)
    print(bills)
