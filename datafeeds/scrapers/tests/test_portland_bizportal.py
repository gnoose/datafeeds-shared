import argparse
from datetime import date, timedelta
import functools as ft
import os
import unittest
from unittest import mock

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.common.typing import BillingDatum
from datafeeds.common.typing import make_billing_pdf_attachment
from datafeeds.scrapers import portland_bizportal as pb
from datafeeds.scrapers.portland_bizportal import (
    PortlandBizportalConfiguration,
    PortlandBizportalScraper,
)

TEST_DIR = os.path.split(__file__)[0]


"""
    Run this to launch the PGE bill PDF scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_portland_bizportal.py utility_account service_id \
        account_group account_number username password --start 2020-11-01 --end 2021-02-10

    --start and --end are optional; if not set, will get bills for previous 90 days
"""


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


def test_upload_bills(meter_oid, service_id, task_id, bills):
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
    utility_account: str,
    service_id: str,
    account_group: str,
    account_number: str,
    start_date: date,
    end_date: date,
    username: str,
    password: str,
):
    """Launch a Chrome browser to test the scraper."""
    configuration = PortlandBizportalConfiguration(
        utility="utility:portland-ge",
        utility_account_id=utility_account,
        account_group=account_group,
        bizportal_account_number=account_number,
        service_id=service_id,
    )
    credentials = Credentials(username, password)
    scraper = PortlandBizportalScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    with mock.patch("datafeeds.scrapers.pge.bill_pdf.upload_bill_to_s3"):
        scraper.scrape(
            readings_handler=None,
            bills_handler=ft.partial(
                test_upload_bills, -1, service_id, None, "portland-bizportal"
            ),
            partial_bills_handler=None,
            pdfs_handler=None,
        )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("utility_account", type=str)
    parser.add_argument("service_id", type=str)
    parser.add_argument("account_group", type=str)
    parser.add_argument("account_number", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()
    if args.start:
        start_dt = date_parser.parse(args.start).date()
    else:
        start_dt = date.today() - timedelta(days=90)
    if args.end:
        end_dt = date_parser.parse(args.end).date()
    else:
        end_dt = date.today()
    test_scraper(
        args.utility_account,
        args.service_id,
        args.account_group,
        args.account_number,
        start_dt,
        end_dt,
        args.username,
        args.password,
    )
