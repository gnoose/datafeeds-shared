import argparse
from datetime import date
import functools as ft

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.saltriver.billing import (
    SaltRiverBillingConfiguration,
    SaltRiverBillingScraper,
)

"""
    Run this to launch the Salt River billing scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_saltriver_billing.py utility_account_id start end username password
"""


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
    utility_account_id: str,
    start_date: date,
    end_date: date,
    username: str,
    password: str,
):
    configuration = SaltRiverBillingConfiguration(account_id=utility_account_id)
    credentials = Credentials(username, password)
    scraper = SaltRiverBillingScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        bills_handler=ft.partial(
            test_upload_bills, "saltriver-billing", -1, utility_account_id, None
        ),
        partial_bills_handler=None,
        readings_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("utility_account_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.utility_account_id,
        date_parser.parse(args.start).date(),
        date_parser.parse(args.end).date(),
        args.username,
        args.password,
    )
