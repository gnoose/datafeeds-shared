import argparse
from datetime import date
import functools as ft

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.sce_react.basic_billing import (
    SceReactBasicBillingConfiguration,
    SceReactBasicBillingScraper,
)

"""
    Run this to launch the SCE basic billing scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_sce_react_basic_billing.py \\
          service_id start end username password --gen_service_id 123

    If --gen_service_id is passed, get generation bills by combining dates and usage from bills for
    service_id with cost values from gen_service_id.
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


def test_upload_partial_bills(meter, configuration, task_id, bills):
    print("Partial bill results:\n")
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
    service_id: str,
    gen_service_id: str,
    start_date: date,
    end_date: date,
    username: str,
    password: str,
):
    is_partial = gen_service_id is not None
    configuration = SceReactBasicBillingConfiguration(
        service_id=service_id,
        gen_service_id=gen_service_id,
        scrape_partial_bills=is_partial,
        scrape_bills=not is_partial,
    )
    credentials = Credentials(username, password)
    scraper = SceReactBasicBillingScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        bills_handler=ft.partial(test_upload_bills, -1, service_id, None),
        partial_bills_handler=ft.partial(test_upload_partial_bills, None, None, None),
        readings_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("service_id", type=str)
    parser.add_argument("--gen_service_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.service_id,
        args.gen_service_id,
        date_parser.parse(args.start).date(),
        date_parser.parse(args.end).date(),
        args.username,
        args.password,
    )
