import argparse
from datetime import date
from unittest import mock

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.duke.billing import DukeBillingConfiguration, DukeBillingScraper

"""
    Run this to launch the Duke billing scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_duke_billing.py utility_acct start end username password
"""


def test_scraper(
    account_id: str, start_date: date, end_date: date, username: str, password: str
):
    configuration = DukeBillingConfiguration(utility="duke", account_id=account_id)
    credentials = Credentials(username, password)
    scraper = DukeBillingScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    with mock.patch("datafeeds.scrapers.duke.pages.upload_bill_to_s3"):
        scraper.scrape(
            readings_handler=None,
            bills_handler=print,
            partial_bills_handler=print,
            pdfs_handler=None,
        )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("account_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.account_id,
        date_parser.parse(args.start).date(),
        date_parser.parse(args.end).date(),
        args.username,
        args.password,
    )
