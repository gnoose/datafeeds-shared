import argparse
from datetime import date

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.fpl_myaccount import (
    FPLMyAccountConfiguration,
    FPLMyAccountScraper,
)

"""
    Run this to launch the FPL MyAccount scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_fpl_my_account.py utility_acct start end username
    password
"""


def test_scraper(
    account_number: str, start_date: date, end_date: date, username: str, password: str
):
    configuration = FPLMyAccountConfiguration(account_number=account_number)
    credentials = Credentials(username, password)
    scraper = FPLMyAccountScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        readings_handler=print,
        bills_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("account_number", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.account_number,
        date_parser.parse(args.start, dayfirst=True).date(),
        date_parser.parse(args.end, dayfirst=True).date(),
        args.username,
        args.password,
    )
