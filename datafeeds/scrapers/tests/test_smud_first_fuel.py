import argparse
from datetime import date

from dateutil.parser import parse as parse_date

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.smud_first_fuel_interval import (
    SMUDFirstFuelConfiguration,
    SMUDFirstFuelScraper,
)


"""
    Run this to launch the FPL MyAccount scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_smud_first_fuel.py utility_acct service_id start end username password
"""


def test_scraper(
    utility_account_id: str,
    service_id: str,
    start_date: date,
    end_date: date,
    username: str,
    password: str,
):
    configuration = SMUDFirstFuelConfiguration(
        utility_account_id=utility_account_id, service_id=service_id
    )
    credentials = Credentials(username, password)
    scraper = SMUDFirstFuelScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        readings_handler=print,
        bills_handler=None,
        pdfs_handler=None,
        partial_bills_handler=None,
    )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("utility_account", type=str)
    parser.add_argument("service_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.utility_account,
        args.service_id,
        parse_date(args.start).date(),
        parse_date(args.end).date(),
        args.username,
        args.password,
    )
