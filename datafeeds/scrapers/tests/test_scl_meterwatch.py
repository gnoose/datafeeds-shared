import argparse
from unittest import mock
from datetime import date
import functools as ft
from typing import List

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.scl_meterwatch import SCLMeterWatchConfiguration, SCLMeterWatchScraper

"""
    Run this to launch the scl-meterwatch scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_scl_meterwatch.py service_id start end username 
    password
"""


def test_scraper(
    service_id: str, start_date: date, end_date: date, username: str, password: str
):
    configuration = SCLMeterWatchConfiguration(meter_numbers=service_id)
    credentials = Credentials(username, password)
    scraper = SCLMeterWatchScraper(
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
    parser.add_argument("service_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.service_id,
        date_parser.parse(args.start),
        date_parser.parse(args.end),
        args.username,
        args.password,
    )
