import argparse
from datetime import date

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.svp.interval import SVPIntervalConfiguration, SVPIntervalScraper

"""
    Run this to launch the FPL MyAccount scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_svp_interval.py point_id start end username password
"""


def test_scraper(
    point_id: str, start_date: date, end_date: date, username: str, password: str
):
    configuration = SVPIntervalConfiguration(point_id=point_id)
    credentials = Credentials(username, password)
    scraper = SVPIntervalScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    scraper.scrape(
        readings_handler=print, bills_handler=None, pdfs_handler=None,
    )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("point_id", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.point_id,
        date_parser.parse(args.start),
        date_parser.parse(args.end),
        args.username,
        args.password,
    )
