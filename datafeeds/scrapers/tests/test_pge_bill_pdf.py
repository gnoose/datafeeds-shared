import argparse
from unittest import mock
from datetime import date
import functools as ft
from typing import List

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.common.typing import BillPdf
from datafeeds.scrapers.pge.bill_pdf import PgeBillPdfScraper, PgeBillPdfConfiguration

"""
    Run this to launch the PGE bill PDF scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_pge_bill_pdf.py utility_acct start end username password
"""


def test_pdf_upload(task_id: str, pdfs: List[BillPdf]):
    print("POST pdfs")
    for pdf in [pdf.to_json() for pdf in pdfs]:
        print("\t%s" % pdf)


def test_scraper(
    utility_account: str, start_date: date, end_date: date, username: str, password: str
):
    configuration = PgeBillPdfConfiguration(
        utility="pge", utility_account=utility_account
    )
    credentials = Credentials(username, password)
    scraper = PgeBillPdfScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    with mock.patch("datafeeds.scrapers.pge.bill_pdf.upload_bill_to_s3"):
        scraper.scrape(
            readings_handler=None,
            bills_handler=None,
            pdfs_handler=ft.partial(test_pdf_upload, None),
        )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("utility_account", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.utility_account,
        date_parser.parse(args.start),
        date_parser.parse(args.end),
        args.username,
        args.password,
    )
