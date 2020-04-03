from unittest import mock
from datetime import date
import functools as ft
from typing import List

from datafeeds.common.support import Credentials, DateRange
from datafeeds.common.typing import BillPdf
from datafeeds.scrapers.pge.bill_pdf import PgeBillPdfScraper, PgeBillPdfConfiguration


def test_pdf_upload(task_id: str, pdfs: List[BillPdf]):
    print("POST pdfs")
    for pdf in [pdf.to_json() for pdf in pdfs]:
        print("\t%s" % pdf)


def test_scraper():
    # TODO: get command line args
    utility_account = "123"
    start_date = date(2020, 3, 1)
    end_date = date(2020, 4, 1)
    username = "test"
    password = "123"
    configuration = PgeBillPdfConfiguration(utility_account=utility_account)
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
    # $ export PYTHONPATH=$(pwd)
    # $ python datafeeds/scrapers/tests/test_pge_bill_pdf.py
    test_scraper()
