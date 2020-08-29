import argparse
from unittest import mock
from datetime import date
import functools as ft
from unittest.mock import MagicMock

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.scrapers.sce_react.energymanager_greenbutton import (
    SceReactEnergyManagerGreenButtonConfiguration,
    SceReactEnergyManagerGreenButtonScraper,
)

"""
    Run this to launch the SCE GreenButton scraper:

    $ export PYTHONPATH=$(pwd)
    $ python datafeeds/scrapers/tests/test_sce_react_greenbutton.py service_id "address" start end username password
"""


def test_upload_readings(
    transforms, meter_oid: int, scraper: str, task_id: str, readings
):
    print("readings=\n", readings)


def test_scraper(
    service_id: str,
    address: str,
    start_date: date,
    end_date: date,
    username: str,
    password: str,
):
    configuration = SceReactEnergyManagerGreenButtonConfiguration(
        service_id=service_id,
        meta={"serviceAccountAddress": address},
        meter=MagicMock(),
    )
    credentials = Credentials(username, password)
    scraper = SceReactEnergyManagerGreenButtonScraper(
        credentials, DateRange(start_date, end_date), configuration
    )
    scraper.start()
    with mock.patch("datafeeds.scrapers.pge.bill_pdf.upload_bill_to_s3"):
        scraper.scrape(
            readings_handler=ft.partial(test_upload_readings, None),
            bills_handler=None,
            pdfs_handler=None,
            partial_bills_handler=None,
        )
    scraper.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("service_id", type=str)
    parser.add_argument("address", type=str)
    parser.add_argument("start", type=str)
    parser.add_argument("end", type=str)
    parser.add_argument("username", type=str)
    parser.add_argument("password", type=str)
    args = parser.parse_args()
    test_scraper(
        args.service_id,
        args.address,
        date_parser.parse(args.start).date(),
        date_parser.parse(args.end).date(),
        args.username,
        args.password,
    )
