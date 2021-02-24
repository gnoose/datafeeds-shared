import argparse
from datetime import date
import functools as ft
from unittest.mock import MagicMock

from dateutil import parser as date_parser

from datafeeds.common.support import Credentials, DateRange
from datafeeds.models import UtilityService
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


def setup_fixture():
    data = {}
    # no change
    values = {
        "tariff": "TD-TOU-GS-2-D",
        "utility": "utility:sce",
        "gen_tariff": "CPA-TOU-GS-2-D",
        "gen_utility": "utility:clean-power-alliance",
        "gen_utility_account_id": "2-38-849-8875",
        "provider_type": "tnd-only",
    }
    initial = UtilityService(
        service_id="3-045-3661-03",
        account_id="2-38-849-8875",
        gen_service_id="3-048-4172-81",
    )
    expected = UtilityService(
        service_id="3-045-3661-03",
        account_id="2-38-849-8875",
        gen_service_id="3-048-4172-81",
    )
    for key in values:
        setattr(initial, key, values[key])
        setattr(expected, key, values[key])
    data["3-045-3661-03"] = {"initial": initial, "expected": expected}

    # add generation
    values = {
        "tariff": "TOU-GS-1-D",
        "utility": "utility:sce",
        "provider_type": "bundled",
    }
    initial = UtilityService(
        service_id="3-045-3661-31", account_id="2-38-849-8875", gen_service_id=None
    )
    expected = UtilityService(
        service_id="3-045-3661-31",
        account_id="2-38-849-8875",
        gen_service_id="3-048-4158-63",
    )
    for key in values:
        setattr(initial, key, values[key])
        setattr(expected, key, values[key])
    values = {
        "tariff": "TD-TOU-GS-1-D",
        "gen_service_id": "3-048-4158-63",
        "gen_tariff": "CPA-TOU-GS-1-D",
        "gen_utility": "utility:clean-power-alliance",
        "gen_utility_account_id": "2-38-849-8875",
        "provider_type": "tnd-only",
    }
    for key in values:
        setattr(expected, key, values[key])
    data["3-045-3661-31"] = {"initial": initial, "expected": expected}

    # remove generation
    values = {
        "tariff": "TD-TOU-GS-3D",
        "utility": "utility:sce",
        "provider_type": "tnd-only",
        "gen_tariff": "CPA-TOU-GS-3D",
        "gen_utility": "utility:clean-power-alliance",
        "gen_utility_account_id": "2-03-240-2471",
    }
    initial = UtilityService(
        service_id="3-010-5590-40",
        account_id="2-03-240-2471",
        gen_service_id="3-010-5590-00",
    )
    expected = UtilityService(
        service_id="3-010-5590-40", account_id="2-03-240-2471", gen_service_id=None
    )
    for key in values:
        setattr(initial, key, values[key])
        setattr(expected, key, values[key])
    values = {
        "tariff": "TOU-GS-3D",
        "gen_utility": None,
        "gen_utility_account_id": None,
        "provider_type": "bundled",
    }
    for key in values:
        setattr(expected, key, values[key])
    data["3-010-5590-40"] = {"initial": initial, "expected": expected}
    return data


def mock_set_tariff_from_utility_code(utility_tariff_code: str, provider_type: str):
    prefix = {"bundled": "", "tnd-only": "TD-", "generation-only": "CPA-"}
    return prefix[provider_type] + utility_tariff_code


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
    fixture = setup_fixture().get(service_id)
    if fixture:
        scraper.utility_service = fixture["initial"]
        set_tariff_mock = MagicMock()
        set_tariff_mock.return_value = mock_set_tariff_from_utility_code
        scraper.utility_service.set_tariff_from_utility_code = set_tariff_mock
    scraper.start()
    scraper.scrape(
        bills_handler=ft.partial(test_upload_bills, -1, service_id, None),
        partial_bills_handler=ft.partial(test_upload_partial_bills, None, None, None),
        readings_handler=None,
        pdfs_handler=None,
    )
    scraper.stop()
    if fixture:
        print("field\tactual\texpected\tmatch?")
        fields = [
            "service_id",
            "tariff",
            "utility_account_id",
            "gen_service_id",
            "gen_tariff",
            "gen_utility",
            "gen_utility_account_id",
            "provider_type",
        ]
        matches = []
        for field in fields:
            actual = getattr(scraper.utility_service, field)
            expected = getattr(fixture["expected"], field)
            print(f"{field}\t{actual}\t{expected}\t{actual == expected}")
            if actual == expected:
                matches.append(field)
        if matches == fields:
            print("\nOK")
        else:
            print(f"\nFAILED: mismatches = {set(fields) - set(matches)}")


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
