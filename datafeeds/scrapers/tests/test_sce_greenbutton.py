from datetime import datetime, date
from unittest import TestCase, mock

from datafeeds import config
from datafeeds.common import DateRange, BillingDatum
from datafeeds.scrapers import sce_greenbutton as module
from datafeeds.scrapers.sce_greenbutton import (
    IngestApiError,
    ServiceUpdate,
    Bill,
    Configuration,
    CostDetail,
    process_bill,
    Scraper,
    correct_bills,
)


class MockResponse:
    def __init__(self, status: int, body: dict):
        self.status = status
        self.body = body

    @property
    def status_code(self) -> int:
        return self.status

    def json(self) -> dict:
        return self.body


class TestSCEGreenButtonSync(TestCase):
    def setUp(self) -> None:
        self.config_original = config.INGEST_ENDPOINT
        config.INGEST_ENDPOINT = "https://ingest/"

    def tearDown(self) -> None:
        config.INGEST_ENDPOINT = self.config_original

    def test_process_bill_no_infer(self):
        """A bill with a total should be returned without modification"""
        b = Bill(
            total=10.0,
            use=1.0,
            details=[CostDetail(note="Don't use this.", amount=1000.0)],
        )
        self.assertEqual(b.to_json(), process_bill(b).to_json())

    def test_process_bill_no_data(self):
        """Processing a bill without a total or details should produce a null value."""
        b = Bill(
            start=datetime(2019, 1, 1), end=datetime(2019, 1, 31), total=None, use=None
        )
        self.assertIsNone(process_bill(b))

    def test_process_bill_infer(self):
        """When a bill has details and no total, processing produces a bill with an inferred total."""
        b_in = Bill(
            start=datetime(2019, 1, 1),
            end=datetime(2019, 1, 31),
            details=[
                CostDetail(note="Use this.", amount=1000.0),
                CostDetail(note="Franchise Fee", amount=2000.0),
            ],
        )

        b_out = process_bill(b_in)
        b_expected = Bill(
            start=datetime(2019, 1, 1),
            end=datetime(2019, 1, 31),
            total=1000.0,
            details=[
                CostDetail(note="Use this.", amount=1000.0),
                CostDetail(note="Franchise Fee", amount=2000.0),
            ],
        )
        self.assertEqual(b_expected.to_json(), b_out.to_json())

    def test_execute(self):
        """The scraper can properly convert a ServiceUpdate to a set of scraper results."""

        credentials = None
        dates = DateRange(date(2019, 1, 1), date(2019, 1, 31))
        conf = Configuration(subscription="WVBMMX94X", usage_point="M5OC6U")
        scraper = Scraper(credentials, dates, configuration=conf)

        def _mock_api_response(*args, **kwargs):
            body = ServiceUpdate(
                subscription="WVBMMX94X",
                usage_point="M5OC6U",
                bills=[
                    Bill(
                        start=datetime(2019, 1, 1, 8),
                        end=datetime(2019, 1, 31, 8),
                        total=1000.0,
                        use=2_000_000.0,
                        unit="Wh",
                    )
                ],
            ).to_json()
            return MockResponse(200, body)

        with mock.patch.object(module.requests, "post", new=_mock_api_response):
            result = scraper._execute()

        expected = [
            BillingDatum(
                start=date(2019, 1, 1),
                end=date(2019, 1, 31),
                cost=1000.0,
                used=2000.0,
                peak=None,
                items=None,
                attachments=None,
            )
        ]

        self.assertEqual(expected, result.bills)

    def test_execute_api_error(self):
        """The scraper raises an error in the event an API call to ingest fails."""

        credentials = None
        dates = DateRange(date(2019, 1, 1), date(2019, 1, 31))
        conf = Configuration(subscription="WVBMMX94X", usage_point="M5OC6U")
        scraper = Scraper(credentials, dates, configuration=conf)

        def _mock_api_response(*args, **kwargs):
            return MockResponse(403, None)

        with mock.patch.object(module.log, "error") as log_mock:
            with mock.patch.object(module.requests, "post", new=_mock_api_response):
                with self.assertRaises(IngestApiError):
                    scraper._execute()
                    self.assertEqual(1, log_mock.call_count)

    def test_bill_exclusion_logic(self):
        """The scraper rejects a bill if it has equal cost and 100x the use of another scraped bill."""
        input = [
            BillingDatum(
                start=date(2019, 1, 1),
                end=date(2019, 1, 31),
                cost=1.0,
                used=200.0,
                peak=None,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=date(2019, 2, 1),
                end=date(2019, 2, 28),
                cost=1000.0,
                used=200.0,
                peak=None,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=date(2019, 3, 1),
                end=date(2019, 3, 31),
                cost=1.0,
                used=2.0,
                peak=None,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=date(2019, 4, 1),
                end=date(2019, 4, 30),
                cost=1.0,
                used=None,
                peak=None,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=date(2019, 5, 1),
                end=date(2019, 5, 31),
                cost=10.0,
                used=0.0,
                peak=None,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=date(2019, 6, 1),
                end=date(2019, 6, 30),
                cost=10.0,
                used=0.0,
                peak=None,
                items=None,
                attachments=None,
            ),
        ]

        actual = correct_bills(input)
        self.assertEqual(input[1:], actual)
