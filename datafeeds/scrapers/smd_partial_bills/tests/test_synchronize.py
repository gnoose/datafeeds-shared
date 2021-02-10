from unittest import TestCase
from datetime import datetime, timedelta, date
from typing import List

from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.common.support import Credentials, DateRange
from datafeeds.models import (
    UtilityService,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.utility_service import UtilityServiceSnapshot

from datafeeds.scrapers.smd_partial_bills.models import (
    Bill as SmdBill,
    CustomerInfo,
    Artifact,
    GreenButtonProvider,
)
from datafeeds.scrapers.smd_partial_bills.synchronizer import (
    relevant_usage_points,
    SmdPartialBillingScraperConfiguration,
    SmdPartialBillingScraper,
)

FIXTURE_01 = """
 2020-02-13 08:00:00 | 31 days   | 2830.22 | 2975 | 2020-03-20 16:01:35.273   |E20S
 2020-01-14 08:00:00 | 30 days   | 3330.11 | 3661 | 2020-02-17 13:54:45.787   |E20S
 2019-12-13 08:00:00 | 32 days   | 3607.54 | 4120 | 2020-01-24 12:19:52.182   |E20S
 2019-11-14 08:00:00 | 29 days   | 3551.68 | 4171 | 2019-12-22 12:16:47.378   |E20P
 2019-11-14 08:00:00 | 29 days   | 3551.68 | 4171 | 2019-12-19 14:28:32.394   |E20P
 2019-10-15 07:00:00 | 30 days   | 4060.83 | 5762 | 2019-11-22 12:14:39.925   |E20P
 2019-10-15 07:00:00 | 30 days   | 4060.83 | 5762 | 2019-11-19 15:50:46.249   |E20P  | SMD corrects the bad charge.
 2019-10-15 07:00:00 | 30 days   | 5060.83 | 5762 | 2019-11-18 13:47:05.77    |E20P  | Incorrect charge.
 2019-09-16 07:00:00 | 29 days   | 3526.45 | 5300 | 2019-10-19 14:02:33.782   |E19S
 2019-08-15 07:00:00 | 32 days   | 3667.59 | 5749 | 2019-10-01 00:11:58.983   |E19S
 2019-08-15 07:00:00 | 32 days   | 3667.59 | 5749 | 2019-09-20 15:19:12.086   |E19S
 2019-07-16 07:00:00 | 30 days   | 3537.64 | 5220 | 2019-08-19 12:47:26.03    |E19S
 2019-06-14 07:00:00 | 32 days   | 3883.03 | 5730 | 2019-07-30 18:22:34.586   |E19S
 2019-06-14 07:00:00 | 32 days   | 3883.03 | 5730 | 2019-07-20 17:59:10.007   |E19S
 2019-05-15 07:00:00 | 30 days   | 3855.08 | 5678 | 2019-07-02 00:16:33.971   |E19S
 2019-05-15 07:00:00 | 30 days   | 3855.08 | 5678 | 2019-06-25 00:12:07.244   |E19S
 2019-05-15 07:00:00 | 30 days   | 3855.08 | 5678 | 2019-06-20 14:30:03.248   |E19S
 2019-04-16 07:00:00 | 29 days   |  3345.9 | 4596 | 2019-05-19 14:07:42.813   |E19S
 2019-03-15 07:00:00 | 32 days   | 4017.51 | 5424 | 2019-04-30 06:50:35.058   |E19S
 2019-03-17 07:00:00 | 32 days   | 8000.51 | 5424 | 2019-04-29 22:55:55.099   |E19S  | Should be dropped due to overlap.
"""

FIXTURE_02 = """
 2020-02-13 08:00:00 | 31 days   | 2830.22 | 2975 | 2020-03-20 16:01:35.273   |E20S
 2020-01-14 08:00:00 | 30 days   | 3330.11 | 3661 | 2020-02-17 13:54:45.787   |E20S
 2019-12-13 08:00:00 | 32 days   | 3607.54 | 4120 | 2020-01-24 12:19:52.182   |E20S
 2019-11-14 08:00:00 | 29 days   | 3551.68 | 4171 | 2019-12-22 12:16:47.378   |E20P
 2019-10-15 07:00:00 | 30 days   | 4060.83 | 5762 | 2019-11-22 12:14:39.925   |E20P
 2019-09-16 07:00:00 | 29 days   | 3526.45 | 5300 | 2019-10-19 14:02:33.782   |E19S
 2019-08-15 07:00:00 | 32 days   | 3667.59 | 5749 | 2019-10-01 00:11:58.983   |E19S
 2019-07-16 07:00:00 | 30 days   | 3537.64 | 5220 | 2019-08-19 12:47:26.03    |E19S
 2019-06-14 07:00:00 | 32 days   | 3883.03 | 5730 | 2019-07-30 18:22:34.586   |E19S
 2019-05-15 07:00:00 | 30 days   | 3855.08 | 5678 | 2019-07-02 00:16:33.971   |E19S
 2019-04-16 07:00:00 | 29 days   |  3345.9 | 4596 | 2019-05-19 14:07:42.813   |E19S
 2019-03-15 07:00:00 | 32 days   | 4017.51 | 5424 | 2019-04-30 06:50:35.058   |E19S
"""

FIXTURE_03 = """
 2018-04-18 07:00:00 | 29 days          | 12445.77 |  94108000 | 2019-02-07 17:41:48.356 | E19S
 2018-05-17 07:00:00 | 32 days          | 13922.82 |  98131000 | 2019-02-07 17:41:48.369 | E19S
 2018-04-18 07:00:00 | 29 days          | 12445.77 |  94108000 | 2019-02-07 17:42:17.69  | E19S
 2018-05-17 07:00:00 | 32 days          | 13922.82 |  98131000 | 2019-02-07 17:42:26.886 | E19S
 2018-04-17 07:00:00 | 1 day            |   330.82 |   3473000 | 2019-02-07 17:42:17.709 | E19S
 2018-04-18 07:00:00 | 29 days          | 12445.77 |  94108000 | 2019-02-07 18:11:30.611 | E19S
 2018-05-17 07:00:00 | 32 days          | 13922.82 |  98131000 | 2019-02-07 18:12:24.697 | E19S
 2018-04-17 07:00:00 | 1 day            |   330.82 |   3473000 | 2019-02-07 18:11:30.625 | E19S
 2018-04-18 07:00:00 | 29 days          | 12445.77 |  94108000 | 2019-02-07 18:13:50.79  | E19S
 2018-05-17 07:00:00 | 32 days          | 13922.82 |  98131000 | 2019-02-07 18:13:50.822 | E19S
"""


def from_fixture(fixture: str) -> List[SmdBill]:
    results = []
    lines = fixture.split("\n")
    for l in lines:
        tokens = l.split("|")
        if len(tokens) != 6:
            continue
        results.append(
            SmdBill(
                start=datetime.strptime(tokens[0].strip(), "%Y-%m-%d %H:%M:%S"),
                duration=timedelta(
                    days=int(tokens[1].replace("day", "").replace("s", "").strip())
                ),
                cost=float(tokens[2]),
                used=int(tokens[3]),
                published=datetime.strptime(tokens[4].strip(), "%Y-%m-%d %H:%M:%S.%f"),
                tariff=tokens[5].strip() or None,
            )
        )

    return results


class TestSynchronizePrimitives(TestCase):
    """
    This test suite confirms that we have the ability to select SMD data relevant to a particular meter and
    unify that data into a single history of bills.
    """

    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        db.session.begin(subtransactions=True)

        # Some developers will have this record already present as a default, so create one only if
        # it isn't present (for example, in Circle CI).
        provider = db.session.query(GreenButtonProvider).get(2)
        if provider is None:
            db.session.add(
                GreenButtonProvider(oid=2, utility="utility:pge", identifier="gridium")
            )
            db.session.flush()

        self.artifact = Artifact(
            provider_oid=2,
            filename="test.xml",
            url="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/Batch/Subscription/67890?correlationID=c54",
        )
        db.session.add(self.artifact)
        db.session.flush()

        us = UtilityService(service_id="12345", account_id="12345")
        us.tariff = "E-19-S"
        us.utility = "utility:pge"

        db.session.add(us)
        db.session.flush()

        self.meter = Meter("meter1", utility_service=us)
        db.session.add(self.meter)
        db.session.flush()

    def tearDown(self):
        db.session.rollback()
        db.session.remove()

    def add_customer_info(self, service_id: str, usage_point: str):
        """Initialize a dummy customer info record."""
        ci = CustomerInfo(
            artifact=self.artifact,
            subscription="dummy sub",
            service_id=service_id,
            usage_point=usage_point,
            self_url="dummy url %s %s" % (service_id, usage_point),
        )
        db.session.add(ci)
        return ci

    def add_bill(self, usage_point: str, start: datetime, duration: timedelta):
        bill = SmdBill(
            subscription="ABCDE",
            usage_point=usage_point,
            start=start,
            duration=duration,
            used=100.0,
            used_unit="wh",
            cost=200.0,
            tariff="E19S",
            self_url="%s-%s"
            % (
                start,
                start + duration,
            ),  # Self URLs must be distinct, this URL is not realistic.
            artifact=self.artifact,
        )
        db.session.add(bill)
        db.session.flush()
        return bill

    def test_usage_point_selection(self):
        """The synchronization task can select relevant usage points from customer info based on service ID."""
        db.session.add(
            MeterDataSource(
                meter=self.meter, name="share-my-data", meta={"usage_point": "FGHIJ"},
            )
        )
        db.session.flush()

        # Real usage points use digit strings, but this makes
        # distinguishing service ids and usage points clearer.
        self.add_customer_info("12345", "ABCDE")
        self.add_customer_info("67890", "FGHIJ")
        self.add_customer_info("11121", "KLMNO")
        self.add_customer_info("31415", "ABCDE")
        self.add_customer_info("12345", "UVWXY")

        actual = relevant_usage_points(self.meter)
        expected = {"ABCDE", "UVWXY", "FGHIJ"}
        self.assertEqual(expected, actual)

    def test_usage_point_selection_no_meta(self):
        """When a meter's data source does not have an initial usage point, one is selected and saved."""
        mds = MeterDataSource(meter=self.meter, name="share-my-data")
        db.session.add(mds)
        db.session.flush()

        # Real usage points use digit strings, but this makes
        # distinguishing service ids and usage points clearer.
        self.add_customer_info("12345", "ABCDE")

        actual = relevant_usage_points(self.meter)
        expected = {"ABCDE"}
        self.assertEqual(expected, actual)

        db.session.flush()
        self.assertEqual("ABCDE", mds.meta["usage_point"])

    def test_usage_point_selection_from_snapshot_table(self):
        """Can select usage points across multiple historical service ids from UtilityServiceSnapshot table"""
        snapshot = UtilityServiceSnapshot(
            service=self.meter.service,
            service_id="49392",
            system_created=datetime.utcnow(),
            system_modified=datetime.utcnow(),
            service_modified=datetime.utcnow() - timedelta(weeks=2),
        )
        db.session.add(snapshot)
        db.session.flush()

        self.add_customer_info("12345", "ABCDE")
        self.add_customer_info("49392", "EEEFJ")
        self.add_customer_info("99101", "UFSSL")

        actual = relevant_usage_points(self.meter)
        expected = {"EEEFJ", "ABCDE"}
        self.assertEqual(actual, expected)

    def test_bill_precedence(self):
        """When bills are selected from SMD tables they are presented in chronological order with no overlaps."""
        raw_bills = from_fixture(FIXTURE_01)
        actual = SmdBill.unify_bills(raw_bills)
        expected = from_fixture(FIXTURE_02)

        actual_docs = [b.as_dict() for b in actual]
        expected_docs = list(reversed([b.as_dict() for b in expected]))

        self.assertEqual(expected_docs, actual_docs)

    def test_scraper(self):
        """The Synchronizer can extract partial bill data from the SMD tables."""
        self.add_customer_info("12345", "ABCDE")
        self.add_bill("ABCDE", datetime(2020, 1, 1), timedelta(days=30))
        self.add_bill("ABCDE", datetime(2020, 2, 1), timedelta(days=28))
        self.add_bill("ABCDE", datetime(2020, 3, 1), timedelta(days=30))

        config = SmdPartialBillingScraperConfiguration(self.meter)
        scraper = SmdPartialBillingScraper(
            Credentials(None, None),
            DateRange(date(2019, 12, 1), date(2020, 5, 1)),
            configuration=config,
        )
        results = scraper._execute()
        self.assertEqual(3, len(results.tnd_bills))

        # Perform a quick sanity check that we found the right bill dates.
        # Conversion from an SMD bill to a billing datum is tested elsewhere in depth.

        # Note: Dates intentionally do not line up with SMD records; this ensures the dates
        # agree with PDF bill data.
        expected = [
            (date(2020, 1, 2), date(2020, 1, 31)),
            (date(2020, 2, 2), date(2020, 2, 29)),
            (date(2020, 3, 2), date(2020, 3, 31)),
        ]

        actual = [(b.start, b.end) for b in results.tnd_bills]
        self.assertEqual(expected, actual)

    def test_one_day_bills(self):
        """
        Test that one-day bills from SMD are adjusted by adding one day to the end date,
        and adjusting the subsequent bill if necessary.
        """
        raw_bills = from_fixture(FIXTURE_03)
        actual = SmdBill.unify_bills(raw_bills)
        adjusted = SmdBill.adjust_single_day_bills(actual)
        self.assertEqual(len(adjusted), 3)
        self.assertEqual(adjusted[0].initial, date(2018, 4, 18))
        self.assertEqual(
            adjusted[0].closing, date(2018, 4, 19), "Was previously 4/18/18"
        )

        self.assertEqual(
            adjusted[1].initial,
            date(2018, 4, 20),
            "Previously 4/19/18, had to be adjusted w/ 4/18 bill.",
        )
        self.assertEqual(adjusted[1].closing, date(2018, 5, 17))

        self.assertEqual(adjusted[2].initial, date(2018, 5, 18))
        self.assertEqual(adjusted[2].closing, date(2018, 6, 18))
