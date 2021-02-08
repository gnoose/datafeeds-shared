from datetime import date, datetime, timedelta
from unittest import TestCase

from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.common.typing import BillingDatum
from datafeeds.common.typing import BillingDatumItemsEntry
from datafeeds.models import UtilityService, Meter
from datafeeds.scrapers.smd_partial_bills.models import (
    Bill,
    GreenButtonProvider,
    Artifact,
    CustomerInfo,
    IntervalData,
    ReadingType,
)


class TestBillModel(TestCase):
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

        us = UtilityService(
            service_id="current_service_id", account_id="current_account_id"
        )
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

    def test_initial_closing(self):
        """An SMD bill can compute its initial and closing dates based on its start and duration."""
        b = Bill(start=datetime(2019, 1, 1, 7), duration=timedelta(days=30))
        self.assertEqual(date(2019, 1, 2), b.initial)
        self.assertEqual(date(2019, 1, 31), b.closing)

    def test_overlap(self):
        """SMD bills can be compared for overlaps based upon their initial and closing dates."""
        b0 = Bill(start=datetime(2019, 1, 1, 7), duration=timedelta(days=30))
        b1 = Bill(start=datetime(2019, 1, 15, 7), duration=timedelta(days=30))
        b2 = Bill(
            start=datetime(2019, 1, 1, 7) + timedelta(days=30),
            duration=timedelta(days=30),
        )

        self.assertTrue(b0.overlaps(b0))
        self.assertTrue(b0.overlaps(b1))
        self.assertTrue(b1.overlaps(b0))
        self.assertFalse(b0.overlaps(b2))
        self.assertFalse(b2.overlaps(b0))

    def test_bill_conversion_electric(self):
        """An SMD bill can be converted to a BillingDatum, with appropriate unit conversions for electric meters."""
        b = Bill(
            start=datetime(2019, 1, 1, 7),
            duration=timedelta(days=30),
            used=150000.0,
            used_unit="Wh",
            cost=11.0,
            _line_items=[
                {
                    "note": "Customer Charge",
                    "unit": "Wh",
                    "amount": 91.99,
                    "quantity": 20000.0,
                },
                {
                    "note": "Utility Users' Tax",
                    "unit": "Wh",
                    "amount": 148.63,
                    "quantity": 0.0,
                },
                {
                    "note": "Part Peak Energy Charge",
                    "unit": "Wh",
                    "amount": 1302.24,
                    "quantity": 11703443.200000001,
                },
                {
                    "note": "Peak Energy Charge",
                    "unit": "Wh",
                    "amount": 1560.6,
                    "quantity": 10282000.0,
                },
                {
                    "note": "Max Demand Charge",
                    "unit": "W",
                    "amount": 1906.65,
                    "quantity": 157440.0,
                },
                {
                    "note": "Max Peak Demand Charge",
                    "unit": "W",
                    "amount": 1966.33,
                    "quantity": 152960.0,
                },
                {
                    "note": "Off Peak Energy Charge",
                    "unit": "Wh",
                    "amount": 2392.15,
                    "quantity": 28326240.0,
                },
            ],
        )

        actual = b.to_billing_datum()
        expected = BillingDatum(
            start=date(2019, 1, 2),
            end=date(2019, 1, 31),
            cost=11.0,
            used=150,
            peak=157,
            statement=date(2019, 1, 31),
            items=[],
            attachments=None,
            utility_code="",
        )

        self.assertEqual(expected.start, actual.start)
        self.assertEqual(expected.end, actual.end)
        self.assertEqual(expected.cost, actual.cost)
        self.assertEqual(expected.used, actual.used)
        self.assertEqual(expected.peak, actual.peak)

        expected = [
            BillingDatumItemsEntry(
                description="Customer Charge",
                unit="kWh",
                total=91.99,
                quantity=20.0000,
                rate=None,
                kind="other",
            ),
            BillingDatumItemsEntry(
                description="Utility Users' Tax",
                unit="kWh",
                total=148.63,
                quantity=0.0,
                rate=None,
                kind="other",
            ),
            BillingDatumItemsEntry(
                description="Part Peak Energy Charge",
                unit="kWh",
                total=1302.24,
                quantity=11703.443200000001,
                rate=None,
                kind="use",
            ),
            BillingDatumItemsEntry(
                description="Peak Energy Charge",
                unit="kWh",
                total=1560.6,
                quantity=10282.0000,
                rate=None,
                kind="use",
            ),
            BillingDatumItemsEntry(
                description="Max Demand Charge",
                unit="kW",
                total=1906.65,
                quantity=157.4400,
                rate=None,
                kind="demand",
            ),
            BillingDatumItemsEntry(
                description="Max Peak Demand Charge",
                unit="kW",
                total=1966.33,
                quantity=152.9600,
                rate=None,
                kind="demand",
            ),
            BillingDatumItemsEntry(
                description="Off Peak Energy Charge",
                unit="kWh",
                total=2392.15,
                quantity=28326.2400,
                rate=None,
                kind="use",
            ),
        ]
        self.assertEqual(expected, actual.items)

    def test_bill_conversion_gas(self):
        """An SMD bill can be converted to a BillingDatum, with appropriate unit conversions for gas meters."""
        b = Bill(
            start=datetime(2017, 4, 25, 7),
            duration=timedelta(days=29),
            used_unit="therm",
            used=4400,
            _line_items=[],
            cost=4609.48,
        )

        actual = b.to_billing_datum()

        expected = BillingDatum(
            start=date(2017, 4, 26),
            end=date(2017, 5, 24),
            cost=4609.48,
            used=4400,
            peak=None,
            items=[],
            statement=date(2017, 5, 24),
            attachments=None,
            utility_code="",
        )

        self.assertEqual(expected.start, actual.start)
        self.assertEqual(expected.end, actual.end)
        self.assertEqual(expected.cost, actual.cost)
        self.assertEqual(expected.used, actual.used)
        self.assertEqual(expected.peak, actual.peak)
        self.assertEqual(expected.items, actual.items)

    def add_customer_info(
        self, service_id: str, usage_point: str, subscription: str, published: datetime
    ):
        """Initialize a dummy customer info record."""
        ci = CustomerInfo(
            artifact=self.artifact,
            subscription=subscription,
            service_id=service_id,
            usage_point=usage_point,
            self_url="dummy url %s %s" % (service_id, usage_point),
            published=published,
        )
        db.session.add(ci)
        return ci

    def test_bill_service_config(self):
        """Test utility account id and service id are pulled from smd_customer_info table and stored on billing datum
        if corresponding records exist"""

        usage_point = "test_usage_point"
        subscription = "test_subscription"
        service_id = "earlier_service_id"

        self.add_customer_info(
            "first_service_id", usage_point, subscription, datetime(2017, 1, 1)
        )
        # This customer info record should be selected - its published date is just before the new bill's published date
        self.add_customer_info(
            service_id, usage_point, subscription, datetime(2017, 4, 1)
        )
        self.add_customer_info(
            "third_service_id", usage_point, subscription, datetime(2017, 7, 1)
        )

        b = Bill(
            start=datetime(2017, 4, 25, 7),
            duration=timedelta(days=29),
            used_unit="Wh",
            used=4400,
            _line_items=[],
            cost=1344.3,
            published=datetime(2017, 5, 1),
            usage_point=usage_point,
            subscription=subscription,
        )
        actual = b.to_billing_datum(self.meter.utility_service)

        expected = BillingDatum(
            start=date(2017, 4, 26),
            end=date(2017, 5, 24),
            cost=1344.3,
            used=4,
            peak=None,
            items=[],
            statement=date(2017, 5, 24),
            attachments=None,
            utility_code=None,
            utility_account_id=None,
            utility="utility:pge",
            service_id="earlier_service_id",
            third_party_expected=False,
        )
        self.assertEqual(actual, expected)

    def test_bill_published_before_smd_customer_info_record(self):
        """Test earliest smd customer info record used if bill published before first smd customer info record"""

        usage_point = "test_usage_point"
        subscription = "test_subscription"

        self.add_customer_info(
            "first_service_id", usage_point, subscription, datetime(2018, 5, 5)
        )
        self.add_customer_info(
            "updated_service_id", usage_point, subscription, datetime(2019, 1, 1)
        )

        b = Bill(
            start=datetime(2017, 1, 1, 7),
            duration=timedelta(days=29),
            used_unit="Wh",
            used=5400,
            _line_items=[],
            cost=3411.2,
            published=datetime(2017, 2, 1),
            usage_point=usage_point,
            subscription=subscription,
        )

        actual = b.to_billing_datum(self.meter.utility_service)

        expected = BillingDatum(
            start=date(2017, 1, 2),
            end=date(2017, 1, 30),
            cost=3411.2,
            used=5,
            peak=None,
            items=[],
            statement=date(2017, 1, 30),
            attachments=None,
            utility_code=None,
            utility_account_id=None,
            utility="utility:pge",
            service_id="first_service_id",
            third_party_expected=False,
        )

        self.assertEqual(actual, expected)

    def test_cca_indicators_in_line_items(self):
        """Test partial_bill.third_party_expected is set to True if PCIA or generation
        credits found in PG&E T&D line items
        """
        usage_point = "test_usage_point"
        subscription = "test_subscription"

        _line_items = [
            {
                "note": "Generation Credit",
                "unit": "Wh",
                "amount": -0.92,
                "quantity": 0.0,
            },
            {
                "note": "Franchise Fee Surcharge",
                "unit": "Wh",
                "amount": 0.01,
                "quantity": 0.0,
            },
            {
                "note": "Peak Energy Charge",
                "unit": "Wh",
                "amount": 0.02,
                "quantity": 80.0,
            },
            {
                "note": "Part Peak Energy Charge",
                "unit": "Wh",
                "amount": 0.06,
                "quantity": 240.0,
            },
            {
                "note": "Power Cost Incentive Adjustment",
                "unit": "Wh",
                "amount": 0.25,
                "quantity": 0.0,
            },
            {
                "note": "Utility Users' Tax",
                "unit": "Wh",
                "amount": 1.55,
                "quantity": 0.0,
            },
            {
                "note": "Off Peak Energy Charge",
                "unit": "Wh",
                "amount": 2.15,
                "quantity": 9920.0,
            },
            {
                "note": "Customer Charge",
                "unit": "Wh",
                "amount": 19.06,
                "quantity": 29000.0,
            },
        ]

        b = Bill(
            start=datetime(2017, 1, 1, 7),
            duration=timedelta(days=29),
            used_unit="Wh",
            used=5400,
            _line_items=_line_items,
            cost=3411.2,
            published=datetime(2017, 2, 1),
            usage_point=usage_point,
            subscription=subscription,
        )

        actual = b.to_billing_datum(self.meter.utility_service)

        expected_line_items = [
            BillingDatumItemsEntry(
                description="Generation Credit",
                quantity=0.0,
                rate=None,
                total=-0.92,
                kind="other",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Franchise Fee Surcharge",
                quantity=0.0,
                rate=None,
                total=0.01,
                kind="other",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Peak Energy Charge",
                quantity=0.08,
                rate=None,
                total=0.02,
                kind="use",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Part Peak Energy Charge",
                quantity=0.24,
                rate=None,
                total=0.06,
                kind="use",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Power Cost Incentive Adjustment",
                quantity=0.0,
                rate=None,
                total=0.25,
                kind="other",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Utility Users' Tax",
                quantity=0.0,
                rate=None,
                total=1.55,
                kind="other",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Off Peak Energy Charge",
                quantity=9.92,
                rate=None,
                total=2.15,
                kind="use",
                unit="kWh",
            ),
            BillingDatumItemsEntry(
                description="Customer Charge",
                quantity=29.0,
                rate=None,
                total=19.06,
                kind="other",
                unit="kWh",
            ),
        ]

        expected = BillingDatum(
            start=date(2017, 1, 2),
            end=date(2017, 1, 30),
            cost=3411.2,
            used=5,
            peak=None,
            items=expected_line_items,
            statement=date(2017, 1, 30),
            attachments=None,
            utility_code=None,
            utility_account_id=None,
            utility="utility:pge",
            service_id=None,
            third_party_expected=True,
        )
        self.assertEqual(actual, expected)

    def test_third_party_expected_for_gas(self):
        b = Bill(
            start=datetime(2017, 1, 1, 7),
            duration=timedelta(days=29),
            used_unit="therm",
            used=679.0,
            _line_items=[],
            cost=625.64,
            published=datetime(2017, 2, 1),
            usage_point="test_usage_point",
            subscription="test",
        )

        actual = b.to_billing_datum(self.meter.utility_service)

        expected = BillingDatum(
            start=date(2017, 1, 2),
            end=date(2017, 1, 30),
            cost=625.64,
            used=679.0,
            peak=None,
            items=[],
            statement=date(2017, 1, 30),
            attachments=None,
            utility_code=None,
            utility_account_id=None,
            utility="utility:pge",
            service_id=None,
            third_party_expected=None,
        )
        self.assertEqual(actual, expected)

    def test_third_party_expected_for_NEM_tariff(self):
        b = Bill(
            start=datetime(2019, 2, 28, 0),
            duration=timedelta(days=27),
            used_unit="Wh",
            used=0,
            _line_items=[],
            cost=35.51,
            published=datetime(2017, 2, 1),
            usage_point="test_usage_point",
            subscription="test",
            tariff="NEM A-1-B",
        )

        actual = b.to_billing_datum(self.meter.utility_service)

        expected = BillingDatum(
            start=date(2019, 3, 1),
            end=date(2019, 3, 27),
            cost=35.51,
            used=0,
            peak=None,
            items=[],
            statement=date(2019, 3, 27),
            attachments=None,
            utility_code="NEM A-1-B",
            utility_account_id=None,
            utility="utility:pge",
            service_id=None,
            third_party_expected=None,
        )
        self.assertEqual(actual, expected)

    def test_third_party_expected_for_NEM_reverse_flow_channel(self):
        """Test that the presence of a reverse flow channel means we are unsure if a service is
        bundled for a given billing period.
        """
        b = Bill(
            start=datetime(2019, 2, 28, 0),
            duration=timedelta(days=27),
            used_unit="Wh",
            used=0,
            _line_items=[],
            cost=35.51,
            published=datetime(2017, 2, 1),
            usage_point="test_usage_point",
            subscription="test",
            tariff="B10S",
        )

        reading_type = ReadingType(
            kind="energy",
            commodity="electricity SecondaryMetered",
            unit_of_measure="Wh",
            flow_direction="reverse",
            self_url="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/ReadingType/test='",
            artifact=self.artifact,
        )
        db.session.add(reading_type)
        db.session.flush()

        interval_data = IntervalData(
            usage_point="test_usage_point",
            subscription="test",
            readings=[1] * 96,
            start=datetime(2019, 2, 28, 0),
            duration=timedelta(days=1),
            reading_type_oid=reading_type.oid,
            artifact=self.artifact,
            self_url="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/Subscription/test/UsagePoint/"
            "test_usage_point/MeterReading/test==/IntervalBlock/test",
        )
        db.session.add(interval_data)
        actual = b.to_billing_datum(self.meter.utility_service)

        expected = BillingDatum(
            start=date(2019, 3, 1),
            end=date(2019, 3, 27),
            cost=35.51,
            used=0,
            peak=None,
            items=[],
            statement=date(2019, 3, 27),
            attachments=None,
            utility_code="B10S",
            utility_account_id=None,
            utility="utility:pge",
            service_id=None,
            third_party_expected=None,
        )
        self.assertEqual(actual, expected)
