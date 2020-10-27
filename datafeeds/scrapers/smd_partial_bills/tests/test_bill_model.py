from datetime import date, datetime, timedelta
from unittest import TestCase

from datafeeds.common import BillingDatum
from datafeeds.common.typing import BillingDatumItemsEntry
from datafeeds.scrapers.smd_partial_bills.models import Bill


class TestBillModel(TestCase):
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
