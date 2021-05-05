import unittest
from unittest import mock

import random
from datetime import date, datetime, timedelta

from datafeeds import db
from datafeeds.common import upload, test_utils
from datafeeds.common.exceptions import InvalidMeterDataException
from datafeeds.common.partial_billing import PartialBillValidator
from datafeeds.common.typing import (
    BillingDatum,
    BillPdf,
    OverlappedBillingDataDateRangeError,
    NoFutureBillsError,
    AttachmentEntry,
    BillingDatumItemsEntry,
    Status,
)
from datafeeds.common.upload import _upload_bills_to_services
from datafeeds.models.billaudit import BillAudit, WorkflowState
from datafeeds.models.meter import ProductEnrollment, Building
from datafeeds.models.utility_service import TND_ONLY

from datafeeds.scrapers.sce_react.energymanager_billing import (
    SceReactEnergyManagerBillingConfiguration,
)

from datafeeds.models.bill import (
    Bill,
    PartialBill,
    PartialBillProviderType,
    InvalidBillError,
)

billing_data = [
    BillingDatum(
        start=datetime(2019, 1, 6),
        end=datetime(2019, 2, 3),
        cost=987.76,
        used=4585.0,
        peak=25.0,
        items=None,
        attachments=[],
        statement=datetime(2019, 2, 3),
        utility_code=None,
    ),
    BillingDatum(
        start=datetime(2019, 2, 4),
        end=datetime(2019, 3, 4),
        cost=882.39,
        used=4787.0,
        peak=54.0,
        items=None,
        attachments=[],
        statement=datetime(2019, 3, 4),
        utility_code=None,
    ),
    BillingDatum(
        start=datetime(2019, 3, 5),
        end=datetime(2019, 4, 2),
        cost=706.5,
        used=3072.0,
        peak=45.0,
        items=None,
        attachments=[],
        statement=datetime(2019, 4, 2),
        utility_code=None,
    ),
]  # Intentionally has datetimes instead of dates


class TestPartialBillProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]
        self.meter = meters[0]
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
            scrape_bills=False,
            scrape_partial_bills=True,
        )

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def _get_partial_bill_from_billing_datum(self, bd: BillingDatum) -> PartialBill:
        return (
            db.session.query(PartialBill)
            .filter(PartialBill.initial == bd.start)
            .filter(PartialBill.closing == bd.end)
            .filter(PartialBill.cost == bd.cost)
            .filter(PartialBill.used == bd.used)
            .filter(PartialBill.peak == bd.peak)
            .first()
        )

    @mock.patch("datafeeds.common.partial_billing.PartialBillProcessor.log_summary")
    def test_create_partial_bills(self, mocked_logging):
        service = self.meter.utility_service
        service.tariff = "TOU-GS-3D"
        db.session.add(service)

        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )

        self.assertEqual(partial_bills.count(), 0)

        # Three new partial bills added for the given service
        status = upload.upload_partial_bills(
            self.meter, None, billing_data, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(status, Status.SUCCEEDED)
        self.assertEqual(partial_bills.count(), 3)
        all_bills = partial_bills.all()
        modified_dates = [pb.modified for pb in all_bills]
        original_bill = all_bills[0]
        second_bill = all_bills[1]
        third_bill = all_bills[2]
        original_modified = original_bill.modified
        self.assertEqual(original_bill.initial, billing_data[0].start)
        self.assertEqual(original_bill.closing, billing_data[0].end)
        self.assertEqual(original_bill.cost, float(billing_data[0].cost))
        self.assertEqual(original_bill.peak, float(billing_data[0].peak))
        self.assertIsNone(original_bill.third_party_expected, "default value")
        self.assertEqual(
            original_bill.utility_code,
            None,
            "utility code is None if None scraped",
        )

        # No new partial bills have arrived, so no changes made
        upload.upload_partial_bills(
            self.meter, None, billing_data, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 3)
        self.assertEqual([pb.modified for pb in partial_bills.all()], modified_dates)

        altered_cost = [
            BillingDatum(
                start=date(2019, 1, 6),
                end=date(2019, 2, 3),
                cost=988.76,
                used=4585.0,
                peak=25.0,
                items=None,
                attachments=[],
                statement=date(2019, 2, 3),
                utility_code=None,
            )
        ]
        # Existing bill superseded because new partial bill with new cost uploaded
        status = upload.upload_partial_bills(
            self.meter, None, altered_cost, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(status, Status.SUCCEEDED)
        self.assertEqual(partial_bills.count(), 4)
        replacement_bill = self._get_partial_bill_from_billing_datum(altered_cost[0])

        self.assertEqual(original_bill.superseded_by, replacement_bill.oid)
        self.assertIsNone(replacement_bill.superseded_by)
        self.assertEqual(original_bill.initial, replacement_bill.initial)
        self.assertEqual(original_bill.closing, replacement_bill.closing)
        self.assertNotEqual(original_bill.cost, replacement_bill.cost)
        self.assertEqual(original_bill.peak, replacement_bill.peak)
        self.assertNotEqual(original_bill.modified, original_modified)

        overlapping_bill_datum = [
            BillingDatum(
                start=date(2019, 2, 25),
                end=date(2019, 3, 25),
                cost=706.5,
                used=3072.0,
                peak=45.0,
                items=None,
                attachments=[],
                statement=date(2019, 3, 25),
                utility_code=None,
            )
        ]
        # New bill overlaps dates with two existing partial bills
        upload.upload_partial_bills(
            self.meter, None, overlapping_bill_datum, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        overlapping_bill = self._get_partial_bill_from_billing_datum(
            overlapping_bill_datum[0]
        )
        self.assertEqual(partial_bills.count(), 5)
        self.assertEqual(
            partial_bills.filter(PartialBill.superseded_by.isnot(None)).count(), 3
        )
        self.assertEqual(second_bill.superseded_by, overlapping_bill.oid)
        self.assertEqual(third_bill.superseded_by, overlapping_bill.oid)

        bad_usage_detected = [
            BillingDatum(
                start=date(2019, 2, 25),
                end=date(2019, 3, 25),
                cost=706.5,
                used=0.0,
                peak=45.0,
                items=None,
                attachments=[],
                statement=date(2019, 3, 25),
                utility_code=None,
            )
        ]
        # Bad usage detected so we don't supersede the original bill
        upload.upload_partial_bills(
            self.meter, None, bad_usage_detected, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 5)
        self.assertEqual(partial_bills.filter(PartialBill.used == 0.0).count(), 0)

        bad_usage_new = [
            BillingDatum(
                start=date(2019, 4, 3),
                end=date(2019, 5, 4),
                cost=806.5,
                used=0.0,
                peak=55.0,
                items=None,
                attachments=[],
                statement=date(2019, 5, 4),
                utility_code=None,
            )
        ]
        # Zero usage okay as long as we're not overwriting existing zero usage
        upload.upload_partial_bills(
            self.meter, None, bad_usage_new, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 6)
        self.assertEqual(partial_bills.filter(PartialBill.used == 0.0).count(), 1)

        overlaps_with_closing = [
            BillingDatum(
                start=date(2019, 5, 4),
                end=date(2019, 6, 3),
                cost=100.5,
                used=90.0,
                peak=59.0,
                items=None,
                attachments=[],
                statement=date(2019, 6, 3),
                utility_code=None,
            )
        ]
        # Snaps start date, because initial bill starts on the closing date of an existing bill
        upload.upload_partial_bills(
            self.meter, None, overlaps_with_closing, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 7)
        expected_date = date(2019, 5, 5)
        self.assertEqual(
            partial_bills.filter(PartialBill.initial == expected_date).first().cost,
            100.5,
        )

        # Test that manual bills aren't overwritten
        most_recent_bill = (
            db.session.query(PartialBill)
            .filter(PartialBill.initial == date(2019, 5, 5))
            .first()
        )
        most_recent_bill.manual = True
        db.session.add(most_recent_bill)
        new_bill_updating_manual = [
            BillingDatum(
                start=date(2019, 5, 5),
                end=date(2019, 6, 3),
                cost=500.5,
                used=90.0,
                peak=59.0,
                items=None,
                attachments=[],
                statement=date(2019, 6, 3),
                utility_code=None,
            )
        ]
        upload.upload_partial_bills(
            self.meter, None, new_bill_updating_manual, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 7)
        self.assertIsNone(most_recent_bill.superseded_by)

    def test_snap_start_date(self):
        partial_billing_datum = [
            BillingDatum(
                start=date(2019, 5, 1),
                end=date(2019, 5, 30),
                cost=500.5,
                used=90.0,
                peak=59.0,
                items=None,
                attachments=[],
                statement=date(2019, 6, 3),
                utility_code=None,
                third_party_expected=True,
            )
        ]
        upload.upload_partial_bills(
            self.meter, None, partial_billing_datum, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == self.meter.utility_service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )
        self.assertEqual(partial_bills.count(), 1)
        partial_bill = partial_bills[0]
        self.assertEqual(partial_bill.cost, 500.5)

        one_day_partial_billing_datum = [
            BillingDatum(
                start=date(2019, 5, 30),
                end=date(2019, 5, 31),
                cost=20.5,
                used=9,
                peak=18,
                items=None,
                attachments=[],
                statement=date(2019, 7, 3),
                utility_code=None,
                third_party_expected=True,
            )
        ]

        with self.assertRaises(Exception) as exc:
            upload.upload_partial_bills(
                self.meter,
                None,
                one_day_partial_billing_datum,
                PartialBillProviderType.TND_ONLY,
            )
        self.assertIn(
            "Snapping start date would create bill (2019-05-31 - 2019-05-31)",
            str(exc.exception),
        )

    def test_third_party_expected_in_billing_datum(self):
        """Test third_party_expected in billing datum will persist to the partial bill"""
        partial_billing_datum = [
            BillingDatum(
                start=date(2019, 5, 5),
                end=date(2019, 6, 3),
                cost=500.5,
                used=90.0,
                peak=59.0,
                items=None,
                attachments=[],
                statement=date(2019, 6, 3),
                utility_code=None,
                third_party_expected=True,
            )
        ]
        upload.upload_partial_bills(
            self.meter, None, partial_billing_datum, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == self.meter.utility_service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )
        self.assertEqual(partial_bills.count(), 1)
        partial_bill = partial_bills[0]
        self.assertEqual(partial_bill.cost, 500.5)
        self.assertTrue(partial_bill.third_party_expected)

    def test_superseding_third_party_expected(self):
        """Test incoming partial bill with new value for "third_party_expected" will cause existing
        partial bill to be superseded
        """
        partial_billing_datum = [
            BillingDatum(
                start=date(2019, 5, 5),
                end=date(2019, 6, 3),
                cost=500.5,
                used=90.0,
                peak=59.0,
                items=None,
                attachments=[],
                statement=date(2019, 6, 3),
                utility_code=None,  # Third party expected not sent in billing datum
            )
        ]
        upload.upload_partial_bills(
            self.meter, None, partial_billing_datum, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == self.meter.utility_service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )
        self.assertEqual(partial_bills.count(), 1)
        partial_bill = partial_bills[0]
        self.assertEqual(partial_bill.cost, 500.5)
        self.assertIsNone(
            partial_bill.third_party_expected, "No value set on billing datum."
        )

        new_partial_billing_datum = [
            BillingDatum(
                start=date(2019, 5, 5),
                end=date(2019, 6, 3),
                cost=500.5,
                used=90.0,
                peak=59.0,
                items=None,
                attachments=[],
                statement=date(2019, 6, 3),
                utility_code=None,
                third_party_expected=False,
            )
        ]
        upload.upload_partial_bills(
            self.meter,
            None,
            new_partial_billing_datum,
            PartialBillProviderType.TND_ONLY,
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 2)
        original_partial = partial_bills[0]
        new_partial = partial_bills[1]
        self.assertFalse(new_partial.third_party_expected)
        self.assertEqual(original_partial.superseded_by, new_partial.oid)

    def test_new_pdfs_override(self):
        service = self.meter.utility_service

        # Three new partial bills added for the given service
        upload.upload_partial_bills(
            self.meter, None, billing_data, PartialBillProviderType.TND_ONLY
        )

        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )
        self.assertEqual(partial_bills.count(), 3)

        first_attachments = [
            AttachmentEntry(
                key="123456789",
                kind="bill",
                format="PDF",
                source="",
                statement="2010-10-01",
                utility="pge",
                utility_account_id=service.utility_account_id,
                gen_utility=service.gen_utility,
                gen_utility_account_id=service.gen_utility_account_id,
            )
        ]

        new_partials = [
            BillingDatum(
                start=datetime(2019, 1, 6),
                end=datetime(2019, 2, 3),
                cost=987.76,
                used=4585.0,
                peak=25.0,
                items=None,
                attachments=first_attachments,
                statement=datetime(2019, 2, 3),
                utility_code=None,
            )
        ]

        # Partial coming in with new attachment
        upload.upload_partial_bills(
            self.meter, None, new_partials, PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(partial_bills.count(), 4)

        superseded_partial = (
            db.session.query(PartialBill)
            .filter(PartialBill.superseded_by.isnot(None))
            .first()
        )
        self.assertEqual(superseded_partial.initial, new_partials[0].start)
        self.assertEqual(superseded_partial.closing, new_partials[0].end)
        self.assertEqual(superseded_partial.attachments, [])

        replacement = db.session.query(PartialBill).get(
            superseded_partial.superseded_by
        )
        self.assertEqual(replacement.initial, new_partials[0].start)
        self.assertEqual(replacement.closing, new_partials[0].end)
        self.assertEqual(
            replacement.attachments,
            [{"key": "123456789", "kind": "bill", "format": "PDF"}],
        )

        second_attachments = [
            AttachmentEntry(
                key="1234a56789",
                kind="bill",
                format="PDF",
                source="",
                statement="2010-10-01",
                utility="pge",
                utility_account_id=service.utility_account_id,
                gen_utility=service.gen_utility,
                gen_utility_account_id=service.gen_utility_account_id,
            )
        ]
        new_partials = [
            BillingDatum(
                start=datetime(2019, 1, 6),
                end=datetime(2019, 2, 3),
                cost=987.76,
                used=4585.0,
                peak=25.0,
                items=None,
                attachments=second_attachments,
                statement=datetime(2019, 2, 3),
                utility_code=None,
            )
        ]
        # Partial coming in with updated attachment
        upload.upload_partial_bills(
            self.meter, None, new_partials, PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(partial_bills.count(), 5)

        replacement = (
            db.session.query(PartialBill)
            .filter(
                PartialBill.superseded_by.is_(None),
                PartialBill.initial == datetime(2019, 1, 6),
            )
            .first()
        )

        self.assertEqual(replacement.initial, new_partials[0].start)
        self.assertEqual(replacement.closing, new_partials[0].end)
        self.assertEqual(
            replacement.attachments,
            [{"key": "1234a56789", "kind": "bill", "format": "PDF"}],
        )

    def test_new_line_items_override(self):
        service = self.meter.utility_service

        # Three new partial bills added for the given service
        upload.upload_partial_bills(
            self.meter, None, billing_data, PartialBillProviderType.TND_ONLY
        )

        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )
        self.assertEqual(partial_bills.count(), 3)

        items = [
            BillingDatumItemsEntry(
                description="Part Peak 129,262.000000 kWh @ $0.10640",
                quantity=129262.0,
                rate=0.1064,
                total=13753.48,
                kind="use",
                unit="kwh",
            )
        ]

        new_partials = [
            BillingDatum(
                start=datetime(2019, 1, 6),
                end=datetime(2019, 2, 3),
                cost=987.76,
                used=4585.0,
                peak=25.0,
                items=items,
                attachments=[],
                statement=datetime(2019, 2, 3),
                utility_code=None,
            )
        ]

        # Partial coming in with new line items
        upload.upload_partial_bills(
            self.meter, None, new_partials, PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(partial_bills.count(), 4)

        superseded_partial = (
            db.session.query(PartialBill)
            .filter(PartialBill.superseded_by.isnot(None))
            .first()
        )
        self.assertEqual(superseded_partial.initial, new_partials[0].start)
        self.assertEqual(superseded_partial.closing, new_partials[0].end)
        self.assertEqual(superseded_partial.items, [])

        replacement = db.session.query(PartialBill).get(
            superseded_partial.superseded_by
        )
        self.assertEqual(replacement.initial, new_partials[0].start)
        self.assertEqual(replacement.closing, new_partials[0].end)
        self.assertEqual(
            replacement.items,
            [
                {
                    "description": "Part Peak 129,262.000000 kWh @ $0.10640",
                    "quantity": 129262.0,
                    "rate": 0.1064,
                    "total": 13753.48,
                    "kind": "use",
                    "unit": "kwh",
                }
            ],
        )

        # No data changed, so no new partials created
        upload.upload_partial_bills(
            self.meter, None, new_partials, PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(partial_bills.count(), 4)

    @mock.patch("datafeeds.common.partial_billing.PartialBillProcessor.log_summary")
    def test_partial_bill_line_items(self, _):
        """Test identical but reordered line items don't cause the PB to be superseded"""

        self.assertEqual(PartialBill.sort_items([]), [])
        self.assertEqual(
            PartialBill.sort_items([{"missing_key": "value"}]),
            [{"missing_key": "value"}],
        )
        self.assertEqual(
            PartialBill.sort_items(
                [
                    {"total": 11, "description": "Generation Credit"},
                    {"total": 8, "description": "Generation Credit"},
                ]
            ),
            [
                {"total": 8, "description": "Generation Credit"},
                {"total": 11, "description": "Generation Credit"},
            ],
        )

        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == self.meter.service)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )

        self.assertEqual(partial_bills.count(), 0)

        original = [
            BillingDatum(
                start=datetime(2019, 1, 6),
                end=datetime(2019, 2, 3),
                cost=987.76,
                used=4585.0,
                peak=25.0,
                items=[
                    BillingDatumItemsEntry(
                        description="Generation Credit",
                        quantity=0.0,
                        rate=None,
                        total=-65.61,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Franchise Fee Surcharge",
                        quantity=0.0,
                        rate=None,
                        total=0.39,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Power Cost Incentive Adjustment",
                        quantity=0.0,
                        rate=None,
                        total=15.94,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Customer Charge",
                        quantity=32.0,
                        rate=None,
                        total=26.28,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Part Peak Energy Charge",
                        quantity=221.9045,
                        rate=None,
                        total=54.5,
                        kind="use",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Off Peak Energy Charge",
                        quantity=368.695,
                        rate=None,
                        total=82.85,
                        kind="use",
                        unit="kWh",
                    ),
                ],
                attachments=[],
                statement=datetime(2019, 2, 3),
                utility_code=None,
            )
        ]

        status = upload.upload_partial_bills(
            self.meter, None, original, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(status, Status.SUCCEEDED)
        self.assertEqual(partial_bills.count(), 1)
        original_partial = partial_bills[0]

        status = upload.upload_partial_bills(
            self.meter, None, original, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(status, Status.COMPLETED, "Partial doesn't have new data")
        self.assertEqual(partial_bills.count(), 1)

        reordered_line_items = [
            BillingDatum(
                start=datetime(2019, 1, 6),
                end=datetime(2019, 2, 3),
                cost=987.76,
                used=4585.0,
                peak=25.0,
                items=[
                    BillingDatumItemsEntry(
                        description="Off Peak Energy Charge",
                        quantity=368.695,
                        rate=None,
                        total=82.85,
                        kind="use",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Generation Credit",
                        quantity=0.0,
                        rate=None,
                        total=-65.61,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Franchise Fee Surcharge",
                        quantity=0.0,
                        rate=None,
                        total=0.39,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Power Cost Incentive Adjustment",
                        quantity=0.0,
                        rate=None,
                        total=15.94,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Customer Charge",
                        quantity=32.0,
                        rate=None,
                        total=26.28,
                        kind="other",
                        unit="kWh",
                    ),
                    BillingDatumItemsEntry(
                        description="Part Peak Energy Charge",
                        quantity=221.9045,
                        rate=None,
                        total=54.5,
                        kind="use",
                        unit="kWh",
                    ),
                ],
                attachments=[],
                statement=datetime(2019, 2, 3),
                utility_code=None,
            )
        ]

        status = upload.upload_partial_bills(
            self.meter, None, reordered_line_items, PartialBillProviderType.TND_ONLY
        )
        db.session.flush()
        self.assertEqual(
            status,
            Status.COMPLETED,
            "Reordered identical line items don't cause pb to be superseded",
        )
        self.assertEqual(partial_bills.count(), 1)
        self.assertIsNone(
            db.session.query(PartialBill).get(original_partial.oid).superseded_by
        )

    def test_scrape_utility_code(self):
        service = self.meter.utility_service

        # Three new partial bills added for the given service
        upload.upload_partial_bills(
            self.meter, None, billing_data, PartialBillProviderType.TND_ONLY
        )

        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )
        self.assertEqual(partial_bills.count(), 3)

        # partial scraped with new tariff
        new_partial = BillingDatum(
            start=datetime(2019, 1, 6),
            end=datetime(2019, 2, 3),
            cost=987.76,
            used=4585.0,
            peak=25.0,
            items=None,
            attachments=[],
            statement=datetime(2019, 2, 3),
            utility_code="A6",
        )

        upload.upload_partial_bills(
            self.meter, None, [new_partial], PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(partial_bills.count(), 4)

        superseded_partial = (
            db.session.query(PartialBill)
            .filter(PartialBill.superseded_by.isnot(None))
            .first()
        )
        self.assertEqual(superseded_partial.initial, new_partial.start.date())
        self.assertEqual(superseded_partial.closing, new_partial.end.date())
        self.assertEqual(superseded_partial.utility_code, None)

        replacement = db.session.query(PartialBill).get(
            superseded_partial.superseded_by
        )
        self.assertEqual(
            replacement.utility_code, "A6", "scraped tariffs persist to partial"
        )


class TestPartialBillValidator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]
        self.meter = meters[0]
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
            scrape_bills=False,
            scrape_partial_bills=True,
        )

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_validate_partial_bills_overlapping_error(self):
        overlapping_bills = [
            BillingDatum(
                start=date(2019, 2, 25),
                end=date(2019, 3, 25),
                cost=706.5,
                used=3072.0,
                peak=45.0,
                items=None,
                attachments=[],
                statement=date(2019, 3, 25),
                utility_code=None,
            ),
            BillingDatum(
                start=date(2019, 3, 15),
                end=date(2019, 4, 15),
                cost=886.5,
                used=3032.0,
                peak=91.0,
                items=None,
                attachments=[],
                statement=date(2019, 4, 15),
                utility_code=None,
            ),
        ]

        with self.assertRaises(OverlappedBillingDataDateRangeError):
            validator = PartialBillValidator(overlapping_bills)
            validator.run_prevalidation()

        status = upload.upload_partial_bills(
            self.meter, None, overlapping_bills, PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(status, Status.FAILED)

    def test_validate_partial_bills_no_future_bills_error(self):
        today = datetime.today().date()
        future_bill = [
            BillingDatum(
                start=today,
                end=today + timedelta(days=30),
                cost=706.5,
                used=3072.0,
                peak=45.0,
                items=None,
                attachments=[],
                statement=today + timedelta(days=30),
                utility_code=None,
            )
        ]

        with self.assertRaises(NoFutureBillsError):
            validator = PartialBillValidator(future_bill)
            validator.run_prevalidation()

        status = upload.upload_partial_bills(
            self.meter, None, future_bill, PartialBillProviderType.TND_ONLY
        )
        self.assertEqual(status, Status.FAILED)


bills_list = [
    BillingDatum(
        start=date(2019, 1, 6),
        end=date(2019, 2, 3),
        cost=987.76,
        used=4585.0,
        peak=25.0,
        items=None,
        attachments=[],
        statement=date(2019, 2, 3),
        utility_code=None,
    ),
    BillingDatum(
        start=date(2019, 2, 4),
        end=date(2019, 3, 4),
        cost=882.39,
        used=4787.0,
        peak=54.0,
        items=None,
        attachments=[],
        statement=date(2019, 3, 4),
        utility_code=None,
    ),
    BillingDatum(
        start=date(2019, 3, 5),
        end=date(2019, 4, 2),
        cost=706.5,
        used=3072.0,
        peak=45.0,
        items=None,
        attachments=[],
        statement=date(2019, 4, 2),
        utility_code=None,
    ),
]


class TestBillUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        self.account, meters = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]
        self.meter = meters[0]
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
            scrape_bills=True,
            scrape_partial_bills=False,
        )
        self.account.name = "test account"
        self.meter.utility_service.utility = "utility:sce"
        self.meter_two = meters[1]
        building_oid = 12345
        building = Building(
            oid=building_oid,
            building=building_oid,
            _timezone="America/Los_Angeles",
            account=self.account.oid,
            name="Test building name",
            visible=True,
        )
        self.meter.building = building

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_update_zero_cost(self):
        """Verify replacing zero-cost bills with cost from current bills."""
        service = self.meter.utility_service
        db.session.add(service)
        # no current bills
        bills_list[0] = bills_list[0]._replace(cost=0)
        updated = upload.verify_bills(self.meter.oid, bills_list)
        self.assertEqual(len(bills_list), len(updated))
        for idx, bill in enumerate(bills_list):
            self.assertEqual(bill, updated[idx])

        # with current bill
        billing_datum = bills_list[0]
        bill = Bill(
            service=self.meter.service,
            initial=billing_datum.start,
            closing=billing_datum.end,
            cost=123.45,
            used=billing_datum.used,
            peak=billing_datum.peak,
        )
        db.session.add(bill)
        db.session.flush()
        updated = upload.verify_bills(self.meter.oid, bills_list)
        self.assertEqual(len(bills_list), len(updated))
        self.assertEqual(updated[0].cost, 123.45)
        for idx, bill in enumerate(bills_list[1:]):
            self.assertEqual(bill, updated[idx + 1])

    def test_upload_bills(self):
        service = self.meter.utility_service
        db.session.add(service)

        bill_1 = Bill()
        bill_1.initial = date(2019, 3, 5)
        bill_1.closing = date(2019, 4, 2)
        bill_1.service = service.oid
        db.session.add(bill_1)
        db.session.flush()

        status = upload.upload_bills(
            self.meter.oid, service.service_id, None, None, bills_list
        )
        # No bills newer bills have arrived
        self.assertEqual(status, Status.COMPLETED)

        new_bills_list = [
            BillingDatum(
                start=date(2019, 1, 6),
                end=date(2019, 6, 3),
                cost=987.76,
                used=4585.0,
                peak=25.0,
                items=None,
                attachments=[],
                statement=date(2019, 2, 3),
                utility_code=None,
            )
        ]

        status = upload.upload_bills(
            self.meter.oid, service.service_id, None, None, new_bills_list
        )
        # A more recent bill arrived
        self.assertEqual(status, Status.SUCCEEDED)

        bill_2 = Bill()
        bill_2.service = service.oid
        bill_2.closing = date(2018, 4, 2)
        bill_3 = Bill()
        bill_3.service = service.oid
        most_recent = date(2020, 6, 1)
        bill_3.closing = most_recent
        db.session.add(bill_2)
        db.session.add(bill_3)
        db.session.flush()

        # _latest_closing returns the most recent closing
        self.assertEqual(most_recent, upload._latest_closing(service.service_id))

    def test_bill_upload_attributes(self):
        """Test incoming bill attributes persist as expected to final bill"""
        att = [
            AttachmentEntry(
                key="123456789",
                kind="bill",
                format="PDF",
                source="",
                statement="2010-10-01",
                utility="pge",
                utility_account_id=self.meter.utility_service.utility_account_id,
                gen_utility=self.meter.utility_service.gen_utility,
                gen_utility_account_id=self.meter.utility_service.gen_utility_account_id,
            )
        ]

        items = [
            BillingDatumItemsEntry(
                description="Generation Credit",
                quantity=0.0,
                rate=None,
                total=-65.61,
                kind="other",
                unit="kWh",
            )
        ]
        bill_data = BillingDatum(
            start=datetime(2019, 1, 6),
            end=datetime(2019, 2, 3),
            cost=987.76,
            used=4585.023423423,
            peak=25.0,
            items=items,
            attachments=att,
            statement=datetime(2019, 2, 3),
            utility_code="Utility-Code",
        )

        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, [bill_data]
        )
        self.assertEqual(len(ret), 1)
        bill = ret[0]
        self.assertEqual(bill.service, self.meter.service)
        self.assertEqual(bill.initial, datetime(2019, 1, 6).date())
        self.assertEqual(bill.closing, datetime(2019, 2, 3).date())
        self.assertEqual(bill.utility_code, "Utility-Code")
        self.assertEqual(bill.cost, 987.76)
        self.assertEqual(bill.used, 4585.0234, "Used is rounded.")
        self.assertEqual(bill.peak, 25)
        self.assertEqual(bill.source, "datafeeds")
        self.assertTrue(bill.visible)
        self.assertIsNotNone(bill.manual)
        self.assertFalse(bill.manual)
        self.assertEqual(
            bill.items,
            [
                {
                    "description": "Generation Credit",
                    "quantity": 0.0,
                    "rate": None,
                    "total": -65.61,
                    "kind": "other",
                    "unit": "kWh",
                }
            ],
        )
        self.assertEqual(
            bill.attachments, [{"key": "123456789", "kind": "bill", "format": "PDF"}]
        )
        self.assertFalse(bill.manual)
        self.assertTrue(bill.visible)
        self.assertIsNotNone(bill.oid)
        self.assertIsNone(bill.tnd_cost)
        self.assertIsNone(bill.gen_cost)
        self.assertIsNotNone(bill.created)
        self.assertIsNotNone(bill.modified)

        bill_audit_record = (
            db.session.query(BillAudit).filter(BillAudit.bill == bill.oid).first()
        )
        self.assertIsNone(
            bill_audit_record, "No bill audit record exists; meter not enrolled."
        )

    def test_snap_start_date(self):
        # Create first bill
        one_bill_list = [bills_list[0]]
        _upload_bills_to_services(self.meter.utility_service.service_id, one_bill_list)

        # Incoming bill overlaps last bill by one day.
        new_bd = [
            BillingDatum(
                start=datetime(2019, 2, 3),
                end=datetime(2019, 3, 4),
                cost=882.39,
                used=4787.0,
                peak=54.0,
                items=None,
                attachments=[],
                statement=datetime(2019, 3, 4),
                utility_code=None,
            )
        ]

        _upload_bills_to_services(self.meter.utility_service.service_id, new_bd)

        bills = (
            db.session.query(Bill)
            .filter(Bill.service == self.meter.service)
            .order_by(Bill.initial)
        )
        self.assertEqual(bills.count(), 2)

        self.assertEqual(
            bills[0].initial,
            datetime(2019, 1, 6).date(),
            "existing bill stayed the same",
        )
        self.assertEqual(
            bills[0].closing,
            datetime(2019, 2, 3).date(),
            "existing bill stayed the same",
        )

        self.assertEqual(
            bills[1].initial,
            datetime(2019, 2, 4).date(),
            "Incoming snapped to new start date.",
        )
        self.assertEqual(bills[1].closing, datetime(2019, 3, 4).date())

    def test_write_bill_to_multiple_services(self):
        """Test incoming bill data is written to all services with a matching service id"""
        self.meter_two.utility_service.service_id = (
            self.meter.utility_service.service_id
        )
        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, [bills_list[0]]
        )
        self.assertEqual(len(ret), 2, "Same bill added to two services.")
        self.assertNotEqual(ret[0].oid, ret[1].oid)
        self.assertNotEqual(ret[0].service, ret[1].service)
        self.assertEqual(
            {ret[0].service, ret[1].service},
            {self.meter.service, self.meter_two.service},
        )

        bill_one_dict = ret[0].__dict__
        bill_two_dict = ret[1].__dict__

        for record in [bill_one_dict, bill_two_dict]:
            record.pop("_sa_instance_state")
            record.pop("service")
            record.pop("oid")

        self.assertEqual(bill_one_dict, bill_two_dict, "both bills have same values.")

    def test_bill_upload_paths(self):
        """Sanity check for new/update/duplicate/skip/overlap bill creation paths."""
        # New bill, created.
        one_bill_list = [bills_list[0]]
        one_bill_list[0] = one_bill_list[0]._replace(used=4585.0234)

        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, one_bill_list
        )
        self.assertEqual(len(ret), 1)

        one_bill_list[0] = one_bill_list[0]._replace(used=4585.02343242342)
        bills = db.session.query(Bill).filter(Bill.service == self.meter.service)
        self.assertEqual(bills.count(), 1)
        self.assertTrue(bills[0].visible)
        modified = bills[0].modified
        original_oid = bills[0].oid
        db.session.flush()
        # Duplicate bill, skipped.
        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, one_bill_list
        )
        self.assertEqual(len(ret), 0)
        self.assertEqual(bills.count(), 1)
        self.assertEqual(modified, bills[0].modified)
        self.assertEqual(bills[0].oid, original_oid)

        # Updating bill
        one_bill_list[0] = one_bill_list[0]._replace(cost=333.12)
        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, one_bill_list
        )
        self.assertEqual(len(ret), 1)
        self.assertEqual(bills.count(), 1)
        self.assertEqual(bills[0].cost, 333.12)
        self.assertNotEqual(modified, bills[0].modified)
        self.assertEqual(bills[0].oid, original_oid)
        db.session.flush()

        # Overlaps bill - delete and recreate:
        one_bill_list[0] = one_bill_list[0]._replace(start=datetime(2019, 1, 5))
        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, one_bill_list
        )
        self.assertEqual(len(ret), 1)
        self.assertEqual(bills.count(), 1)
        self.assertEqual(bills[0].initial, datetime(2019, 1, 5).date())
        new_oid = bills[0].oid
        self.assertNotEqual(bills[0].oid, original_oid)

        # Existing is manual - skip.
        bills[0].manual = True
        db.session.add(bills[0])
        db.session.flush()
        new_modified = bills[0].modified
        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, one_bill_list
        )
        self.assertEqual(len(ret), 0)
        self.assertEqual(bills.count(), 1)
        self.assertEqual(bills[0].modified, new_modified)
        self.assertEqual(new_oid, bills[0].oid)

        bills[0].manual = False
        db.session.add(bills[0])
        db.session.flush()
        new_modified = bills[0].modified

        # Existing is stitched, not scraped. - skip
        pb = PartialBill(
            provider_type=TND_ONLY,
            cost=333.12,
            initial=datetime(2019, 1, 5),
            closing=datetime(2019, 2, 3),
            service=self.meter.utility_service.oid,
        )
        db.session.add(pb)
        bills[0].partial_bills.append(pb)
        db.session.flush()

        one_bill_list[0] = one_bill_list[0]._replace(cost=233.12)
        ret, _ = _upload_bills_to_services(
            self.meter.utility_service.service_id, one_bill_list
        )
        self.assertEqual(len(ret), 0)
        self.assertEqual(bills.count(), 1)
        self.assertEqual(bills[0].modified, new_modified)
        self.assertEqual(new_oid, bills[0].oid)

    def test_create_bill_audits_new_bills(self):
        """Test bill audit creation workflow - new bills"""
        pe = ProductEnrollment(
            meter=self.meter.oid, product="opsbillaudit", status="active"
        )
        db.session.add(pe)
        db.session.flush()

        one_bill = [bills_list[0]]

        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )
        bills = db.session.query(Bill).filter(Bill.service == self.meter.service).all()

        self.assertEqual(len(bills), 1)
        bill = bills[0]
        self.assertFalse(bill.visible)

        bill_audit_record = (
            db.session.query(BillAudit).filter(BillAudit.bill == bill.oid).first()
        )
        self.assertEqual(bill_audit_record.workflow_state, WorkflowState.pending)
        self.assertEqual(bill_audit_record.audit_verdict, None)
        self.assertEqual(bill_audit_record.audit_issues, None)
        self.assertEqual(bill_audit_record.audit_errors, None)
        self.assertEqual(bill_audit_record.latest_audit, None)
        self.assertEqual(bill_audit_record.bill, bill.oid)
        self.assertEqual(bill_audit_record.bill_service, bill.service)
        # Test attributes cached on bill audit record
        self.assertEqual(bill_audit_record.bill_initial, bill.initial)
        self.assertEqual(bill_audit_record.account_hex, self.account.hex_id)
        self.assertEqual(bill_audit_record.account_name, "test account")
        self.assertEqual(bill_audit_record.building_name, "Test building name")
        self.assertEqual(bill_audit_record.utility, "utility:sce")
        self.assertEqual(len(bill_audit_record.events), 1)
        self.assertEqual(
            bill_audit_record.events[0].description, "Initialized bill audit."
        )

        # Mock bill audit run through analytica - bill is now visible, and bill audit is in review.
        bill.visible = True
        bill_audit_record.workflow_state = WorkflowState.review
        db.session.add(bill)
        db.session.add(bill_audit_record)

        one_bill[0] = one_bill[0]._replace(cost=333.12)

        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )

        bills = db.session.query(Bill).filter(Bill.service == self.meter.service).all()

        self.assertEqual(len(bills), 1)
        bill = bills[0]
        self.assertTrue(bill.visible, "Bill not marked as hidden")
        bill_audit_records = db.session.query(BillAudit).filter(
            BillAudit.bill == bill.oid
        )
        self.assertEqual(bill_audit_records.count(), 1)
        self.assertEqual(
            bill_audit_record,
            bill_audit_records[0],
            "No change to existing bill audit record.",
        )

    def test_bill_audit_update_bill(self):
        """Test bill audit creation workflow - updating existing bills"""
        one_bill = [bills_list[0]]

        # Create bill
        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )
        bills = db.session.query(Bill).filter(Bill.service == self.meter.service).all()

        self.assertEqual(len(bills), 1)
        bill = bills[0]
        bill_oid = bill.oid
        bill_modified = bill.modified
        self.assertTrue(bill.visible)
        bill_audit_records = db.session.query(BillAudit).filter(
            BillAudit.bill == bill.oid
        )
        self.assertEqual(
            bill_audit_records.count(), 0, "Meter not enrolled in bill audit."
        )

        # Enroll in bill audit
        pe = ProductEnrollment(
            meter=self.meter.oid, product="opsbillaudit", status="active"
        )
        db.session.add(pe)

        # Update bill
        one_bill[0] = one_bill[0]._replace(cost=333.12)
        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )
        bills = db.session.query(Bill).filter(Bill.service == self.meter.service).all()
        self.assertEqual(len(bills), 1)
        bill = bills[0]
        self.assertEqual(bill.oid, bill_oid)
        self.assertNotEqual(bill.modified, bill_modified)
        self.assertTrue(
            bill.visible, "Even though bill was updated, we left the bill visible."
        )
        self.assertEqual(bill_audit_records.count(), 1)
        self.assertEqual(bill_audit_records[0].workflow_state, WorkflowState.pending)

    def test_bill_audit_bill_overlap(self):
        """Test bill audit creation workflow - bill overlaps existing bills"""

        # Enroll meter in bill audit, create bill.
        pe = ProductEnrollment(
            meter=self.meter.oid, product="opsbillaudit", status="active"
        )
        db.session.add(pe)
        db.session.flush()

        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            bills_list,
        )
        bills = (
            db.session.query(Bill)
            .filter(Bill.service == self.meter.service)
            .order_by(Bill.initial)
        )
        self.assertEqual(bills.count(), 3)
        bill = bills[0]
        bill_modified = bill.modified

        bill_oid = bill.oid
        bill_audit_records = db.session.query(BillAudit).filter(
            BillAudit.bill == bill.oid
        )
        self.assertEqual(bill_audit_records.count(), 1, "Meter enrolled in bill audit.")

        # Mock bill audit being completed
        bill.visible = True
        bill_audit = bill_audit_records[0]
        bill_audit.workflow_state = WorkflowState.done
        db.session.add(bill)
        db.session.add(bill_audit)

        # Update bill with different date, so it will overlap two bills
        bill_updates = [bills_list[0]]
        bill_updates[0] = bill_updates[0]._replace(end=datetime(2019, 2, 4))
        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            bill_updates,
        )
        db.session.flush()

        self.assertEqual(bills.count(), 2, "Incoming bill replaced two bills")
        bill = bills[0]
        self.assertNotEqual(
            bill.oid,
            bill_oid,
            "Bill overlapped existing, so existing bill was deleted and new created.",
        )
        self.assertEqual(bill.initial, datetime(2019, 1, 6).date())
        self.assertEqual(bill.closing, datetime(2019, 2, 4).date())
        self.assertNotEqual(bill.modified, bill_modified)

        self.assertEqual(
            bill_audit_records.count(),
            0,
            "No bill audits attached to existing deleted bill.",
        )
        bill_audit_records = db.session.query(BillAudit).filter(
            BillAudit.bill == bill.oid
        )
        self.assertEqual(
            bill_audit_records.count(),
            1,
            "No bill audits attached to existing deleted bill.",
        )
        self.assertEqual(
            bill_audit_records[0].workflow_state,
            WorkflowState.pending,
            "New pending bill audit created.",
        )
        self.assertTrue(
            bill.visible,
            "bill kept visible, because we previously had overlapping bill.",
        )

    def test_bill_audit_bill_skipped(self):
        """
        Test bill audits not created if incoming bill skipped.
        """
        one_bill = [bills_list[0]]

        # Create bill
        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )
        bills = db.session.query(Bill).filter(Bill.service == self.meter.service).all()

        self.assertEqual(len(bills), 1)
        bill = bills[0]
        self.assertTrue(bill.visible)
        bill_audit_records = db.session.query(BillAudit).filter(
            BillAudit.bill == bill.oid
        )
        self.assertEqual(
            bill_audit_records.count(), 0, "Meter not enrolled in bill audit."
        )

        # Enroll in bill audit
        pe = ProductEnrollment(
            meter=self.meter.oid, product="opsbillaudit", status="active"
        )
        db.session.add(pe)

        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )

        self.assertEqual(
            bill_audit_records.count(),
            0,
            "No bill audits created; incoming duplicate bill was skipped.",
        )

    def test_bad_usage_override(self):
        one_bill = [bills_list[0]]

        # Create bill
        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )
        bills = db.session.query(Bill).filter(Bill.service == self.meter.service)

        self.assertEqual(bills.count(), 1)
        bill = bills[0]
        original_used = bill.used
        original_modified = bill.modified

        one_bill[0] = one_bill[0]._replace(used=0)
        upload.upload_bills(
            self.meter.oid,
            self.meter.utility_service.service_id,
            None,
            None,
            one_bill,
        )

        self.assertEqual(bills.count(), 1)
        self.assertEqual(bills[0].modified, original_modified)
        self.assertEqual(
            bills[0].used, original_used, "0 usage non-zero cost bill was discarded."
        )

    def test_upload_incoming_data_overlaps(self):
        """Test that incoming billing data must not overlap."""
        one_bill = [bills_list[0], bills_list[0]]

        with self.assertRaises(InvalidBillError):
            upload.upload_bills(
                self.meter.oid,
                self.meter.utility_service.service_id,
                None,
                None,
                one_bill,
            )

    def test_incoming_future_bills(self):
        """Test that incoming bills can't end in the future"""
        today = datetime.today().date()
        incoming = BillingDatum(
            start=today,
            end=today + timedelta(days=30),
            cost=987.76,
            used=4585.0,
            peak=25.0,
            items=None,
            attachments=[],
            statement=datetime(2019, 2, 3),
            utility_code=None,
        )

        with self.assertRaises(NoFutureBillsError):
            upload.upload_bills(
                self.meter.oid,
                self.meter.utility_service.service_id,
                None,
                None,
                [incoming],
            )


class TestReadingsUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]
        self.meter = meters[0]
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
        )

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    @mock.patch("datafeeds.config.enabled")
    def test_upload_readings(self, mocked_config):
        readings_data = {"2017-04-02": [59.1, 30.2, None]}
        transforms = []

        # 96 readings per day are expected
        with self.assertRaises(InvalidMeterDataException):
            upload.upload_readings(
                transforms, self.meter_ids[0], None, None, readings_data
            )
        readings_data_list = []
        for i in range(96):
            readings_data_list.append(random.randrange(10000) / 100)
        readings_data = {"2017-04-02": readings_data_list}
        status = upload.upload_readings(
            transforms, self.meter_ids[0], None, None, readings_data
        )
        self.assertEqual(status, Status.SUCCEEDED)

        status = upload.upload_readings(
            transforms, self.meter_ids[0], None, None, readings_data
        )
        # There is no new data
        self.assertEqual(status, Status.COMPLETED)

        readings_data_list = []
        for i in range(96):
            readings_data_list.append(random.randrange(10000) / 100)
        readings_data = {"2017-04-02": readings_data_list}
        status = upload.upload_readings(
            transforms, self.meter_ids[0], None, None, readings_data
        )
        # Now there is new data
        self.assertEqual(status, Status.SUCCEEDED)

        # Different day means new data
        readings_data = {"2017-04-03": readings_data_list}
        status = upload.upload_readings(
            transforms, self.meter_ids[0], None, None, readings_data
        )
        self.assertEqual(status, Status.SUCCEEDED)

        # New data can be from past days
        readings_data = {"2017-04-01": readings_data_list}
        status = upload.upload_readings(
            transforms, self.meter_ids[0], None, None, readings_data
        )
        self.assertEqual(status, Status.SUCCEEDED)


class TestPdfAttachment(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]
        self.meter = meters[0]
        self.meter_two = meters[1]
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
        )
        self.service = self.meter.utility_service

        self.bill_1 = Bill()
        self.bill_1.initial = date(2019, 4, 2)
        self.bill_1.closing = date(2019, 5, 2)
        self.bill_1.service = self.service.oid
        db.session.add(self.bill_1)
        db.session.flush()

        # Creating bill on another meter on same account, with same utility account id.
        self.meter_two.utility_service.utility_account_id = (
            self.service.utility_account_id
        )
        db.session.add(self.meter_two.utility_service)

        self.bill_2 = Bill()
        self.bill_2.initial = date(2019, 4, 2)
        self.bill_2.closing = date(2019, 5, 2)
        self.bill_2.service = self.meter_two.utility_service.oid
        db.session.add(self.bill_2)
        db.session.flush()

        self.key = "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf"
        self.bill_pdf = BillPdf(
            self.service.utility_account_id,
            self.service.utility_account_id,
            date(2019, 4, 2),
            date(2019, 5, 2),
            date(2019, 5, 2),
            self.key,
        )

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_attach_bill_pdfs(self):
        """Meter only flag = False attaches bill pdfs to relevant bills across multiple services sharing
        the same account id"""
        pdfs = []
        # Empty list has no new data
        status = upload.attach_bill_pdfs(self.meter_ids[0], None, False, pdfs)
        self.assertEqual(status, Status.COMPLETED)

        service = self.meter.utility_service
        db.session.add(service)

        pdfs.append(self.bill_pdf)
        status = upload.attach_bill_pdfs(self.meter_ids[0], self.key, False, pdfs)
        # Now there is new data
        self.assertEqual(status, Status.SUCCEEDED)
        self.assertEqual(
            self.bill_1.attachments,
            [
                {
                    "kind": "bill",
                    "key": "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf",
                    "format": "PDF",
                }
            ],
            "Attachment added to matching bill with same utility account id",
        )
        self.assertEqual(
            self.bill_2.attachments,
            [
                {
                    "kind": "bill",
                    "key": "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf",
                    "format": "PDF",
                }
            ],
            "Attachment also added to bill on meter two, which has the same utility account id.",
        )

    def test_attach_bill_pdfs_to_given_meter(self):
        """Meter only flag to attach_bill_pdfs only attaches PDF's to bills on given meter.  Matches
        up bill initial instead of statement date."""
        pdfs = [self.bill_pdf]
        status = upload.attach_bill_pdfs(
            self.meter_ids[0], self.key, meter_only=True, pdfs=pdfs
        )
        self.assertEqual(status, Status.SUCCEEDED)
        self.assertEqual(
            self.bill_1.attachments,
            [
                {
                    "kind": "bill",
                    "key": "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf",
                    "format": "PDF",
                }
            ],
            "Attachment added to matching bill with same utility account id",
        )

        self.assertIsNone(
            self.bill_2.attachments,
            "Bill not attached to other service with matching account id.",
        )

    def test_add_multiple_pdfs_to_given_bill(self):
        """Multiple statements found, one with newer information, which is added first:"""
        second_bill_pdf = BillPdf(
            self.service.utility_account_id,
            self.service.utility_account_id,
            start=date(2019, 4, 2),
            end=date(2019, 5, 2),
            statement=date(2019, 9, 2),
            s3_key="1ea5a6c9-0a3c-d0fc-a0ba-NEWEST.pdf",
        )

        pdfs = [self.bill_pdf, second_bill_pdf]
        status = upload.attach_bill_pdfs(
            self.meter_ids[0], self.key, meter_only=True, pdfs=pdfs
        )
        self.assertEqual(status, Status.SUCCEEDED)
        self.assertEqual(
            self.bill_1.attachments,
            [
                {
                    "kind": "bill",
                    "key": "1ea5a6c9-0a3c-d0fc-a0ba-NEWEST.pdf",
                    "format": "PDF",
                },
                {
                    "kind": "bill",
                    "key": "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf",
                    "format": "PDF",
                },
            ],
            "Both statements attached.",
        )

        pdfs = [self.bill_pdf, second_bill_pdf]
        status = upload.attach_bill_pdfs(
            self.meter_ids[0], self.key, meter_only=True, pdfs=pdfs
        )
        self.assertEqual(status, Status.COMPLETED, "All PDF's already attached.")
