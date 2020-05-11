import unittest
from unittest import mock

from datetime import date, datetime, timedelta

from datafeeds import db
from datafeeds.common import upload, test_utils
from datafeeds.common.partial_billing import PartialBillValidator
from datafeeds.common.typing import (
    BillingDatum,
    OverlappedBillingDataDateRangeError,
    NoFutureBillsError,
)
from datafeeds.scrapers.sce_react.energymanager_billing import (
    SceReactEnergyManagerBillingConfiguration,
)

from datafeeds.models.bill import PartialBill

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

        partial_bills = (
            db.session.query(PartialBill)
            .filter(PartialBill.service == service.oid)
            .order_by(PartialBill.initial)
            .order_by(PartialBill.created)
        )

        self.assertEqual(partial_bills.count(), 0)

        # Three new partial bills added for the given service
        upload.upload_partial_bills(self.meter, self.configuration, billing_data)
        db.session.flush()
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

        # No new partial bills have arrived, so no changes made
        upload.upload_partial_bills(self.meter, self.configuration, billing_data)
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
            )
        ]
        # Existing bill superseded because new partial bill with new cost uploaded
        upload.upload_partial_bills(self.meter, self.configuration, altered_cost)
        db.session.flush()
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
            )
        ]
        # New bill overlaps dates with two existing partial bills
        upload.upload_partial_bills(
            self.meter, self.configuration, overlapping_bill_datum
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
            )
        ]
        # Bad usage detected so we don't supersede the original bill
        upload.upload_partial_bills(self.meter, self.configuration, bad_usage_detected)
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
            )
        ]
        # Zero usage okay as long as we're not overwriting existing zero usage
        upload.upload_partial_bills(self.meter, self.configuration, bad_usage_new)
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
            )
        ]
        # Snaps start date, because initial bill starts on the closing date of an existing bill
        upload.upload_partial_bills(
            self.meter, self.configuration, overlaps_with_closing
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
            )
        ]
        upload.upload_partial_bills(
            self.meter, self.configuration, new_bill_updating_manual
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 7)
        self.assertIsNone(most_recent_bill.superseded_by)


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
            ),
        ]

        with self.assertRaises(OverlappedBillingDataDateRangeError):
            validator = PartialBillValidator(overlapping_bills)
            validator.run_prevalidation()

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
            )
        ]

        with self.assertRaises(NoFutureBillsError):
            validator = PartialBillValidator(future_bill)
            validator.run_prevalidation()