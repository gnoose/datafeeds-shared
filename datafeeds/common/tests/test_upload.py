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

from datafeeds.scrapers.sce_react.energymanager_billing import (
    SceReactEnergyManagerBillingConfiguration,
)

from datafeeds.models.bill import Bill, PartialBill

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
            self.meter, self.configuration, None, billing_data
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
        self.assertEqual(
            original_bill.utility_code, None, "utility code is None if None scraped",
        )

        # No new partial bills have arrived, so no changes made
        upload.upload_partial_bills(self.meter, self.configuration, None, billing_data)
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
            self.meter, self.configuration, None, altered_cost
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
            self.meter, self.configuration, None, overlapping_bill_datum
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
            self.meter, self.configuration, None, bad_usage_detected
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
        upload.upload_partial_bills(self.meter, self.configuration, None, bad_usage_new)
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
            self.meter, self.configuration, None, overlaps_with_closing
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
            self.meter, self.configuration, None, new_bill_updating_manual
        )
        db.session.flush()
        self.assertEqual(partial_bills.count(), 7)
        self.assertIsNone(most_recent_bill.superseded_by)

    def test_new_pdfs_override(self):
        service = self.meter.utility_service

        # Three new partial bills added for the given service
        upload.upload_partial_bills(self.meter, self.configuration, None, billing_data)

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
        upload.upload_partial_bills(self.meter, self.configuration, None, new_partials)
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
        upload.upload_partial_bills(self.meter, self.configuration, None, new_partials)
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
        upload.upload_partial_bills(self.meter, self.configuration, None, billing_data)

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
        upload.upload_partial_bills(self.meter, self.configuration, None, new_partials)
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
        upload.upload_partial_bills(self.meter, self.configuration, None, new_partials)
        self.assertEqual(partial_bills.count(), 4)

    def test_scrape_utility_code(self):
        service = self.meter.utility_service

        # Three new partial bills added for the given service
        upload.upload_partial_bills(self.meter, self.configuration, None, billing_data)

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

        upload.upload_partial_bills(self.meter, self.configuration, None, [new_partial])
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
            self.meter, self.configuration, None, overlapping_bills
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
            self.meter, self.configuration, None, future_bill
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
        (account, meters) = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]
        self.meter = meters[0]
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
            scrape_bills=True,
            scrape_partial_bills=False,
        )

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_upload_bills(self):
        service = self.meter.utility_service
        db.session.add(service)

        bill_1 = Bill()
        bill_1.closing = date(2019, 4, 2)
        bill_1.service = service.oid
        db.session.add(bill_1)
        db.session.flush()

        print("service_id is %s" % service.service_id)
        status = upload.upload_bills(self.meter, service.service_id, None, bills_list)
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
            self.meter, service.service_id, None, new_bills_list
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
        self.configuration = SceReactEnergyManagerBillingConfiguration(
            utility=self.meter.utility_service.utility,
            utility_account_id=self.meter.utility_service.utility_account_id,
            service_id=self.meter.service_id,
        )
        self.service = self.meter.utility_service

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_attach_bill_pdfs(self):
        pdfs = []
        # Empty list has no new data
        status = upload.attach_bill_pdfs(self.meter_ids[0], None, pdfs)
        self.assertEqual(status, Status.COMPLETED)

        service = self.meter.utility_service
        db.session.add(service)

        bill_1 = Bill()
        bill_1.initial = date(2019, 4, 2)
        bill_1.closing = date(2019, 5, 2)
        bill_1.service = service.oid
        db.session.add(bill_1)
        db.session.flush()
        key = "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf"
        service = self.meter.utility_service
        print(service.utility_account_id)
        bill_pdf = BillPdf(
            service.utility_account_id,
            service.utility_account_id,
            date(2019, 4, 2),
            date(2019, 5, 2),
            date(2019, 5, 2),
            key,
        )
        pdfs.append(bill_pdf)
        status = upload.attach_bill_pdfs(self.meter_ids[0], key, pdfs)
        # Now there is new data
        self.assertEqual(status, Status.SUCCEEDED)
