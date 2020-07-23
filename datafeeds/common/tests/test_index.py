from datetime import datetime, date, timedelta
import unittest
from typing import List
from unittest import mock

from datafeeds import db
from datafeeds.common import test_utils, index
from datafeeds.common.typing import BillingData, BillingDatum, BillPdf
from datafeeds.models.meter import MeterReading
from datafeeds.models.user import (
    SnapmeterUser,
    SnapmeterAccountUser,
    SnapmeterUserSubscription,
)


class IndexTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        self.account = account
        self.meters = meters
        # create internal user
        self.user = SnapmeterUser(
            email="test%s@test.com" % datetime.now().strftime("%s%f"),
            groups=["groups:superusers"],
        )
        db.session.add(self.user)
        db.session.flush()

    def add_internal_user(self):
        db.session.add(
            SnapmeterAccountUser(user=self.user.oid, account=self.account.oid)
        )
        db.session.add(
            SnapmeterUserSubscription(
                user=self.user.oid, subscription="snapmeter", meter=self.meters[0].oid
            )
        )
        db.session.flush()

    def set_external_user(self):
        self.user.groups = ["groups:users"]
        db.session.add(self.user)
        db.session.flush()

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_update_billing_range(self, index_etl_run):
        meter = self.meters[0]
        meter = self.meters[0]
        # no readings, no users
        bills: BillingData = []
        task_id = "abc123"
        index.update_billing_range(task_id, meter.oid, bills)
        self.assertEqual(
            {"dataType": "bill", "emailSubscribers": 0, "accountUsers": 0},
            index_etl_run.call_args[0][1],
        )
        # with users, but user is internal
        self.add_internal_user()
        index_etl_run.reset_mock()
        index.update_billing_range(task_id, meter.oid, bills)
        self.assertEqual(
            {"dataType": "bill", "emailSubscribers": 0, "accountUsers": 0},
            index_etl_run.call_args[0][1],
        )
        # with external user
        self.set_external_user()
        index_etl_run.reset_mock()
        index.update_billing_range(task_id, meter.oid, bills)
        self.assertEqual(
            {"dataType": "bill", "emailSubscribers": 1, "accountUsers": 1},
            index_etl_run.call_args[0][1],
        )
        # with users and bills
        end = date.today() - timedelta(days=7)
        for _ in range(3):
            start = end - timedelta(days=30)
            bills.append(
                BillingDatum(
                    start=start,
                    end=end,
                    statement=end,
                    cost=100.0,
                    used=100.0,
                    peak=10.0,
                    items=[],
                    attachments=[],
                    utility_code="ABC",
                )
            )
            end = start - timedelta(days=1)
        index_etl_run.reset_mock()
        index.update_billing_range(task_id, meter.oid, bills)
        expected = {
            "dataType": "bill",
            "emailSubscribers": 1,
            "accountUsers": 1,
            "billingFrom": min(b.start for b in bills),
            "billingTo": max(b.end for b in bills),
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_update_bill_pdf_range(self, index_etl_run):
        meter = self.meters[0]
        # no pdfs, no users
        pdfs: List[BillPdf] = []
        task_id = "abc123"
        index.update_bill_pdf_range(task_id, meter.oid, pdfs)
        self.assertEqual(
            {"dataType": "bill", "emailSubscribers": 0, "accountUsers": 0},
            index_etl_run.call_args[0][1],
        )
        # with users, but user is internal
        self.add_internal_user()
        index_etl_run.reset_mock()
        index.update_bill_pdf_range(task_id, meter.oid, pdfs)
        self.assertEqual(
            {"dataType": "bill", "emailSubscribers": 0, "accountUsers": 0},
            index_etl_run.call_args[0][1],
        )
        # with external user
        self.set_external_user()
        index_etl_run.reset_mock()
        index.update_bill_pdf_range(task_id, meter.oid, pdfs)
        self.assertEqual(
            {"dataType": "bill", "emailSubscribers": 1, "accountUsers": 1},
            index_etl_run.call_args[0][1],
        )
        # with users and pdfs
        end = date.today() - timedelta(days=7)
        for _ in range(3):
            start = end - timedelta(days=30)
            pdfs.append(
                BillPdf(
                    utility_account_id="123",
                    gen_utility_account_id=None,
                    start=start,
                    end=end,
                    s3_key="abc123.pdf",
                )
            )
            end = start - timedelta(days=1)
        index_etl_run.reset_mock()
        index.update_bill_pdf_range(task_id, meter.oid, pdfs)
        expected = {
            "dataType": "bill",
            "emailSubscribers": 1,
            "accountUsers": 1,
            "billingFrom": min(b.start for b in pdfs),
            "billingTo": max(b.end for b in pdfs),
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_set_interval_fields(self, index_etl_run):
        meter = self.meters[0]
        # no readings, no users
        task_id = "abc123"
        index.set_interval_fields(task_id, meter.oid, [])
        self.assertEqual(
            {
                "dataType": "interval",
                "updatedDays": 0,
                "emailSubscribers": 0,
                "accountUsers": 0,
            },
            index_etl_run.call_args[0][1],
        )
        # with users, but user is internal
        self.add_internal_user()
        index_etl_run.reset_mock()
        index.set_interval_fields(task_id, meter.oid, [])
        self.assertEqual(
            {
                "dataType": "interval",
                "updatedDays": 0,
                "emailSubscribers": 0,
                "accountUsers": 0,
            },
            index_etl_run.call_args[0][1],
        )
        # with external user
        self.set_external_user()
        index_etl_run.reset_mock()
        index.set_interval_fields(task_id, meter.oid, [])
        self.assertEqual(
            {
                "dataType": "interval",
                "updatedDays": 0,
                "emailSubscribers": 1,
                "accountUsers": 1,
            },
            index_etl_run.call_args[0][1],
        )
        # with users and readings
        today = date.today()
        dates = [
            today - timedelta(days=7),
            today - timedelta(days=6),
            today - timedelta(days=5),
        ]
        readings = []
        for dt in dates:
            readings.append(
                MeterReading(meter=meter.oid, occurred=dt, readings=[1.0] * 96)
            )
        index_etl_run.reset_mock()
        index.set_interval_fields(task_id, meter.oid, readings)
        expected = {
            "dataType": "interval",
            "updatedDays": 3,
            "emailSubscribers": 1,
            "accountUsers": 1,
            "intervalFrom": dates[0],
            "intervalTo": dates[-1],
            "age": 5,
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])
