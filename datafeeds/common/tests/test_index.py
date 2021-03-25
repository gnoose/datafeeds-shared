from datetime import datetime, date, timedelta
import unittest
from typing import List
from unittest import mock

from datafeeds import db
from datafeeds.common import test_utils, index
from datafeeds.common.index import index_logs
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

    def test_run_meta(self):
        meter = self.meters[0]
        expected = {
            "meter": str(meter.oid),
            "meter_name": meter.name,
            "service_id": meter.utility_service.service_id,
            "account": self.account.hex_id,
            "account_name": self.account.name,
            "building": str(meter.building),
            "emailSubscribers": 0,
            "accountUsers": 0,
        }
        # no users
        self.assertEqual(expected, index.run_meta(meter.oid))
        # with users, but user is internal
        self.add_internal_user()
        self.assertEqual(expected, index.run_meta(meter.oid))
        # with external user
        self.set_external_user()
        expected["emailSubscribers"] = 1
        expected["accountUsers"] = 1
        self.assertEqual(expected, index.run_meta(meter.oid))

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_update_billing_range(self, index_etl_run):
        # no bills
        bills: BillingData = []
        task_id = "abc123"
        index.update_billing_range(task_id, bills)
        self.assertEqual(0, index_etl_run.call_count)
        # with bills
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
        index.update_billing_range(task_id, bills)
        expected = {
            "billingFrom": min(b.start for b in bills),
            "billingTo": max(b.end for b in bills),
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_update_bill_pdf_range(self, index_etl_run):
        meter = self.meters[0]
        # no pdfs
        pdfs: List[BillPdf] = []
        task_id = "abc123"
        index.update_bill_pdf_range(task_id, meter.oid, pdfs)
        self.assertEqual(0, index_etl_run.call_count)
        # with pdfs
        end = date.today() - timedelta(days=7)
        for _ in range(3):
            start = end - timedelta(days=30)
            pdfs.append(
                BillPdf(
                    utility_account_id="123",
                    gen_utility_account_id=None,
                    start=start,
                    end=end,
                    statement=end,
                    s3_key="abc123.pdf",
                )
            )
            end = start - timedelta(days=1)
        index_etl_run.reset_mock()
        index.update_bill_pdf_range(task_id, meter.oid, pdfs)
        expected = {
            "billingFrom": min(b.start for b in pdfs),
            "billingTo": max(b.end for b in pdfs),
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_set_interval_fields(self, index_etl_run):
        meter = self.meters[0]
        # no readings
        task_id = "abc123"
        index.set_interval_fields(task_id, [])
        self.assertEqual(
            {
                "updatedDays": 0,
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
        index.set_interval_fields(task_id, readings)
        expected = {
            "updatedDays": 3,
            "intervalFrom": dates[0],
            "intervalTo": dates[-1],
            "age": 5,
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_index_logs(self, index_etl_run):
        log_fixture = "datafeeds/common/tests/log_fixture.txt"
        with open(log_fixture, "r") as f:
            log_data = f.read()
        with mock.patch("datafeeds.common.index.config.LOGPATH", log_fixture):
            index_logs("abc123")
        index_etl_run.assert_called_once_with("abc123", {"log": log_data})
