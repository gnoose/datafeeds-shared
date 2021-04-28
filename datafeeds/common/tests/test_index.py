from datetime import datetime, date, timedelta
import unittest
from typing import List, Dict
from unittest import mock

from datafeeds import db
from datafeeds.common import test_utils, index
from datafeeds.common.index import index_logs, index_bill_records, BILLS_INDEX
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

    @mock.patch("datafeeds.common.index._get_es_connection")
    @mock.patch("datafeeds.common.index.bulk")
    def test_index_bill_records(self, bulk_index, _es_conn):
        meter = self.meters[0]
        records: List[Dict, str] = [
            # incoming only
            {
                "meter": meter.oid,
                "operation": "new",
                "incoming_initial": date(2017, 5, 5),
                "incoming_closing": date(2017, 6, 5),
                # for validation only
                "incoming_initial_str": "2017-05-05T00:00:00-07:00",
                "incoming_closing_str": "2017-06-05T00:00:00-07:00",
                "incoming_cost": 97328.22,
                "incoming_used": 400441,
                "incoming_peak": 933.6,
                "incoming_service": 3188664594280,
                "incoming_manual": False,
                "incoming_has_all_charges": True,
                "incoming_tnd_cost": 45736.33,
                "incoming_gen_cost": 51591.89,
                "incoming_source": "billing_streams",
                "incoming_tnd_used": 400441,
                "incoming_gen_used": 400441,
                "incoming_has_download": True,
            },
            # incoming and retained
            {
                "meter": meter.oid,
                "operation": "skip - cannot override",
                "incoming_initial": date(2018, 7, 12),
                "incoming_closing": date(2018, 8, 9),
                # for validation only
                "incoming_initial_str": "2018-07-12T00:00:00-07:00",
                "incoming_closing_str": "2018-08-09T00:00:00-07:00",
                "incoming_cost": 29879.27,
                "incoming_used": 255278,
                "incoming_peak": None,
                "incoming_service": 9008783371943964,
                "incoming_manual": False,
                "incoming_has_all_charges": False,
                "incoming_tnd_cost": 29879.27,
                "incoming_gen_cost": 0,
                "incoming_source": "billing_streams",
                "incoming_tnd_used": 255278,
                "incoming_gen_used": None,
                "incoming_has_download": False,
                "retained_initial": date(2018, 7, 12),
                "retained_closing": date(2018, 8, 9),
                "retained_cost": 59719.93,
                "retained_used": 255278,
                "retained_peak": 609,
                "retained_service": 9008783371943964,
                "retained_manual": True,
                "retained_has_all_charges": None,
                "retained_tnd_cost": None,
                "retained_gen_cost": None,
                "retained_source": None,
                "retained_tnd_used": None,
                "retained_gen_used": None,
                "retained_has_download": False,
            },
            {
                "meter": meter.oid,
                "operation": "update",
                "incoming_initial": date(2018, 6, 11),
                "incoming_closing": date(2018, 7, 10),
                # for validation only
                "incoming_initial_str": "2018-06-11T00:00:00-07:00",
                "incoming_closing_str": "2018-07-10T00:00:00-07:00",
                "incoming_cost": 31749.66,
                "incoming_used": 270820,
                "incoming_peak": None,
                "incoming_service": 9008625472955284,
                "incoming_manual": False,
                "incoming_has_all_charges": None,
                "incoming_tnd_cost": 31749.66,
                "incoming_gen_cost": 0,
                "incoming_source": "billing_streams",
                "incoming_tnd_used": 270820,
                "incoming_gen_used": None,
                "incoming_has_download": False,
                "replaced_initial": date(2018, 6, 11),
                "replaced_closing": date(2018, 7, 10),
                "replaced_cost": 46265.22,
                "replaced_used": 270820,
                "replaced_peak": 730,
                "replaced_service": 9008625472955284,
                "replaced_manual": True,
                "replaced_has_all_charges": None,
                "replaced_tnd_cost": None,
                "replaced_gen_cost": None,
                "replaced_source": None,
                "replaced_tnd_used": None,
                "replaced_gen_used": None,
                "replaced_has_download": False,
            },
        ]
        index_bill_records("test-scraper", records)
        docs = bulk_index.call_args_list[0][0][1]
        self.assertEqual(3, len(docs))
        for idx, doc in enumerate(docs):
            self.assertEqual(BILLS_INDEX, doc["_index"])
            source = doc["_source"]
            self.assertEqual("test-scraper", source["scraper"])
            self.assertEqual(records[idx]["operation"], source["operation"])
            self.assertEqual(str(meter.oid), source["meter"])
            self.assertEqual(meter.utility_service.service_id, source["service_id"])
            for key in ["initial", "closing"]:
                self.assertEqual(records[idx][f"incoming_{key}_str"], source[key])
            for key in ["cost", "used", "peak"]:
                self.assertEqual(records[idx][f"incoming_{key}"], source[key])
            for key in ["cost", "used", "peak"]:
                if idx == 1:
                    records[idx][f"retained_{key}"], source[f"prev_{key}"]
                if idx == 2:
                    records[idx][f"replaced_{key}"], source[f"prev_{key}"]
            if idx == 1:
                self.assertEqual("2018-07-12T00:00:00-07:00", source["prev_initial"])
                self.assertEqual("2018-08-09T00:00:00-07:00", source["prev_closing"])
            if idx == 2:
                self.assertEqual("2018-06-11T00:00:00-07:00", source["prev_initial"])
                self.assertEqual("2018-07-10T00:00:00-07:00", source["prev_closing"])
