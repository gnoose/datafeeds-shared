from datetime import datetime, date, timedelta
import unittest
from unittest import mock

from datafeeds import db
from datafeeds.common import test_utils, index
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

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        self.account = account
        self.meters = meters

    @mock.patch("datafeeds.common.index.index_etl_run")
    def test_set_interval_fields(self, index_etl_run):
        meter = self.meters[0]
        # create internal user
        user = SnapmeterUser(
            email="test%s@test.com" % datetime.now().strftime("%s"),
            groups=["groups:superusers"],
        )
        db.session.add(user)
        db.session.flush()
        # no readings, no users
        task_id = "abc123"
        index.set_interval_fields(task_id, meter.oid, [])
        self.assertEqual(
            {"updatedDays": 0, "emailSubscribers": 0, "accountUsers": 0},
            index_etl_run.call_args[0][1],
        )
        # with users, but user is internal
        db.session.add(SnapmeterAccountUser(user=user.oid, account=self.account.oid))
        db.session.add(
            SnapmeterUserSubscription(
                user=user.oid, subscription="snapmeter", meter=self.meters[0].oid
            )
        )
        db.session.flush()
        index_etl_run.reset_mock()
        index.set_interval_fields(task_id, meter.oid, [])
        self.assertEqual(
            {"updatedDays": 0, "emailSubscribers": 0, "accountUsers": 0},
            index_etl_run.call_args[0][1],
        )
        # with external user
        user.groups = ["groups:users"]
        db.session.add(user)
        db.session.flush()
        index_etl_run.reset_mock()
        index.set_interval_fields(task_id, meter.oid, [])
        self.assertEqual(
            {"updatedDays": 0, "emailSubscribers": 1, "accountUsers": 1},
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
            "updatedDays": 3,
            "emailSubscribers": 1,
            "accountUsers": 1,
            "intervalUpdatedFrom": dates[0],
            "intervalUpdatedTo": dates[-1],
            "age": 5,
        }
        self.assertEqual(expected, index_etl_run.call_args[0][1])
