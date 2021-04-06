import unittest
from unittest.mock import MagicMock, patch

from datafeeds import db
from datafeeds.common import interval_transform, test_utils
from datafeeds.common.exceptions import InvalidMeterDataException
from datafeeds.models import Meter
from datafeeds.models.meter import Building


class IntervalTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (self.account, meters) = test_utils.create_meters()
        self.meter_ids = [m.oid for m in meters]

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    def test_positive_transform(self):
        """Positive transform sets meter readings to positive values."""
        meter = MagicMock()
        meter.interval = 15
        meter.timezone = "America/Los_Angeles"
        # all positive or null
        data = [float(idx) for idx in range(96)]
        for idx in range(0, 96, 10):
            data[idx] = None
        readings = {
            "2020-04-01": data,
        }
        (transformed, issues) = interval_transform.to_positive(readings, meter)
        self.assertFalse(issues, "all positive ok")
        for idx, val in enumerate(transformed["2020-04-01"]):
            orig_val = readings["2020-04-01"][idx]
            if orig_val is None:
                self.assertIsNone(val)
            else:
                self.assertEqual(abs(orig_val), val)

        # all negative or null
        data = [float(-1.0 * idx) for idx in range(96)]
        for idx in range(0, 96, 10):
            data[idx] = None
        readings = {
            "2020-04-02": data,
        }
        (transformed, issues) = interval_transform.to_positive(readings, meter)
        self.assertFalse(issues, "all negative ok")
        for idx, val in enumerate(transformed["2020-04-02"]):
            orig_val = readings["2020-04-02"][idx]
            if orig_val is None:
                self.assertIsNone(val)
            else:
                self.assertEqual(abs(orig_val), val)

    @patch("datafeeds.common.interval_transform.post_slack_message")
    def test_mixed_transform(self, slack):
        meter = MagicMock()
        meter.interval = 15
        meter.timezone = "America/Los_Angeles"
        intervals = int(1440 / meter.interval)
        readings = {
            # positive and zeroes
            "2020-04-01": [float(idx) if idx % 2 else 0.0 for idx in range(intervals)],
        }
        (transformed, issues) = interval_transform.to_positive(readings, meter)
        slack.assert_not_called()
        self.assertEqual(0, len(issues), "positive and 0s is ok")
        readings = {
            # negatives and zeroes
            "2020-04-02": [
                float(-1 * idx) if idx % 2 else 0.0 for idx in range(intervals)
            ],
        }
        (transformed, issues) = interval_transform.to_positive(readings, meter)
        slack.assert_not_called()
        self.assertEqual(0, len(issues), "negative and 0s is ok")
        readings = {
            # positive and zeroes
            "2020-04-01": [float(idx) if idx % 2 else 0.0 for idx in range(intervals)],
            # negatives and zeroes
            "2020-04-02": [
                float(-1 * idx) if idx % 2 else 0.0 for idx in range(intervals)
            ],
        }
        with self.assertRaises(InvalidMeterDataException) as exc:
            interval_transform.to_positive(readings, meter)
            self.assertTrue("mixed positive and negative values" in exc)
        self.assertIn("mixed positive and negative", slack.call_args_list[0][0][0])

    def test_transform(self):
        """Transform always runs positive transform."""
        # no transforms; always run positive
        meter_id = self.meter_ids[0]
        meter = db.session.query(Meter).get(meter_id)
        building_oid = 12345
        building = Building(
            oid=building_oid,
            building=building_oid,
            _timezone="America/Los_Angeles",
            account=self.account.oid,
            name="Test building name",
            visible=True,
        )
        db.session.add(building)
        db.session.flush()
        meter._building = building.oid
        readings = {
            "2020-04-02": [-1.0 * idx for idx in range(96)],
        }
        transformed = interval_transform.transform([], None, "test", meter_id, readings)
        for val in transformed["2020-04-02"]:
            self.assertTrue(val >= 0.0)
