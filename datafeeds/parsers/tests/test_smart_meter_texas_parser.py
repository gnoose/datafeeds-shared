from datetime import datetime, timedelta
from io import StringIO
import os
from unittest import TestCase

from datafeeds.parsers import smart_meter_texas as smt


TEST_DIR = os.path.dirname(os.path.abspath(__file__))


class TestSmartMeterTexasParser(TestCase):
    def setUp(self) -> None:
        with open(
            os.path.join(TEST_DIR, "fixtures", "smart-meter-texas-example-01.csv"), "r"
        ) as f:
            self.data = f.read()

    def test_row_processing_01(self):
        row = {
            "CONSUMPTION_GENERATION": "Consumption",
            "USAGE_START_TIME": " 23:45",
            "ESIID": "10443720009726046",
            "USAGE_END_TIME": " 00:00",
            "ESTIMATED_ACTUAL": "A",
            "USAGE_KWH": " 000000000000007.980",
            "USAGE_DATE": "2017-08-31",
        }

        start, end = smt.interval_times(row)
        self.assertEqual(datetime(2017, 8, 31, 23, 45), start)
        self.assertEqual(datetime(2017, 9, 1), end)

        demand = smt.interval_demand_kw(row)
        self.assertEqual(7.980 * 4, demand)

    def test_row_processing_02(self):
        row = {
            "ESIID": "10443720009726046",
            "USAGE_START_TIME": " 09:00",
            "ESTIMATED_ACTUAL": "A",
            "CONSUMPTION_GENERATION": "Consumption",
            "USAGE_END_TIME": " 09:15",
            "USAGE_DATE": "2017-09-29",
            "USAGE_KWH": " 000000000000000.000",
        }

        start, end = smt.interval_times(row)
        demand = smt.interval_demand_kw(row)

        self.assertEqual(datetime(2017, 9, 29, 9), start)
        self.assertEqual(datetime(2017, 9, 29, 9, 15), end)
        self.assertEqual(0.0, demand)

    def test_gather_intervals(self):
        history = dict()
        smt.gather_intervals(StringIO(self.data), history)

        self.assertEqual(1, len(history))
        self.assertIn("10443720009726046", history.keys())

        data = history["10443720009726046"]

        self.assertEqual(67200, len(data))
        self.assertEqual(
            [
                smt.IntervalDatum(
                    datetime(2017, 8, 14, 0, 0), datetime(2017, 8, 14, 0, 15), 8.88 * 4
                ),
                smt.IntervalDatum(
                    datetime(2017, 8, 14, 0, 15),
                    datetime(2017, 8, 14, 0, 30),
                    10.08 * 4,
                ),
                smt.IntervalDatum(
                    datetime(2017, 8, 14, 0, 30),
                    datetime(2017, 8, 14, 0, 45),
                    10.32 * 4,
                ),
            ],
            data[:3],
        )

    def test_prepare_timeline(self):
        history = dict()
        smt.gather_intervals(StringIO(self.data), history)
        intervals = history["10443720009726046"]

        t = smt.prepare_timeline(intervals)
        json_record = t.serialize()
        current = datetime(2017, 8, 14)
        while current <= datetime(2019, 7, 14):
            key = current.strftime("%Y-%m-%d")
            self.assertIn(key, json_record)
            self.assertEqual(96, len(json_record.get(key)))
            current += timedelta(days=1)
