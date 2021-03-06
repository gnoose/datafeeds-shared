from datetime import date, datetime, time
import unittest

from datafeeds.common.timeline import Timeline, SerializationError


class TimelineTests(unittest.TestCase):
    def test_interval_counts(self):
        d = date(2018, 4, 1)
        tl = Timeline(d, d)
        result = tl.serialize()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result["2018-04-01"]), 96)
        self.assertTrue(all(x is None for x in result["2018-04-01"]))

        d = date(2018, 4, 1)
        tl = Timeline(d, d, interval=10)
        result = tl.serialize()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result["2018-04-01"]), 144)
        self.assertTrue(all(x is None for x in result["2018-04-01"]))

    def test_insert(self):
        d1 = date(2018, 4, 1)
        d2 = date(2018, 4, 2)

        tl = Timeline(d1, d2)

        dt1 = datetime(2018, 4, 1)
        dt2 = datetime(2018, 4, 1, 0, 45)  # Entry 3
        dt3 = datetime(2018, 4, 1, 10, 45)  # Entry 10 * 4 + 3 = 43
        dt4 = datetime(2018, 4, 2, 11, 30)  # Entry 11 * 4 + 2 = 46
        dt5 = datetime(2018, 4, 3)

        tl.insert(dt1, 1.0)
        tl.insert(dt2, 2.0)
        tl.insert(dt3, 3.0)
        tl.insert(dt4, 4.0)

        # This insert should be disregarded by tl, since it's outside
        # the interval.
        tl.insert(dt5, 1000.0)

        result = tl.serialize()

        # Sanity checks
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result["2018-04-01"]), 96)
        self.assertEqual(len(result["2018-04-02"]), 96)

        # Value checks
        self.assertEqual(result["2018-04-01"][0], 1.0)
        self.assertEqual(result["2018-04-01"][3], 2.0)
        self.assertEqual(result["2018-04-01"][43], 3.0)
        self.assertEqual(result["2018-04-02"][46], 4.0)

        # All other entries are null
        null_count = sum(
            sum(1 for x in result[d] if x is None) for d in ["2018-04-01", "2018-04-02"]
        )
        self.assertEqual(null_count, 2 * 96 - 4)

    def test_exception(self):
        d1 = date(2018, 4, 1)
        d2 = date(2018, 4, 2)

        tl = Timeline(d1, d2)
        tl.insert(datetime(2018, 4, 1, 10, 20), 1.0)  # Not a valid interval endpoint.

        with self.assertRaises(SerializationError):
            tl.serialize()

    def test_extend_timeline(self):
        d1 = date(2020, 5, 1)
        d2 = date(2020, 5, 14)

        tl = Timeline(d1, d2)
        dt1 = datetime(2020, 5, 1)
        tl.insert(dt1, 402.1)
        self.assertEqual(tl.index[d1][time(0, 0, 0)], 402.1)

        earlier_d1 = date(2020, 4, 1)
        earlier_d2 = date(2020, 4, 16)

        # Start date of timeline is backed up
        tl.extend_timeline(earlier_d1, earlier_d2)
        self.assertEqual(tl._start, earlier_d1)
        self.assertEqual(tl._end, d2)
        # Existing interval data in original timeline is not overwritten
        self.assertEqual(tl.index[d1][time(0, 0, 0)], 402.1)
        self.assertIsNone(tl.index[earlier_d1][time(0, 0, 0)])

        # End date of timeline is extended
        dt2 = datetime(2020, 5, 14)
        tl.insert(dt2, 500)
        self.assertEqual(tl.index[d2][time(0, 0, 0)], 500)

        later_d1 = date(2020, 5, 15)
        later_d2 = date(2020, 5, 31)
        tl.extend_timeline(later_d1, later_d2)
        self.assertEqual(tl._start, earlier_d1)
        self.assertEqual(tl._end, later_d2)
        self.assertEqual(tl.index[d2][time(0, 0, 0)], 500)
