import unittest
from datetime import date

import datafeeds.urjanet.model as urja_model


class TestDateIntervalTree(unittest.TestCase):
    def test_point_query(self):
        """Ensure that the DateIntervalTree get_overlaps function works as expected"""
        tree = urja_model.DateIntervalTree()
        tree.add(date(2019, 1, 1), date(2019, 1, 30))
        tree.add(date(2019, 1, 30), date(2019, 2, 28))
        tree.add(date(2019, 1, 15), date(2019, 2, 15))

        # Case 1: Point is in no intervals
        empty = tree.point_query(date(2018, 12, 1))
        self.assertEqual(len(empty), 0)

        # Case 2: Point is in one interval
        # Look at first date in first interval
        one_interval = tree.point_query(date(2019, 1, 1))
        self.assertEqual(len(one_interval), 1)
        self.assertEqual(one_interval[0].begin, date(2019, 1, 1))
        self.assertEqual(one_interval[0].end, date(2019, 1, 30))

        # Look at last date in last interval
        one_interval = tree.point_query(date(2019, 2, 27))
        self.assertEqual(len(one_interval), 1)
        self.assertEqual(one_interval[0].begin, date(2019, 1, 30))
        self.assertEqual(one_interval[0].end, date(2019, 2, 28))

        # Case 3: Point is in two intervals
        two_intervals = sorted(tree.point_query(date(2019, 2, 1)))
        self.assertEqual(len(two_intervals), 2)
        self.assertEqual(two_intervals[0].begin, date(2019, 1, 15))
        self.assertEqual(two_intervals[0].end, date(2019, 2, 15))
        self.assertEqual(two_intervals[1].begin, date(2019, 1, 30))
        self.assertEqual(two_intervals[1].end, date(2019, 2, 28))

        # Case 4: Endpoints shouldn't get results
        empty = tree.point_query(date(2018, 2, 28))
        self.assertEqual(len(empty), 0)

    def test_range_query(self):
        """Ensure that the DateIntervalTree get_overlaps function works as expected"""
        tree = urja_model.DateIntervalTree()
        tree.add(date(2019, 1, 1), date(2019, 1, 30))
        tree.add(date(2019, 1, 30), date(2019, 2, 28))

        # First query: hit the first interval
        all_overlaps = tree.range_query(date(2018, 12, 1), date(2019, 1, 10))
        self.assertEqual(len(all_overlaps), 1)

        overlap = all_overlaps[0]
        self.assertEqual(overlap.begin, date(2019, 1, 1))
        self.assertEqual(overlap.end, date(2019, 1, 30))

        # Second query: hit the second interval
        all_overlaps = tree.range_query(date(2019, 2, 15), date(2019, 3, 10))
        self.assertEqual(len(all_overlaps), 1)

        overlap = all_overlaps[0]
        self.assertEqual(overlap.begin, date(2019, 1, 30))
        self.assertEqual(overlap.end, date(2019, 2, 28))

        # Third query: hit both intervals
        all_overlaps = tree.range_query(date(2019, 1, 15), date(2019, 2, 15))
        self.assertEqual(len(all_overlaps), 2)

        all_overlaps.sort(key=lambda ival: ival.begin)
        overlap1 = all_overlaps[0]
        self.assertEqual(overlap1.begin, date(2019, 1, 1))
        self.assertEqual(overlap1.end, date(2019, 1, 30))
        overlap2 = all_overlaps[1]
        self.assertEqual(overlap2.begin, date(2019, 1, 30))
        self.assertEqual(overlap2.end, date(2019, 2, 28))

        # Fourth Query: Hit no intervals
        empty = tree.range_query(date(2018, 11, 1), date(2018, 12, 1))
        self.assertEqual(len(empty), 0)

    def test_shift_endpoints(self):
        """Ensure that the shift_endpoints function works as expected"""
        tree = urja_model.DateIntervalTree()
        tree.add(date(2019, 1, 1), date(2019, 1, 10), 1)
        tree.add(date(2019, 1, 10), date(2019, 1, 20), 2)
        tree.add(date(2019, 1, 21), date(2019, 1, 30), 3)

        shifted = urja_model.DateIntervalTree.shift_endpoints(tree)
        shifted_intervals = sorted(list(shifted.intervals()))
        self.assertEqual(len(shifted_intervals), 3)

        self.assertEqual(shifted_intervals[0].begin, date(2019, 1, 1))
        self.assertEqual(
            shifted_intervals[0].end, date(2019, 1, 9)
        )  # this interval should be adjusted
        self.assertEqual(shifted_intervals[0].data, 1)  # Ensure the data remains intact

        self.assertEqual(shifted_intervals[1].begin, date(2019, 1, 10))
        self.assertEqual(shifted_intervals[1].end, date(2019, 1, 20))
        self.assertEqual(shifted_intervals[1].data, 2)

        self.assertEqual(shifted_intervals[2].begin, date(2019, 1, 21))
        self.assertEqual(shifted_intervals[2].end, date(2019, 1, 30))
        self.assertEqual(shifted_intervals[2].data, 3)
