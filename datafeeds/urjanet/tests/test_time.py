from datetime import date
import unittest

from datafeeds.urjanet.model import DateIntervalTree


class TimeTests(unittest.TestCase):
    def test_shift_endpoints(self):
        tree = DateIntervalTree()
        # default behavior
        # for: 2019-02-01 - 2019-02-28, 2019-02-28 - 2019-03-31
        # to:  2019-02-01 - 2019-02-27, 2020-02-28 - 2019-03-31
        tree.add(date(2019, 2, 28), date(2019, 3, 31), None)
        tree.add(date(2019, 2, 1), date(2019, 2, 28), None)
        shifted = sorted(DateIntervalTree.shift_endpoints(tree).intervals())
        self.assertEqual(date(2019, 2, 1), shifted[0].begin)
        self.assertEqual(date(2019, 2, 27), shifted[0].end)
        self.assertEqual(date(2019, 2, 28), shifted[1].begin)
        self.assertEqual(date(2019, 3, 31), shifted[1].end)

    def test_shift_endpoints_start(self):
        tree = DateIntervalTree()
        # shift start
        # for: 2019-02-01 - 2019-02-28, 2019-02-28 - 2019-03-31
        # to:  2019-02-01 - 2019-02-28, 2020-03-01 - 2019-03-31
        tree.add(date(2019, 2, 28), date(2019, 3, 31), None)
        tree.add(date(2019, 2, 1), date(2019, 2, 28), None)
        shifted = sorted(DateIntervalTree.shift_endpoints_start(tree).intervals())
        print(shifted)
        self.assertEqual(date(2019, 2, 1), shifted[0].begin)
        self.assertEqual(date(2019, 2, 28), shifted[0].end)
        self.assertEqual(date(2019, 3, 1), shifted[1].begin)
        self.assertEqual(date(2019, 3, 31), shifted[1].end)
