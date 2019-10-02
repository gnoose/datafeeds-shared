import unittest
from datetime import date

import datafeeds.urjanet.tests.util as test_util
import datafeeds.urjanet.model as urja_model


class TestUrjanetModel(unittest.TestCase):
    def make_basic_dataset(self):
        """Helper function that creates a simple Urjanet dataset in memory"""
        account = test_util.default_account()
        meter = test_util.default_meter()
        for idx in range(1, 12):
            meter.charges.append(
                test_util.default_charge(
                    IntervalStart=date(2000, idx, 1),
                    IntervalEnd=date(2000, idx + 1, 1)))
        account.meters.append(meter)

        urja_data = urja_model.UrjanetData()
        urja_data.accounts.append(account)
        return urja_data

    def test_filter_by_date_range_after(self):
        """Test that we can filter Urjanet data with the 'after' clause (capturing all data after a certain date)"""
        urja_data = self.make_basic_dataset()

        # Filter to only see charges starting after 2000-5-1. There should be 7
        result = urja_model.filter_by_date_range(urja_data, after=date(2000, 5, 1))
        self.assertEqual(len(result.accounts), 1)
        result_account = result.accounts[0]
        self.assertEqual(len(result_account.meters), 1)
        result_meter = result_account.meters[0]
        self.assertEqual(len(result_meter.charges), 7)

        # One day back should have the same result
        result = urja_model.filter_by_date_range(urja_data, after=date(2000, 4, 30))
        self.assertEqual(len(result.accounts), 1)
        result_account = result.accounts[0]
        self.assertEqual(len(result_account.meters), 1)
        result_meter = result_account.meters[0]
        self.assertEqual(len(result_meter.charges), 7)

        # Move forward by one day to drop one charge
        result = urja_model.filter_by_date_range(urja_data, after=date(2000, 5, 2))
        self.assertEqual(len(result.accounts), 1)
        result_account = result.accounts[0]
        self.assertEqual(len(result_account.meters), 1)
        result_meter = result_account.meters[0]
        self.assertEqual(len(result_meter.charges), 6)

    def test_filter_by_date_range_before(self):
        """Test that we can filter Urjanet data with the 'before' clause (capturing all data before a certain date)"""
        urja_data = self.make_basic_dataset()

        # Filter to only see charges starting before 2000-5-1 . There should be 5
        result = urja_model.filter_by_date_range(urja_data, before=date(2000, 5, 1))
        self.assertEqual(len(result.accounts), 1)
        result_account = result.accounts[0]
        self.assertEqual(len(result_account.meters), 1)
        result_meter = result_account.meters[0]
        self.assertEqual(len(result_meter.charges), 5)

        # One day after should have the same result
        result = urja_model.filter_by_date_range(urja_data, before=date(2000, 5, 2))
        self.assertEqual(len(result.accounts), 1)
        result_account = result.accounts[0]
        self.assertEqual(len(result_account.meters), 1)
        result_meter = result_account.meters[0]
        self.assertEqual(len(result_meter.charges), 5)

        # One day before should drop a charge
        result = urja_model.filter_by_date_range(
            urja_data, before=date(2000, 4, 30))
        self.assertEqual(len(result.accounts), 1)
        result_account = result.accounts[0]
        self.assertEqual(len(result_account.meters), 1)
        result_meter = result_account.meters[0]
        self.assertEqual(len(result_meter.charges), 4)


if __name__ == "__main__":
    unittest.main()
