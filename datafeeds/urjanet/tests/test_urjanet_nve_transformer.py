import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import NVEnergyTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/nve")


class TestUrjanetNVEnergyTransformer(test_util.UrjaFixtureText):
    def nve_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=NVEnergyTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_nve_charges(self):
        """
        Assert that gas charges and misc charges (like service fees and adjustments are excluded from final bill)
        """
        self.nve_test("1763226460173_input.json", "1763226460173_expected.json")

    def test_nve_date_overlaps(self):
        """Assert that NVE dates don't overlap when there are gas charges on the same bill.

        Pulling dates off of meter level instead of account level, which was combining dates
        from different meters.
        """
        self.nve_test(
            "1000085283202600705_input.json", "1000085283202600705_expected.json"
        )

    def test_nve_usages(self):
        """Assert that kVarh values are not getting summed in with usages"""
        self.nve_test(
            "input_1000153730007201599.json", "expected_1000153730007201599.json"
        )

    def test_nve_meter_numbers_changing_over_time(self):
        """
        Assert that if meterNumber changes to one we don't have stored, scraper will default
        to picking the meter on the bill of the same commodity
        """
        self.nve_test(
            "input_1000085283202600721.json", "expected_1000085283202600721.json"
        )

    def test_account_credits_excluded(self):
        """
        Assert previous account balance, late charges, account credits, etc. are excluded.
        """
        self.nve_test(
            "input_1000085283202600721.json", "expected_1000085283202600721.json"
        )


if __name__ == "__main__":
    unittest.main()
