import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import FosterCityTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/fostercity")


class TestUrjanetFosterCityWaterTransformer(test_util.UrjaFixtureText):
    def foster_city_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=FosterCityTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_323vintage_fixture(self):
        """Tests the Foster City water transformer on a very simple account with only water charges"""
        self.foster_city_fixture_test(
            "323Vintage_input.json", "323Vintage_expected.json"
        )

    def test_303velocity_fixture(self):
        """Tests the Foster City water transformer on real account data with irrigation charges"""
        self.foster_city_fixture_test(
            "303Velocity_input.json", "303Velocity_expected.json"
        )

    def test_101lincoln_fixture(self):
        """Tests the Foster City water transformer on real account data with water and sewer charges"""
        self.foster_city_fixture_test(
            "101Lincoln_input.json", "101Lincoln_expected.json"
        )

    def test_393vintage_fixture(self):
        """Tests the Foster City water transformer on real account data with water and sewer charges"""
        self.foster_city_fixture_test(
            "393Vintage_input.json", "393Vintage_expected.json"
        )

    def test_384foster_fixture(self):
        """Tests the Foster City  transformer on a dataset with a "degenerate" statement (start date == end date)"""
        self.foster_city_fixture_test("384Foster_input.json", "384Foster_expected.json")


if __name__ == "__main__":
    unittest.main()
