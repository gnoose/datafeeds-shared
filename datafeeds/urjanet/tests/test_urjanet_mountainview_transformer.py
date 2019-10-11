import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import GenericWaterTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/mountainview")


class TestUrjanetMountainViewTransformer(test_util.UrjaFixtureText):
    def mountainview_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=GenericWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_adobe_fixture(self):
        """Tests the water transformer on an account with water charges."""
        self.mountainview_fixture_test("input_905014620001.json", "expected_905014620001.json")


if __name__ == "__main__":
    unittest.main()
