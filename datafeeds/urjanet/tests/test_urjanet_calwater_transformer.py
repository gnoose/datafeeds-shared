import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import GenericWaterTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/calwater")


class TestUrjanetIrvineRanchTransformer(test_util.UrjaFixtureText):
    def calwater_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=GenericWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_calwater(self):
        """Tests the CalWater transformer on an account with water charges."""
        self.calwater_fixture_test("calwater_input.json", "calwater_expected.json")


if __name__ == "__main__":
    unittest.main()
