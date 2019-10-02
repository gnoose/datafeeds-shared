import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import LadwpWaterTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/ladwp")


class TestUrjanetLadwpWaterTransformer(test_util.UrjaFixtureText):
    def ladwp_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=LadwpWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_707wilshire_fixture(self):
        """Tests the LADWP transformer with a complex fixture based on a real set of utility bills"""
        self.ladwp_fixture_test(
            "707wilshire_input.json",
            "707wilshire_expected.json")


if __name__ == "__main__":
    unittest.main()
