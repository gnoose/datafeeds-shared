import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import AmericanTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/american")


class TestUrjanetAmericanTransformer(test_util.UrjaFixtureText):
    def america_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=AmericanTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_america(self):
        """Tests the CalWater transformer on an account with water charges."""
        self.america_fixture_test("10152200_input.json", "10152200_expected.json")


if __name__ == "__main__":
    unittest.main()
