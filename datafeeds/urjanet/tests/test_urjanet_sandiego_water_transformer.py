import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import GenericWaterTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/sandiego")


class TestUrjanetSanDiegoTransformer(test_util.UrjaFixtureText):
    def san_diego_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=GenericWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_san_diego(self):
        """Tests the SD water transformer on an account with water charges."""
        self.san_diego_fixture_test("sandiego_input.json", "sandiego_expected.json")


if __name__ == "__main__":
    unittest.main()
