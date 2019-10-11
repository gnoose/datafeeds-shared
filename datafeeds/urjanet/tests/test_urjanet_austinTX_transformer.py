import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import AustinTXTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/austinTX")


class TestUrjanetIrvineRanchTransformer(test_util.UrjaFixtureText):
    def calwater_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=AustinTXTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_austinTX(self):
        """Tests the austinTX transformer on an account with water charges."""
        self.calwater_fixture_test("input_07292000.json", "expected_07292000.json")


if __name__ == "__main__":
    unittest.main()
