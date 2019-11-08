import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import AustinTXTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/austin_tx")


class TestUrjanetAustinTXTransformer(test_util.UrjaFixtureText):
    def austin_tx_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=AustinTXTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_austin_tx_water(self):
        """Tests on an account with water charges."""
        self.austin_tx_fixture_test("input_4183840241.json", "expected_4183840241.json")

    def test_austin_tx_electric(self):
        """Tests on an account with electric charges."""
        self.austin_tx_fixture_test("input_0375958587.json", "expected_0375958587.json")


if __name__ == "__main__":
    unittest.main()
