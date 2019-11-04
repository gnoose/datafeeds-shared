import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import HecoTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/heco")


class TestUrjanetHecoTransformer(test_util.UrjaFixtureText):
    def heco_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=HecoTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_202013401833_fixture(self):
        """Tests the HECO transformer on account 202013401833."""
        self.202013401833_fixture_test("202013401833_input.json", "202013401833_expected.json")

    def test_202013401833_fixture(self):
        """Tests the water transformer on account 07292000."""
        self.watauga_fixture_test("07292000_input.json", "07292000_expected.json")


if __name__ == "__main__":
    unittest.main()
