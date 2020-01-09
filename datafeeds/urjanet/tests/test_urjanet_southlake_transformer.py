import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import SouthlakeTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/southlake")


class TestUrjanetSouthlakeTransformer(test_util.UrjaFixtureText):
    def southlake_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=SouthlakeTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_florence_elem_fixture(self):
        """Tests the Southlake water transformer on an account with water charges."""
        self.southlake_fixture_test(
            "florence_elem_input.json", "florence_elem_expected.json"
        )

    def test_keller_isd_fixture(self):
        """Tests the Southlake water transformer on an account with water charges."""
        self.southlake_fixture_test("keller_isd_input.json", "keller_isd_expected.json")


if __name__ == "__main__":
    unittest.main()
