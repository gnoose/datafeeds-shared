import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import SanFranciscoWaterTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/sfpuc")


class TestUrjanetSanFranciscoWaterTransformer(test_util.UrjaFixtureText):
    def sfpuc_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=SanFranciscoWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_sfpuc(self):
        self.sfpuc_test("50_input.json", "50_expected.json")
        self.sfpuc_test("50_alt_input.json", "50_alt_expected.json")


if __name__ == "__main__":
    unittest.main()
