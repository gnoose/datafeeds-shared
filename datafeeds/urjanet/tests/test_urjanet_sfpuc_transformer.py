import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import SfpucWaterTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/sfpuc")


class TestUrjanetSfpucWaterTransformer(test_util.UrjaFixtureText):
    def sfpuc_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=SfpucWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_50California_fixture(self):
        """Tests the SFPUC transformer with a relatively simple account with just water charges"""
        self.sfpuc_fixture_test("50California_input.json", "50California_expected.json")

    def test_50California_alt_fixture(self):
        """Tests the SFPUC transformer with a relatively simple account, with sewer charges"""
        self.sfpuc_fixture_test(
            "50California_alt_input.json", "50California_alt_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
