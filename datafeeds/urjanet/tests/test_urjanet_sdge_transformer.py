import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import SDGETransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/sdge")


class TestUrjanetSDGETransformer(test_util.UrjaFixtureText):
    def sdge_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=SDGETransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_sdge_1711649865730(self):
        self.sdge_test("1711649865730_input.json", "1711649865730_expected.json")

    def test_sdge_12224763160(self):
        self.sdge_test("12224763160_input.json", "12224763160_expected.json")


if __name__ == "__main__":
    unittest.main()
