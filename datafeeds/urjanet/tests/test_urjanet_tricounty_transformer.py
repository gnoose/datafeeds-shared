import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import TriCountyTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/tricounty")


class TestUrjanetTriCountyTransformer(test_util.UrjaFixtureText):
    def tricounty_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=TriCountyTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_tricounty(self):
        self.tricounty_test("166650_input.json", "166650_expected.json")
        self.tricounty_test("175124_input.json", "175124_expected.json")


if __name__ == "__main__":
    unittest.main()
