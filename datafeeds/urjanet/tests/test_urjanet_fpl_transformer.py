import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/fpl")


class TestUrjanetFPLTransformer(test_util.UrjaFixtureText):
    def fpl_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=UrjanetGridiumTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_fpl(self):
        self.fpl_test("fpl01_input.json", "fpl01_expected.json")


if __name__ == "__main__":
    unittest.main()
