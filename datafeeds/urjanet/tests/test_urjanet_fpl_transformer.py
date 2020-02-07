import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import FPLTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/fpl")


class TestUrjanetFPLTransformer(test_util.UrjaFixtureText):
    def fpl_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=FPLTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_fpl(self):
        pass
        # self.fpl_test("10152200_input.json", "10152200_expected.json")

if __name__ == "__main__":
    unittest.main()
