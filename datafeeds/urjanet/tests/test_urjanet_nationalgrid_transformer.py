import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import NationalGridTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/nationalgrid")


class TestUrjanetNationalGridTransformer(test_util.UrjaFixtureText):
    def nationalgrid_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=NationalGridTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_nationalgrid(self):
        self.nationalgrid_test(
            "4504812258396414_input.json", "4504812258396414_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
