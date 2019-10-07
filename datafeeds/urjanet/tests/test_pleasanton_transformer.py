import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import PleasantonTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/pleasanton")


class TestUrjanetPleasantonTransformer(test_util.UrjaFixtureText):
    def pleasanton_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=PleasantonTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_4097_fixture(self):
        """Tests the water transformer on account Pleasanton."""
        self.watauga_fixture_test("4097_input.json", "4097_expected.json")


if __name__ == "__main__":
    unittest.main()
