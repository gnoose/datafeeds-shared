import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import FortWorthWaterTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/fort-worth")


class TestUrjanetFortWorthTransformer(test_util.UrjaFixtureText):
    def fortworth_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=FortWorthWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_fixture(self):
        """Tests the water transformer with missing StatementDate fields."""
        self.fortworth_fixture_test(
            "1778689632898_input.json", "1778689632898_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
