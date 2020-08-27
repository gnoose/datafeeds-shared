import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer.clean_power_alliance import (
    CleanPowerAllianceTransformer,
)


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/clean_power_alliance")


class TestUrjanetCleanPowerAllianceTransformer(test_util.UrjaFixtureText):
    def urja_clean_power_alliance_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=CleanPowerAllianceTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_basic_sce_cca_fixture(self):
        self.urja_clean_power_alliance_fixture_test(
            "input_3050658598.json", "expected_3050658598.json"
        )


if __name__ == "__main__":
    unittest.main()
