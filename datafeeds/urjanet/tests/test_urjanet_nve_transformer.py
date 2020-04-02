import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import NVEnergyTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/nve")


class TestUrjanetNVEnergyTransformer(test_util.UrjaFixtureText):
    def nve_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=NVEnergyTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_nve(self):
        self.nve_test("1763226460173_input.json", "1763226460173_expected.json")


if __name__ == "__main__":
    unittest.main()
