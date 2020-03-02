import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import LADWPTransformer
from datafeeds.urjanet.transformer import LosAngelesWaterTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/ladwp")


class TestUrjanetLADWPTransformer(test_util.UrjaFixtureText):
    def ladwp_water_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=LosAngelesWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def ladwp_electricity_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=LADWPTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_ladwp_water(self):
        self.ladwp_water_test("1737669238819_input.json", "1737669238819_expected.json")()

    def test_ladwp_electricity(self):
        self.ladwp_electricity_test("1707479190338_input.json", "1707479190338_expected.json")()


if __name__ == "__main__":
    unittest.main()
