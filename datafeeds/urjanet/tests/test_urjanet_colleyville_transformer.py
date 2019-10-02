import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import GenericWaterTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/colleyville")


# Note: Total use in *expected.json files is reported in CCF; there is a conversion since line items are in
# thousands of gallons.

class TestUrjanetFosterCityWaterTransformer(test_util.UrjaFixtureText):
    def foster_city_fixture_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=GenericWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date
        )

    def test_x5589_fixture(self):
        """Test the City of Colleyville water transformer on an account with water and sewer charges."""
        self.foster_city_fixture_test(
            "x5589_input.json",
            "x5589_expected.json")

    def test_x5592_fixture(self):
        """Test the City of Colleyville water transformer on an account with only irrigation charges."""
        self.foster_city_fixture_test(
            "x5592_input.json",
            "x5592_expected.json"
        )

    def test_x5593_fixture(self):
        """Test the City of Colleyville water transformer on another account with only irrigation charges."""
        self.foster_city_fixture_test(
            "x5593_input.json",
            "x5593_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
