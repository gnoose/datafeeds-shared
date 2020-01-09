"""
Tests for tranforming Fort Worth Urjanet data.

Note: Technically, these are tests of the Generic Water Transformer. I'm breaking them
out according to utility so that:

a) It's clear what the provenance of the test data is.
b) If we determine a utility-specific transformer is needed later, it's easy to add one.
"""

import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import GenericWaterTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/fortworth")


class TestUrjanetFortWorthCityWaterTransformer(test_util.UrjaFixtureText):
    def fort_worth_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=GenericWaterTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_3821_staghorn_fixture(self):
        """Tests the Fort Worth water transformer on an account with water and sewer charges."""
        self.fort_worth_fixture_test(
            "3821_staghorn_input.json", "3821_staghorn_expected.json"
        )

    def test_12120_woodland_fixture(self):
        """Tests the Fort Worth water transformer on an account with water and sewer charges."""
        self.fort_worth_fixture_test(
            "12120_woodland_input.json", "12120_woodland_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
