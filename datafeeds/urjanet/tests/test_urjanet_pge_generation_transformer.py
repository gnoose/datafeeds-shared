import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import PacificGasElectricUrjaXMLTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/pge_generation")

"""
Tests for the Third Party PG&E Transformer that transforms data from our Urja XML tables, which
is different than our Urja SQL delivery tables.
"""


class TestUrjanetPacGeGenerationTransformer(test_util.UrjaFixtureText):
    def pge_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=PacificGasElectricUrjaXMLTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_pge_generation_corrections(self):
        """This tests third party charges only being extracted on meters with corrections """

        self.pge_fixture_test("input_7891521167.json", "expected_7891521167.json")

    def test_pge_generation_more_corrections(self):
        """This tests third party charges only being extracted on meters with corrections """

        self.pge_fixture_test("input_1956598439939.json", "expected_1956598439939.json")

    def test_pge_generation_esp_customer_mismatch(self):
        """This tests third party charges where the ESP customer number (where the generation charges are logged),
        does not match the T&D SAID"""

        self.pge_fixture_test("input_7639827457.json", "expected_7639827457.json")

    def test_service_config_changes(self):
        """This tests that bills are stitched together across multiple account number and SAID changes"""

        self.pge_fixture_test("input_3179287382201.json", "expected_3179287382201.json")

    def test_pge_generation_missing_usages(self):
        """This tests a meter with a bill where the usage is not available in urja xml but the charge data
        still exists. Because billing streams will default to using the T&D usage, it is not critical that usage
        is returned on this partial. Returns the charge, rather than returning a billing gap.
        """
        self.pge_fixture_test(
            "input_4504982154174952.json", "expected_4504982154174952.json"
        )

    def test_pge_generation_one_day_cca_bills(self):
        """There is a one-day CCA bill in this data - original dates 11/1/2017 - 11/2/2017.
        End date is backed up, because there's an overlap, which causes a one day IntervalTree.
        Start date must also be backed up, so final dates: 10-31 - 11/1.

        This date shift will cause it to correctly be absorbed into the T&D bill of
        10/04/2017 - 11/01/2017
        """
        self.pge_fixture_test(
            "input_4504818421343606.json", "expected_4504818421343606.json"
        )


if __name__ == "__main__":
    unittest.main()
