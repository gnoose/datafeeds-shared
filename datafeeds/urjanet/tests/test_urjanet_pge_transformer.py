import os
import unittest
from datetime import date

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import PacificGasElectricTransformer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/pge")


class TestUrjanetPacGeTransformer(test_util.UrjaFixtureText):
    def pge_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=PacificGasElectricTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_simple_fixture(self):
        """This tests a simple, synthetic fixture with a single billing period"""

        self.pge_fixture_test(
            "simple_fixture_input.json", "simple_fixture_expected.json"
        )

    def test_simple_mutli_statement_fixture(self):
        """This tests a simple, synthetic fixture with a billing period that spans two statements"""

        self.pge_fixture_test(
            "multi_source_link_input.json", "multi_source_link_expected.json"
        )

    def test_301_industrial_fixture(self):
        """301 is a real meter that exhibits edge cases of CCA and corrections billing

        meter oid 4504960933350245
        """
        self.pge_fixture_test(
            "4504960933350245_input.json",
            "4504960933350245_expected.json",
            start_date=date(2017, 1, 1),
        )

    def test_3140_kearney_fixture(self):
        """3140 is a real meter that exhibits edge cases of CCA billing"""
        self.pge_fixture_test(
            "3140_input.json", "3140_expected.json", start_date=date(2015, 1, 1),
        )

    def test_lamesa_fixture(self):
        """The La Mesa fixture is a real meter that exhibits edge cases of CCA billing"""
        self.pge_fixture_test(
            "lamesa_input.json", "lamesa_expected.json", start_date=date(2018, 1, 1)
        )

    def test_90digital_fixture(self):
        """The 90 fixture is a real meter that exhibits edge cases of CCA billing

        meter oid 4504827624949904"""
        self.pge_fixture_test(
            "4504827624949904_input.json",
            "4504827624949904_expected.json",
            start_date=date(2017, 1, 1),
        )

    def test_nem_charges(self):
        """Test that the PG&E transformer can accurately detect NEM charges"""
        hits = ["Total NEM Charges Before Taxes", "total nem charges before taxes"]
        transformer = PacificGasElectricTransformer()
        for hit in hits:
            charge = test_util.default_charge(ChargeActualName=hit)
            self.assertTrue(transformer.is_nem_charge(charge))

    def test_adjustment_charges(self):
        """Test that the PG&E transformer can accurately detect correction charges"""
        hits = [
            "01/05/2017 - 02/03/2017 26,400.000000 kWh",
            "01/06/2017 - 02/03/2017 1,976.000000 Therms",
            "01/18/2018 - 02/15/2018 53,055.720000 kWh",
            "04/29/2015 - 04/30/2015 7,012.000000 kWh",
            "10/18/2018 - 11/18/2018 78,840.000000 kWh",
        ]
        transformer = PacificGasElectricTransformer()

        for hit in hits:
            charge = test_util.default_charge(ChargeActualName=hit)
            self.assertTrue(transformer.is_correction_charge(charge))

    def test_end_date_adjustment(self):
        """Verify that end date does not need to be adjusted."""
        for oid in [1761981218832, 1762510856194, 1762511273986]:
            self.pge_fixture_test("%s_input.json" % oid, "%s_expected.json" % oid)

    def test_single_day_usage(self):
        self.pge_fixture_test(
            "4504832674154194_input.json", "4504832674154194_expected.json"
        )

    def test_overlapping_periods(self):
        self.pge_fixture_test("1830585771461_input.json", "1830585771461_expected.json")

    def test_zero_usages_in_corrections(self):
        """
        This fixture is a real meter where corrections issued had zero usages in the same billing period
        as earlier statements in the same billing period with non-zero usages.
        """
        self.pge_fixture_test("6262199406_input.json", "6262199406_expected.json")


if __name__ == "__main__":
    unittest.main()
