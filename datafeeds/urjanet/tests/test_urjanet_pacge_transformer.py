import os
import unittest
from datetime import date

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import PacGeGridiumTransfomer

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/pacge")


class TestUrjanetPacGeTransformer(test_util.UrjaFixtureText):
    def pacge_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=PacGeGridiumTransfomer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_simple_fixture(self):
        """This tests a simple, synthetic fixture with a single billing period"""

        self.pacge_fixture_test(
            "simple_fixture_input.json", "simple_fixture_expected.json"
        )

    def test_simple_mutli_statement_fixture(self):
        """This tests a simple, synthetic fixture with a billing period that spans two statements"""

        self.pacge_fixture_test(
            "multi_source_link_input.json", "multi_source_link_expected.json"
        )

    def test_301_industrial_fixture(self):
        """301 Industrial is a real meter that exhibits edge cases of CCA and corrections billing"""
        self.pacge_fixture_test(
            "301industrial_input.json",
            "301industrial_expected.json",
            start_date=date(2018, 1, 1),
        )

    def test_3140_kearney_fixture(self):
        """3140 Kearney is a real meter that exhibits edge cases of CCA billing"""
        self.pacge_fixture_test(
            "3140kearney_input.json",
            "3140kearney_expected.json",
            start_date=date(2018, 1, 1),
        )

    def test_lamesa_fixture(self):
        """The La Mesa fixture is a real meter that exhibits edge cases of CCA billing"""
        self.pacge_fixture_test(
            "lamesa_input.json", "lamesa_expected.json", start_date=date(2018, 1, 1)
        )

    def test_90digital_fixture(self):
        """The 90 Digital fixture is a real meter that exhibits edge cases of CCA billing"""
        self.pacge_fixture_test(
            "90digital_input.json",
            "90digital_expected.json",
            start_date=date(2018, 1, 1),
        )

    def test_nem_charges(self):
        """Test that the PG&E transformer can accurately detect NEM charges"""
        hits = ["Total NEM Charges Before Taxes", "total nem charges before taxes"]
        transformer = PacGeGridiumTransfomer()
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
        transformer = PacGeGridiumTransfomer()

        for hit in hits:
            charge = test_util.default_charge(ChargeActualName=hit)
            self.assertTrue(transformer.is_correction_charge(charge))


if __name__ == "__main__":
    unittest.main()
