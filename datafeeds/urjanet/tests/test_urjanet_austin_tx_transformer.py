import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import AustinTXTransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/austin_tx")


class TestUrjanetAustinTXTransformer(test_util.UrjaFixtureText):
    def austin_tx_fixture_test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=AustinTXTransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_austin_tx_water(self):
        """Tests on an account with water charges.

        This account contains bills with charges for two separate meters.
        """
        self.austin_tx_fixture_test(
            "input_4183840241_227386.json", "expected_4183840241_227386.json"
        )
        self.austin_tx_fixture_test(
            "input_4183840241_234114.json", "expected_4183840241_234114.json"
        )

    def test_austin_tx_water_and_electric(self):
        """Tests on account with both water and electric charges."""
        self.austin_tx_fixture_test(
            "input_2901591427_9999590.json", "expected_2901591427_9999590.json"
        )
        self.austin_tx_fixture_test(
            "input_2901591427_213758.json", "expected_2901591427_213758.json"
        )

    def test_austin_tx_other(self):
        """Test other oddities with Austin bills."""
        # electric meter with late fee
        self.austin_tx_fixture_test(
            "input_0375958587_4000187.json", "expected_0375958587_4000187.json"
        )
        # multiple submeters
        self.austin_tx_fixture_test(
            "input_8166749309_6106597,2003604.json",
            "expected_8166749309_6106597,2003604.json",
        )
        self.austin_tx_fixture_test(
            "input_8166749309_6104572.json", "expected_8166749309_6104572.json"
        )


if __name__ == "__main__":
    unittest.main()
