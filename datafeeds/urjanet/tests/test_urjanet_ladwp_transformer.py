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
        self.ladwp_water_test("1737669238819_input.json", "1737669238819_expected.json")

    def test_ladwp_electricity(self):
        self.ladwp_electricity_test(
            "1707479190338_input.json", "1707479190338_expected.json"
        )
        # include only kWh in usage; this meter has kVARH values in Usage
        """
        mysql> select UsageActualName, RateComponent, UsageAmount, EnergyUnit from `Usage`
        where MeterFK=20380627 and IntervalEnd='2020-04-02' and RateComponent='[total]';
        +-----------------+---------------+-------------+------------+
        | UsageActualName | RateComponent | UsageAmount | EnergyUnit |
        +-----------------+---------------+-------------+------------+
        |                 | [total]       | 421600.0000 | kWh        |
        |                 | [total]       |  68000.0000 | kVARH      |
        +-----------------+---------------+-------------+------------+
        """
        self.ladwp_electricity_test(
            "1897216016387_input.json", "1897216016387_expected.json"
        )
        # when there are two SAIDs in an account, get charges only for the requested SAID
        self.ladwp_electricity_test(
            "1783933378562_input.json", "1783933378562_expected.json"
        )
        # get max of several peaks, not the sum of the peaks
        self.ladwp_electricity_test(
            "1783662075908_input.json", "1783662075908_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
