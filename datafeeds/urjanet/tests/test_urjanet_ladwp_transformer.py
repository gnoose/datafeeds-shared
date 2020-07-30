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
        # two billing periods on one statement, with inconsistent date ranges
        self.ladwp_electricity_test(
            "1783662075906_input.json", "1783662075906_expected.json"
        )
        # use line items sum if total includes both billing periods
        self.ladwp_electricity_test(
            "2958424893675_input.json", "2958424893675_expected.json"
        )
        # exclude cancelled floating charges
        self.ladwp_electricity_test(
            "4504741985576710_input.json", "4504741985576710_expected.json"
        )
        # exclude deposit floating charges
        self.ladwp_electricity_test(
            "1704730394626_input.json", "1704730394626_expected.json"
        )
        # exclude Sewer Service floating charges
        self.ladwp_electricity_test(
            "1707869342446_input.json", "1707869342446_expected.json"
        )
        # exclude Late Payment floating charges
        self.ladwp_electricity_test(
            "2958424893679_input.json", "2958424893679_expected.json"
        )
        # duplicate usages with dates off by 1
        """
        mysql> select PK, IntervalStart, IntervalEnd, RateComponent, UsageActualName, UsageAmount
        from `Usage`
        where AccountFK=5597518 and EnergyUnit = 'kWh' and IntervalStart >= '2017-10-01'
        order by RateComponent;
        +----------+---------------+-------------+---------------+-----------------+--------------+
        | PK       | IntervalStart | IntervalEnd | RateComponent | UsageActualName | UsageAmount  |
        +----------+---------------+-------------+---------------+-----------------+--------------+
        | 72338292 | 2017-10-11    | 2017-11-08  | [mid_peak]    | Low Peak kWh    |       0.0000 |
        | 72338293 | 2017-10-11    | 2017-11-08  | [mid_peak]    | Low Peak kWh    |  110000.0000 |
        | 72338294 | 2017-10-12    | 2017-11-08  | [mid_peak]    | Low Peak kWh    |  110000.0000 |
        | 72338300 | 2017-10-12    | 2017-11-08  | [off_peak]    | Base kWh        |  196000.0000 |
        | 72338303 | 2017-10-11    | 2017-11-08  | [off_peak]    | Base kWh        |       0.0000 |
        | 72338295 | 2017-10-12    | 2017-11-08  | [on_peak]     | High Peak kWh   |   90000.0000 |
        | 72338298 | 2017-10-11    | 2017-11-08  | [on_peak]     | High Peak kWh   |   90000.0000 |
        | 72338299 | 2017-10-11    | 2017-11-08  | [on_peak]     | High Peak kWh   |       0.0000 |
        | 72338289 | 2017-10-11    | 2017-11-08  | [total]       |                 | 1739000.0000 |
        +----------+---------------+-------------+---------------+-----------------+--------------+
        """
        self.ladwp_electricity_test(
            "1707869340434_input.json", "1707869340434_expected.json"
        )

        # Account date ranges overlap too much; use Meter date range instead
        """
        mysql> select PK, IntervalStart, IntervalEnd from Account
        where AccountNumber='2317168547' and UtilityProvider = 'LADeptOfWAndP' and
            IntervalStart > '2019-12-01' order by IntervalEnd;
        +---------+---------------+-------------+
        | PK      | IntervalStart | IntervalEnd |
        +---------+---------------+-------------+
        | 5678901 | 2019-12-02    | 2020-01-02  |
        | 5688493 | 2019-12-31    | 2020-03-03  |
        | 5692910 | 2020-03-02    | 2020-04-01  |
        +---------+---------------+-------------+

        mysql> select AccountFK, PK, MeterNumber, IntervalStart, IntervalEnd from Meter
        where AccountFK in (5678901, 5688493);
        +-----------+----------+------------------------+---------------+-------------+
        | AccountFK | PK       | MeterNumber            | IntervalStart | IntervalEnd |
        +-----------+----------+------------------------+---------------+-------------+
        |   5678901 | 20345682 | 1BPMYVL00231-0000 3342 | 2019-12-02    | 2019-12-31  |
        |   5678901 | 20345683 |                        | 2019-12-02    | 2020-01-02  |
        |   5688493 | 20368287 | 1BPMYVL00231-0000 3342 | 2019-12-31    | 2020-03-02  |
        |   5688493 | 20368288 |                        | 2020-01-02    | 2020-03-03  |
        +-----------+----------+------------------------+---------------+-------------+
        """
        self.ladwp_electricity_test(
            "1777465083514_input.json", "1777465083514_expected.json"
        )
        # Meter has Charge records with a single day date range
        self.ladwp_electricity_test(
            "3109584374973_input.json", "3109584374973_expected.json"
        )
        # Account has a single day date range
        self.ladwp_electricity_test(
            "1846296798118_input.json", "1846296798118_expected.json"
        )
        # single day Account and Meter records
        self.ladwp_electricity_test(
            "1707869342549_input.json", "1707869342549_expected.json"
        )
        self.ladwp_electricity_test(
            "1707869342551_input.json", "1707869342551_expected.json"
        )
        self.ladwp_electricity_test(
            "1846296798118_input.json", "1846296798118_expected.json"
        )
        # This meter has a long billing period (2019-06-12 to 2019-08-09). The transformer uses Charge records
        # to split these, but one Charge record (PK=237348898) has the same long period.
        self.ladwp_electricity_test(
            "1707479190340_input.json", "1707479190340_expected.json"
        )


if __name__ == "__main__":
    unittest.main()
