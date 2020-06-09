import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import SDGETransformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/sdge")


class TestUrjanetSDGETransformer(test_util.UrjaFixtureText):
    def sdge_test(self, input_name, expected_name, start_date=None, end_date=None):
        self.fixture_test(
            transformer=SDGETransformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test_sdge_1711649865730(self):
        self.sdge_test("1711649865730_input.json", "1711649865730_expected.json")

    def test_sdge_12224763160(self):
        self.sdge_test("12224763160_input.json", "12224763160_expected.json")

    def test_sdge_1222476316(self):
        self.sdge_test("1222476316_input.json", "1222476316_expected.json")

    def test_sdge_multi_meter(self):
        """Test transform of account that contains multiple meters."""
        self.sdge_test("4505136472785304_input.json", "4505136472785304_expected.json")

    def test_sdge_correction(self):
        """Test transform of a meter with a bill correction.

        service_id = 06695173 (not provisioned in our system; changed to 06769269)

        Original bill for 8/15/17 - 9/14/17
            https://sources.o2.urjanet.net/sourcewithhttpbasicauth?id=1e8c5420-6f4a-d5d7-95b2-22000aa6a643
            Account PK: 5622439
            14,959 kWh, $3,053.06
        Corrected bill for 8/15/17 - 9/14/17
            https://sources.o2.urjanet.net/sourcewithhttpbasicauth?id=1e8c5420-cfbe-dbd0-95b2-22000aa6a643
            Account PK: 5622300
            15,066 kWh, $3,066.06

        Mysteriously, this bill PDF shows other periods (9/14, 10/15, 11/13, 12/13) with correction
        amounts of $0.00
        """
        self.sdge_test("06695173_input.json", "06695173_expected.json")

    def test_sdge_meter_not_included(self):
        """Test transform of a statement that don't include data for the requested meter.

        SDGE Urjanet data usually (but not always) contains data for one SAID per utility account.
        This testcase verifies the case where a statement for a utility does not contain data for
        the requested MeterNumber/SAID.

        These should be skipped, and should not create billing periods with 0 usage and 0 cost.

        Example: this statement for AccountNumber/utility_account_id 15234310047 contains data
        for 06695173, but not 06769269.

        mysql> select a.PK, StatementDate, a.IntervalStart, a.IntervalEnd, m.MeterNumber, SourceLink
        from Account a, Meter m where a.PK=5650999 and m.AccountFK=a.PK
        *************************** 1. row ***************************
                   PK: 5650999
        StatementDate: 2019-07-18
        IntervalStart: 2019-06-16
          IntervalEnd: 2019-07-16
          MeterNumber: 06695173
           SourceLink: https://sources.o2.urjanet.net/sourcewithhttpbasicauth?id=1e9aaab6-5cb4-d2f3-b7d1-0ed11b1fc08a
        """
        self.sdge_test("06769269_input.json", "06769269_expected.json")

    def test_sdge_overlapping_dates(self):
        """Test that Account record with overlapping dates should use dates from Meter instead.

        This statement contains data for two meters, with different date ranges. The date ranges
        in Account overlap:
        mysql> select PK, IntervalStart, IntervalEnd, StatementDate
        from Account
        where AccountNumber='96420041075' and IntervalEnd >= '2015-07-01' and IntervalEnd < '2015-10-01';
        +---------+---------------+-------------+---------------+
        | PK      | IntervalStart | IntervalEnd | StatementDate |
        +---------+---------------+-------------+---------------+
        | 5505663 | 2015-06-22    | 2015-07-22  | 2015-07-27    |
        | 5505686 | 2015-07-22    | 2015-08-21  | 2015-09-08    |
        | 5505685 | 2015-08-20    | 2015-09-21  | 2015-09-28    |
        +---------+---------------+-------------+---------------+

        The Meter table has different date ranges for each SAID:
        mysql> select PK, MeterNumber, IntervalStart, IntervalEnd from Meter where AccountFK=5505686;
        +----------+-------------+---------------+-------------+
        | PK       | MeterNumber | IntervalStart | IntervalEnd |
        +----------+-------------+---------------+-------------+
        | 19757873 | 00641958    | 2015-07-22    | 2015-08-21  |
        | 19757874 | 06699590    | 2015-07-22    | 2015-08-20  |
        +----------+-------------+---------------+-------------+
        """
        self.sdge_test("2000499001794_input.json", "2000499001794_expected.json")


if __name__ == "__main__":
    unittest.main()
