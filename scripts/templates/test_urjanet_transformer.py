import os
import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import _UtilityName_Transformer


TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/_utilityId_")


class TestUrjanet_UtilityName_Transformer(test_util.UrjaFixtureText):
    def _utilityId__test(
        self, input_name, expected_name, start_date=None, end_date=None
    ):
        self.fixture_test(
            transformer=_UtilityName_Transformer(),
            input_path=os.path.join(DATA_DIR, input_name),
            expected_path=os.path.join(DATA_DIR, expected_name),
            start_date=start_date,
            end_date=end_date,
        )

    def test__utilityId_(self):
        self._utilityId__test()


if __name__ == "__main__":
    unittest.main()
