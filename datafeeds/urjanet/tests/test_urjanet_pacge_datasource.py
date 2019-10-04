import unittest
from unittest.mock import Mock

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.datasource import PacificGasElectricDataSource


class TestUrjanetPacGeTransformer(test_util.UrjaFixtureText):
    def test_invalid_account_number(self):
        """Ensure that an exception is thrown when creating a PG&E datasource with an invalid account number"""
        conn = Mock()
        short_account_number = "012345"
        meter_number = "01234567890"
        with self.assertRaises(ValueError):
            PacificGasElectricDataSource(short_account_number, meter_number)


if __name__ == "__main__":
    unittest.main()
