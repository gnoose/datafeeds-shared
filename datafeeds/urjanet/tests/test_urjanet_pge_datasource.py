import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.datasource.pge import PacificGasElectricDatasource


class TestUrjanetPacGeTransformer(test_util.UrjaFixtureText):
    def test_invalid_account_number(self):
        """Ensure that an exception is thrown when creating a PG&E datasource with an invalid account number"""
        short_account_number = "012345"
        meter_number = "01234567890"
        with self.assertRaises(ValueError):
            PacificGasElectricDatasource(short_account_number, meter_number)


if __name__ == "__main__":
    unittest.main()
