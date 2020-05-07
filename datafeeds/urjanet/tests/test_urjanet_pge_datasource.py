import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.datasource.pge import PacificGasElectricDatasource


class TestUrjanetPacGeTransformer(test_util.UrjaFixtureText):
    def test_invalid_account_number(self):
        """Ensure that an exception is thrown when loading PG&E data with an invalid account number"""
        short_account_number = "012345"
        meter_number = "01234567890"
        data_source = PacificGasElectricDatasource(
            "utility:pge", short_account_number, meter_number, None, None
        )
        with self.assertRaises(ValueError):
            data_source.load()


if __name__ == "__main__":
    unittest.main()
