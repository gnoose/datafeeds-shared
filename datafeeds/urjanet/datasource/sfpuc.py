from typing import List

from . import UrjanetPyMySqlDataSource
from ..model import Account, Meter


class SfpucWaterDatasource(UrjanetPyMySqlDataSource):
    """Load San Francisco Public Utlity Commission (SFPUC) water data from an Urjanet database

    This class accepts an account number and an optional meter number. If no meter number is specified, all meters
    are loaded from each bill.
    """

    def __init__(self, account_number: str, meter_number: str = None):
        super().__init__(account_number)
        self.meter_number = meter_number

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'SanFranciscoPublicUtilities'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in result_set
        ]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load meters for an account, optionally filtering by meter ID

        Currently, both water and sewer meters are loaded.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer') AND AccountFK=%s"
        if self.meter_number:
            query = query + " AND MeterNumber='%s'"
            result_set = self.fetch_all(query, account_pk, self.meter_number)
        else:
            result_set = self.fetch_all(query, account_pk)

        return [
            UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set
        ]
