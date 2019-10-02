from typing import List

from pymysql import Connection

from . import UrjanetPyMySqlDataSource
from ..model import Account, Meter


class SanDiegoWaterDatasource(UrjanetPyMySqlDataSource):
    """Load San Diego water data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, conn: Connection, account_number: str):
        super().__init__(conn)
        self.account_number = account_number

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'CityOfSanDiegoCA'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in result_set
        ]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load all meters for an account

        Currently only has water meters.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        return [
            UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set
        ]
