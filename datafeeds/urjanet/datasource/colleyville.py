from typing import List

from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account, Meter


class ColleyvilleWaterDatasource(UrjanetPyMySqlDataSource):
    """Load City of Colleyville water data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, account_number: str):
        super().__init__(account_number)
        self.account_number = self.normalize_account_number(account_number)

    @staticmethod
    def normalize_account_number(account_number: str) -> str:
        """Converts Colleyville account numbers into a normalized format

        Raw Colleyville account numbers have a dash ("-") in them ("10005589 - 105267").
        In their terminology, the first part ("10005589") is the "account number" and the
        second part is the "customer number" ("105267").
        """
        return account_number.split("-")[0].strip()

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'CityOfColleyvilleTX'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load all meters for an account

        Currently, water, sewer meters are loaded.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer', 'irrigation') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]
