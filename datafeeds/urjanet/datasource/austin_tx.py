from typing import List

from . import UrjanetPyMySqlDataSource, CommodityType
from ..model import Account, Meter


class AustinTXDatasource(UrjanetPyMySqlDataSource):
    """Load Austin TX data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, account_number: str, commodity_type: CommodityType):
        super().__init__(account_number)
        self.account_number = self.normalize_account_number(account_number)
        self.commodity_type = commodity_type

    @staticmethod
    def normalize_account_number(account_number: str) -> str:
        """Converts Austin TX account numbers into a normalized format

        Raw Austin account numbers have a dash ("-") in them ("10005589 - 105267").
        In their terminology, the first part ("10005589") is the "account number" and the
        second part is the "customer number" ("105267").
        """
        return account_number.split("-")[0].strip().replace(" ", "")

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'CityofAustinTX'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in result_set
        ]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load all meters of the specified service type for an account."""
        query = "SELECT * FROM Meter WHERE ServiceType = %s AND AccountFK=%s"
        result_set = self.fetch_all(query, self.commodity_type.value, account_pk)
        return [
            UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set
        ]
