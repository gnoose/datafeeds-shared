import re
from typing import List

from . import UrjanetPyMySqlDataSource, CommodityType
from ..model import Account, Meter


class AustinTXDatasource(UrjanetPyMySqlDataSource):
    """Load Austin TX data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, account_number: str, commodity_type: CommodityType, said: str):
        super().__init__(account_number)
        self.account_number = self.normalize_account_number(account_number)
        self.commodity_type = commodity_type
        self.said = said

    @staticmethod
    def normalize_account_number(account_number: str) -> str:
        """Converts Austin TX account numbers into a normalized format

        Raw Austin account numbers have a dash ("-") in them ("10005589 - 105267").
        In their terminology, the first part ("10005589") is the "account number" and the
        second part is the "customer number" ("105267").
        Remove non-numeric values from the account number; the utility_account_id in the database
        may contain a letter.
        """
        return re.sub(r"[^\d]", "", account_number.split("-")[0])

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'CityofAustinTX'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load meters matching a Gridium meter SAID.

        A bill can contain usage and charges for multiple meters. Select meters where the
        Urjanet Meter.MeterNumber matches a Gridium utility_service.service_id
        """
        # The utility may totalize submeters, and have two meter numbers for one set of charges.
        # In this case, the SAID should contain both meter ids, separated by commas.
        query = "SELECT * FROM Meter WHERE ServiceType in %s AND AccountFK=%s AND MeterNumber in %s"
        result_set = self.fetch_all(
            query, self.commodity_type.value, account_pk, self.said.split(",")
        )
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]
