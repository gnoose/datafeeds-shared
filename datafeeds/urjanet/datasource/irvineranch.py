from typing import List

from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account, Meter


class IrvineRanchWaterDatasource(UrjanetPyMySqlDataSource):
    """Load Irvine Ranch water data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'IrvineRanchWaterDistrict'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load all meters for an account

        Currently only has water meters.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]
