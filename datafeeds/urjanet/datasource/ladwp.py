from typing import List

from pymysql import Connection

from . import UrjanetPyMySqlDataSource
from ..model import Account, Meter


class LadwpWaterDatasource(UrjanetPyMySqlDataSource):
    def __init__(self, conn: Connection, account_number: str, service_id: str):
        super().__init__(conn)
        self.account_number = account_number
        self.service_id = service_id

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'LADeptOfWAndP'
        """
        acct_no = self.account_number
        result_set = self.fetch_all(query, acct_no)
        return [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in result_set
        ]

    def load_meters(self, account_pk: str) -> List[Meter]:
        """Load meters based on the service id"""
        query = """
            SELECT *
            FROM Meter
            WHERE
                AccountFK=%s
                AND ServiceType='water'
                AND PODid=%s
        """
        result_set = self.fetch_all(query, account_pk, self.service_id)
        return [
            UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set
        ]
