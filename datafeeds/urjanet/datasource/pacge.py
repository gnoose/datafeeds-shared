"""Load Pacific Gas & Electric data from an Urjanet database

The main class defined here is PacificGasElectricDataSource, which
extends the UrjanetPyMySqlDataSource class. The main purpose of
this loader is to pull out data for a specific meter from the
Urjanet database, as identified by:
    (1) The account number (maps to Account.RawAccountNumber in Urja)
    (2) The service id (maps to Meter.PODid in Urja)
"""
from typing import List

from pymysql import Connection

from . import UrjanetPyMySqlDataSource
from ..model import Account, Meter


class PacificGasElectricDataSource(UrjanetPyMySqlDataSource):
    def __init__(self, conn: Connection, account_number: str, meter_id: str):
        """Initialize a PacG&E datasource, for a given meter

        Currently, this class accepts the following account number representations:
            (1) A 10 digit account number followed by a dash and check digit (e.g. 0123456789-1)
            (2) A 10 digit account number followed by a check digit (no dash) (01234567891)
            (3) A 10 digit account number with no dash or check digit (0123456789)

        Args:
            conn: A pymysql database connection
            account_number: A PG&E account number
            meter_id: A PG&E meter id
        """
        super().__init__(conn)
        self.account_number = account_number
        self.meter_id = meter_id
        self.validate()

    def validate(self):
        """Validate the configuration of this datasource. Throws an exception on failure

        Currently, ensures that:
            (1) Account numbers are at least 10 characters long
        """
        if len(self.account_number) < 10:
            raise ValueError("This data source expects PG&E account numbers to be at least 10 digits long")

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""

        # This query finds all Urjanet accounts which either:
        # (1) Have a RawAccountNumber prefixed by this.account_number
        # (2) Have a RawAccountNumber == this.account_number, after removing dashes from the former
        #
        # Note: This query could be cleaned up a little bit, but it has been this way for a while, and I'm not inclined
        # to mess with it that much. In particular, I think the REPLACE clause could be replaced with something cleaner.
        query = """
            SELECT *
            FROM Account
            WHERE
                (RawAccountNumber LIKE %s OR REPLACE(RawAccountNumber, '-', '')=%s)
                AND UtilityProvider = 'PacGAndE'
        """

        account_number_prefix_regex = "{}%".format(self.account_number)
        result_set = self.fetch_all(query, account_number_prefix_regex, self.account_number)
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
                AND ServiceType in ('electric', 'natural_gas', 'lighting')
                AND PODid LIKE %s
        """
        result_set = self.fetch_all(query, account_pk, self.meter_id)
        return [
            UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set
        ]
