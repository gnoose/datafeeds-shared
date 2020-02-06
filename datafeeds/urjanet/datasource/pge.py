from typing import Optional, List

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.transformer import PacificGasElectricTransformer

from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account


class PacificGasElectricDatasource(UrjanetPyMySqlDataSource):
    """Initialize a PacG&E datasource, for a given meter

    Currently, this class accepts the following account number representations:
        (1) A 10 digit account number followed by a dash and check digit (e.g. 0123456789-1)
        (2) A 10 digit account number followed by a check digit (no dash) (01234567891)
        (3) A 10 digit account number with no dash or check digit (0123456789)

    Args:
        account_number: A PG&E account number
        said: Service account ID for the meter (meter.utility_service.service_id)
    """

    def __init__(self, account_number: str, said: str):
        super().__init__(account_number)
        self.meter_id = said
        self.account_number = self.normalize_account_number(account_number)

    def normalize_account_number(self, account_number):
        """Converts PacG&E account numbers into a normalized format
        some PacG&E account numbers have a dash ("-") in them.
        This function removes that dash.
        """
        return account_number.replace("-", "")

    def validate(self):
        """Validate the configuration of this datasource. Throws an exception on failure

        Currently, ensures that:
            (1) Account numbers are at least 10 characters long
        """
        if len(self.account_number) < 10:
            raise ValueError(
                "This data source expects PG&E account numbers to be at least 10 digits long"
            )

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""

        # This query finds all Urjanet accounts which either:
        # (1) Have a RawAccountNumber prefixed by this.account_number
        # (2) Have a RawAccountNumber == this.account_number, after removing dashes from the former
        self.validate()
        query = """
            SELECT *
            FROM Account
            WHERE
                (RawAccountNumber LIKE %s OR REPLACE(RawAccountNumber, '-', '')=%s)
                AND UtilityProvider = 'PacGAndE'
        """
        account_number_prefix_regex = "{}%".format(self.account_number)
        result_set = self.fetch_all(
            query, account_number_prefix_regex, self.account_number
        )
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
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
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        PacificGasElectricDatasource(
            meter.utility_account_id, meter.utility_service.service_id
        ),
        PacificGasElectricTransformer(),
        task_id=task_id,
    )
