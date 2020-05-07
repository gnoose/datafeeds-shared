from typing import List, Optional

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account, Meter as UrjaMeter
from datafeeds.urjanet.transformer import GenericWaterTransformer


class FortWorthWaterDatasource(UrjanetPyMySqlDataSource):
    """Load Fort Worth water data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, utility: str, account_number: str):
        super().__init__(utility, account_number)
        self.account_number = self.normalize_account_number(account_number)

    @staticmethod
    def normalize_account_number(account_number: str):
        """Converts Fort Worth account numbers into a normalized format

        Raw Fort Worth account numbers have a dash ("-") in them. This function removes that dash.
        """
        return account_number.replace("-", "")

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'CityOfFortWorthTX'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[UrjaMeter]:
        """Load all meters for an account

        Currently, water, sewer meters are loaded.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer', 'irrigation') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
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
        FortWorthWaterDatasource(
            meter.utility_service.utility, meter.utility_account_id
        ),
        GenericWaterTransformer(),
        task_id,
    )
