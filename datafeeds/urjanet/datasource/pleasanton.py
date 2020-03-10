from typing import List, Optional

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.transformer import GenericWaterTransformer

from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account, Meter as UrjaMeter


class PleasantonDatasource(UrjanetPyMySqlDataSource):
    """Load Pleasanton water data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, account_number: str):
        super().__init__(account_number)

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'CityOfPleasantonCA'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[UrjaMeter]:
        """Load all meters for an account

        Currently only has water meters.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'irrigation') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
):
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        PleasantonDatasource(meter.utility_account_id),
        GenericWaterTransformer(),
        task_id,
    )
