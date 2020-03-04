from typing import Optional, List

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer


class TriCountyDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, account_number: str, said: str):
        super().__init__(account_number)
        # self.account_number = self.normalize_account_number(account_number)
        self.account_number = account_number
        self.said = said

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'TriCountyECoopTX'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load meters based on the service id"""

        query = """SELECT * FROM Meter WHERE ServiceType = 'electric' AND AccountFK=%s
                and MeterNumber LIKE %s
                """
        result_set = self.fetch_all(query, account_pk, self.said)
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
        TriCountyDatasource(meter.utility_account_id, meter.utility_service.service_id),
        UrjanetGridiumTransformer(),
        task_id=task_id,
    )
