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
from datafeeds.urjanet.transformer import LosAngelesWaterTransformer


class LosAngelesWaterDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, account_number: str, service_id: str):
        super().__init__(account_number)
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
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

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
        LosAngelesWaterDatasource(
            meter.utility_account_id, meter.utility_service.service_id
        ),
        LosAngelesWaterTransformer(),
        task_id=task_id,
    )
