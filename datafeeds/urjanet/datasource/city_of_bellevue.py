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
from datafeeds.urjanet.transformer import CityOfBellevueTransformer


class CityOfBellevueDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, utility: str, account_number: str):
        super().__init__(utility, account_number)
        self.account_number = account_number

    def load_accounts(self) -> List[Account]:
        query = """
            select *
            from Account
            where AccountNumber=%s and UtilityProvider = 'CityOfBellevueWA'
        """
        acct_no = self.account_number
        result_set = self.fetch_all(query, acct_no)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load meters based on the service id"""
        query = """
            select *
            from Meter
            where AccountFK=%s and ServiceType in ('water', 'sewer', 'irrigation', 'sanitation')
        """
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    utility_service = meter.utility_service
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        CityOfBellevueDatasource(
            utility_service.utility,
            utility_service.utility_account_id,
        ),
        CityOfBellevueTransformer(),
        task_id=task_id,
    )
