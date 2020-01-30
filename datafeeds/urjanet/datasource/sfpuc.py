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


class SanFranciscoWaterDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, account_number: str):
        super().__init__(account_number)
        self.account_number = self.normalize_account_number(account_number)

    def load_accounts(self) -> List[Account]:
        """The query for fetching accounts must be provided by implementers"""
        pass  # TODO: update this

    def load_meters(self, account_pk: int) -> List[Meter]:
        """The query for fetching meters must be provided by implementers"""
        pass  # TODO: update this


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
        SanFranciscoWaterDatasource(meter.utility_account_id),
        SanFranciscoWaterTransformer(),
        task_id=task_id,
    )
