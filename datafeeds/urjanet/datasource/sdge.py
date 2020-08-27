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
from datafeeds.urjanet.transformer import SDGETransformer


class SDGEDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, utility: str, account_number: str, said: str):
        super().__init__(utility, account_number)
        self.account_number = account_number
        self.said = said

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id."""
        query = """
            select *
            from Account
            where AccountNumber like %s and UtilityProvider = 'SDGAndE'
        """
        result_set = self.fetch_all(
            query, "%{}%".format(self.account_number.replace(" ", ""))
        )
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account."""
        query = """
            select *
            from Meter
            where Meter.AccountFK=%s and Meter.ServiceType in ('electric', 'natural_gas')
                and Meter.MeterNumber LIKE %s
        """
        result_set = self.fetch_all(
            query, account_pk, "%{}%".format(self.said.replace(" ", ""))
        )
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
        SDGEDatasource(
            meter.utility_service.utility,
            meter.utility_account_id,
            meter.utility_service.service_id,
        ),
        SDGETransformer(),
        task_id=task_id,
    )
