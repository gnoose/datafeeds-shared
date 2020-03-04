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
from datafeeds.urjanet.transformer import ConstellationTransformer


CONSTELLATION_ACCOUNT_ID = "1VEN3173"


class ConstellationDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(
        self, account_number: str = CONSTELLATION_ACCOUNT_ID, said: str = None
    ):
        super().__init__(account_number)
        self.account_number = account_number
        self.said = said

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id.

        Normally, we would allow a billing scraper to be parameterized by
        account id. However, we have only one account, and in fact only one
        meter, for this utility. Rather than increase complexity (which
        would mean touching more systems than tasks), we just use constants.
        We should revisit this if we add more accounts with Constellation
        billing.
        """
        query = """
            select *
            from Account
            where AccountNumber=%s and UtilityProvider='Constellation' and
                StatementType='statement_type_bill'
        """
        result_set = self.fetch_all(query, self.account_number)

        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account.

        Note: This is just for computing line items.
        Urjanet appears to use the last 6 of our service ID as the
        meter number in their records.
        """
        query = """
            select *
            from Meter
            where Meter.AccountFK=%s and Meter.ServiceType in ('electric', 'natural_gas')
                and Meter.MeterNumber like %s
        """
        result_set = self.fetch_all(query, account_pk, "%{}%".format(self.said[-6:]))

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
        ConstellationDatasource(meter.utility_account_id),
        ConstellationTransformer(),
        task_id=task_id,
    )
