import re

from typing import Optional, List

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.transformer import PseUrjanetTransformer
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account


class PseDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, utility: str, account_number: str, said: str):
        super().__init__(utility, account_number)
        self.account_number = account_number
        self.said = said

    def load_accounts(self) -> List[Account]:
        query = """
            select *
            from Account
            where RawAccountNumber=%s and UtilityProvider = 'PugetSoundEnergy'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account.

        PSE meter numbers tend to have a single-letter prefix, e.g. "Z".
        Urjanet sometimes stores the meter number without this prefix.
        Therefore, we look for bills with meter numbers that match either
        the prefix or non-prefixed meter number.
        """
        said_no_prefix = re.sub("^[A-Z]", "", self.said)
        query = """
            select *
            from Meter
            where AccountFK=%s and Meter.ServiceType in ('electric', 'natural_gas')
                and (Meter.MeterNumber like %s or Meter.MeterNumber like %s)
        """
        result_set = self.fetch_all(
            query, account_pk, "%{}%".format(self.said), "%{}%".format(said_no_prefix)
        )
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    # get meter number from meter data source if available, otherwise use service_id
    if datasource.meta and datasource.meta.get("meterNumber"):
        meter_number = datasource.meta.get("meterNumber")
    else:
        meter_number = meter.utility_service.service_id
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        PseDatasource(
            meter.utility_service.utility, meter.utility_account_id, meter_number,
        ),
        PseUrjanetTransformer(),
        task_id=task_id,
    )
