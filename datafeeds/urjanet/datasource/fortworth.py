import logging
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
from datafeeds.urjanet.transformer.fortworth import FortWorthWaterTransformer

log = logging.getLogger(__name__)


class FortWorthWaterDatasource(UrjanetPyMySqlDataSource):
    """Load Fort Worth water data from an Urjanet database

    This class accepts an account number. All meters are currently loaded from each bill.
    """

    def __init__(self, utility: str, account_number: str):
        super().__init__(utility, account_number)
        self.account_number = account_number

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE RawAccountNumber like %s AND UtilityProvider = 'CityOfFortWorthTX'
        """
        # Fort Worth changed their account number formats from 001047273-000629328 to 1047273-629328; we need both
        (part1, part2) = self.account_number.split("-")
        acct_num_pattern = "%{0}-%{1}".format(int(part1), int(part2))
        log.info("RawAccountNumber like %s", acct_num_pattern)
        result_set = self.fetch_all(query, acct_num_pattern)
        rows = [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]
        return rows

    def load_meters(self, account_pk: int) -> List[UrjaMeter]:
        """Load all meters for an account

        Currently, water, sewer meters are loaded.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer', 'irrigation') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        rows = [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]
        return rows


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
        FortWorthWaterTransformer(),
        task_id,
    )
