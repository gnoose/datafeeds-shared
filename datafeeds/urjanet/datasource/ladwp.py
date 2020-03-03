from typing import Optional, List

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.transformer import LADWPTransformer
from datafeeds.urjanet.model import Account


class LADWPDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, account_number: str, service_id: str):
        super().__init__(account_number)
        self.account_number = account_number
        self.service_id = service_id

    def load_accounts(self) -> List[Account]:
        """Get by account number."""
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'LADeptOfWAndP'
        """
        result_set = self.fetch_all(query, self.account_number)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """LADWP formats these bills so that the 'meter number' is often on two lines.

        Urjanet pulls in that second line with a space. Gridium often munges the whole
        thing into a single number. It's unclear who is right. So we use a bit of replace()
        magic to just flatten the whole thing into a single number to normalize this.
        """
        query = """
            SELECT *
            FROM Meter
            WHERE
                AccountFK=%s
                AND replace(Meter.MeterNumber, ' ', '') LIKE %s
                AND ServiceType in ('electric', 'natural_gas')
        """
        result_set = self.fetch_all(query, account_pk, "%%%s%%" % self.service_id)
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
        LADWPDatasource(meter.utility_account_id, meter.utility_service.service_id),
        LADWPTransformer(),
        task_id=task_id,
    )
