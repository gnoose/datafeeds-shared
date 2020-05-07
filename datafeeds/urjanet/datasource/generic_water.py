from typing import Optional, List
from decimal import Decimal, InvalidOperation

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account, Usage
from datafeeds.urjanet.transformer import GenericWaterTransformer


class GenericWaterDatasource(UrjanetPyMySqlDataSource):
    """This class accepts an account number. All meters are currently loaded from each bill."""

    def __init__(
        self,
        utility: str,  # Gridium utility
        utility_provider: str,  # Urjanet utility
        account_number: str,
        conversion_factor: float = 1.0,
    ):
        super().__init__(utility, account_number)
        self.utility_provider = utility_provider
        self.account_number = self.account_number
        try:
            self.conversion_factor = Decimal(conversion_factor)
        except (TypeError, ValueError, InvalidOperation):
            self.conversion_factor = Decimal(1.0)

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id"""
        query = """
            SELECT *
            FROM Account
            WHERE RawAccountNumber=%s AND UtilityProvider = %s
        """
        result_set = self.fetch_all(query, self.account_number, self.utility_provider)
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account

        Currently, water, sewer meters are loaded.
        """

        query = "SELECT * FROM Meter WHERE ServiceType in ('water', 'sewer', 'irrigation') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]

    def load_meter_usages(self, account_pk: int, meter_pk: int) -> List[Usage]:
        """Fetch all usage info for a given meter"""
        query = """
               SELECT *
               FROM `Usage`
               WHERE AccountFK=%s AND MeterFK=%s
           """
        result_set = self.fetch_all(query, account_pk, meter_pk)
        results = [UrjanetPyMySqlDataSource.parse_usage_row(row) for row in result_set]

        for result in results:
            result.UsageAmount = self.conversion_factor * result.UsageAmount

        return results


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
        GenericWaterDatasource(
            meter.utility_service.utility,
            datasource.meta["utility_provider"],
            meter.utility_account_id,
            datasource.meta["conversion_factor"],
        ),
        GenericWaterTransformer(),
        task_id=task_id,
    )
