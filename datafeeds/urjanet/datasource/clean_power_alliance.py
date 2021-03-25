from typing import List, Optional

from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.bill import PartialBillProviderType
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import (
    Account,
    Meter as UrjaMeter,
)
from datafeeds.urjanet.transformer.clean_power_alliance import (
    CleanPowerAllianceTransformer,
)


class SCECleanPowerAllianceDatasource(UrjanetPyMySqlDataSource):
    """Load Clean Power Alliance CCA charges that were delivered on an SCE bill from an Urjanet database"""

    def __init__(
        self,
        utility: str,
        account_number: str,
        gen_utility,
        gen_account_number,
        gen_said,
    ):
        super().__init__(utility, account_number, gen_utility, gen_account_number)
        self.account_number = self.normalize_account_number(account_number)
        # The generation SAID is the primary identifier for extracting the CCA charges from the
        # SCE bill.
        self.gen_service_id = gen_said

    @staticmethod
    def normalize_account_number(account_number: str):
        """Converts Clean Power Alliance account numbers and SAID's into a normalized format

        Raw Clean Power Alliance account numbers and SAID's have a dash ("-") in them. This function removes that dash.
        """
        return account_number.replace("-", "")

    def load_accounts(self) -> List[Account]:
        """We are pulling Clean Power Alliance charges off of an SCE bill.

        The AccountNumber in urjanet is the UtilityService.gen_service_id in our db.
        """
        query = """
            SELECT *
            FROM Account
            WHERE AccountNumber=%s AND UtilityProvider = 'SCE'
        """
        result_set = self.fetch_all(
            query, self.normalize_account_number(self.gen_service_id)
        )
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[UrjaMeter]:
        """Load all electric meters for an account"""

        query = "SELECT * FROM Meter WHERE ServiceType in ('electric') AND AccountFK=%s"
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    return run_urjanet_datafeed(
        account=account,
        meter=meter,
        datasource=datasource,
        params=params,
        urja_datasource=SCECleanPowerAllianceDatasource(
            utility=meter.utility_service.utility,
            account_number=meter.utility_account_id,
            gen_utility=meter.utility_service.utility,
            gen_account_number=meter.utility_service.gen_utility_account_id,
            gen_said=meter.utility_service.gen_service_id,
        ),
        transformer=CleanPowerAllianceTransformer(),
        task_id=task_id,
        partial_type=PartialBillProviderType.GENERATION_ONLY,
    )
