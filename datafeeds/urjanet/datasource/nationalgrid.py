from typing import Optional, List
from collections import defaultdict
from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer.nationalgrid import NationalGridTransformer


class NationalGridDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(
        self,
        utility: str,
        account_number: str,
        said: str,
        gen_utility: str,
        gen_utility_account_id: str,
    ):
        super().__init__(utility, account_number, gen_utility, gen_utility_account_id)
        self.account_number = account_number
        self.said = said

    @staticmethod
    def _choose_representative(group):
        group.sort(key=lambda r: r["StatementDate"])
        return group[-1]

    def _sanitize_statements(self, statements):
        """Get the latest bill for each period.

        Filter rows representing statements to get a unified picture of the
        true bills. When there is more than one bill for a period, we
        choose the latest one.
        """
        groups = defaultdict(list)
        for stat in statements:
            groups[(stat["IntervalStart"], stat["IntervalEnd"])].append(stat)

        return [self._choose_representative(grp) for _, grp in groups.items()]

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id.

        For this utility, a statement is only admissible to our
        system if there is a meter associated with it. (There's at
        most one meter per account.)
        """
        query = """
            select *
            from Account, Meter
            where Account.PK = Meter.AccountFK and Account.RawAccountNumber=%s
                and Account.UtilityProvider='NationalGrid'
        """
        result_set = self._sanitize_statements(
            self.fetch_all(query, self.account_number)
        )
        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account.

        Note: This is just for computing line items.
        For all the accounts we've provisioned thus far, statements
        and physical meters are in one to one correspondence (there
        are no submeters).
        """
        query = """
            select *
            from Meter
            where Meter.AccountFK=%s and Meter.ServiceType in ('electric', 'natural_gas')
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
        NationalGridDatasource(
            utility_service.utility,
            utility_service.utility_account_id,
            utility_service.service_id,
            utility_service.gen_utility,
            utility_service.gen_utility_account_id,
        ),
        NationalGridTransformer(),
        task_id=task_id,
    )
