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


class FPLDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, account_number: str, said: str):
        super().__init__(account_number)
        self.account_number = account_number
        self.said = said

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id."""
        query = """
            select *
            from Account
            where AccountNumber=%s and Account.UtilityProvider='FPL'
        """
        result_set = self.fetch_all(query, self.account_number)

        return [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in result_set
        ]


    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account.

        Normally we'd pull only bill data that matches our service ID. However, FPL has a habit of
        reassigning new service IDs to the same meter. To handle that we're just always going
        to assume that meters in the same account are the same meter, regardless of the ID. This
        assumption depends on the arrangement in which only a single meter is ever attached
        to one statement. If that isn't true, bail out and fix this some other way.
        """
        query = """
            select *
            from Meter
            where Meter.AccountFK=%s
        """
        result_set = self.fetch_all(query, account_pk)
        assert len(result_set) <= 1

        return [
            UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set
        ]

    def load_meter_charges(self, account_pk: int, meter_pk: int) -> List[Charge]:
        """Fetch all charge info for a given meter

        In FPL the taxes for a bill are not stored at the 'Meter' level in Urjanet.
        Instead they are associated with the account only. This means that in order to pull
        the data properly we have to make a couple of assumptions.

        1. There can not be multiple bills in one statement, as we'd have no idea how to associate
           taxes with the right bill (the Meter in urjanet parlance is required to do that and
           in this case that is NULL)
        2. There can not be more than one meter associated with the statement. This is hard to detect
           so we're ignoring it for now.
        """
        query = """
            SELECT *
            FROM Charge
            WHERE AccountFK=%s
        """
        result_set = self.fetch_all(query, account_pk)
        return [UrjanetPyMySqlDataSource.parse_charge_row(row) for row in result_set]

    def load_meter_usages(self, account_pk: int, meter_pk: int) -> List[Usage]:
        """Fetch all usage info for a given meter"""
        query = """
            SELECT *
            FROM `Usage`
            WHERE MeterFK=%s
        """
        result_set = self.fetch_all(query, meter_pk)
        return [UrjanetPyMySqlDataSource.parse_usage_row(row) for row in result_set]


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
        FPLDatasource(meter.utility_account_id),
        UrjanetGridiumTransformer(),
        task_id=task_id,
    )
