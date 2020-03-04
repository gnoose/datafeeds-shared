import itertools

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
from datafeeds.urjanet.transformer import NVEnergyTransformer


class NVEnergyDatasource(UrjanetPyMySqlDataSource):
    """Load data from an Urjanet database"""

    def __init__(self, account_number: str, said: str, meter_number: str):
        super().__init__(account_number)
        self.account_number = self.normalize_account_number(account_number)
        self.said = said
        # from snapmeter_meter_data_source.meta->>'nveMeterNumber'
        self.meter_number = meter_number

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id.

        For this utility, a statement is only admissible to our
        system if there is a meter associated with it. (There's at
        most one meter per account.)
        """
        query = """
            select *
            from Account
            where AccountNumber=%s and Account.UtilityProvider='NVEnergy'
        """
        result_set = self.fetch_all(query, self.account_number)
        # Group the statements by interval, and take the latest statement for a given interval
        result_set = [
            max(group, key=lambda stmt: stmt["StatementDate"])
            for _, group in itertools.groupby(
                result_set, lambda stmt: (stmt["IntervalStart"], stmt["IntervalEnd"])
            )
        ]

        return [UrjanetPyMySqlDataSource.parse_account_row(row) for row in result_set]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account."""
        query = """
            select *
            from Meter
            where Meter.AccountFK=%s and Meter.ServiceType in ('electric', 'natural_gas')
        """
        if self.meter_number:
            query += " AND (Meter.MeterNumber LIKE %s OR Meter.MeterNumber LIKE %s)"
            result_set = self.fetch_all(
                query,
                account_pk,
                "%{}%".format(self.said),
                "%{}".format(self.meter_number),
            )
        else:
            query += " AND Meter.MeterNumber LIKE %s"
            result_set = self.fetch_all(query, account_pk, "%{}%".format(self.said))
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
        NVEnergyDatasource(
            meter.utility_account_id,
            meter.utility_service.service_id,
            datasource.meta["nveMeterNumber"],
        ),
        NVEnergyTransformer(),
        task_id=task_id,
    )
