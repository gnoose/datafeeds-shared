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

    def __init__(
        self,
        utility: str,
        account_number: str,
        said: str,
        meter_number: str,
        commodity: Optional[str] = None,
    ):
        super().__init__(utility, account_number)
        self.account_number = account_number
        self.said = said
        # from snapmeter_meter_data_source.meta->>'nveMeterNumber'
        self.meter_number = meter_number
        self.commodity = commodity or ""

    @property
    def service_type(self) -> Optional[str]:
        """
        On occasion, the MeterNumber changes on a meter over time. Older MeterNumbers are not stored.
        As a last resort, use the service type to get the correct meter.
        """
        conversions = {"kw": "electric", "therms": "natural_gas"}
        return conversions.get(self.commodity, None)

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
            where Meter.AccountFK=%s and Meter.ServiceType in ('electric', 'natural_gas', 'lighting')
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

        if not result_set and self.service_type:
            # If no meters have been found for the account
            query = """
               select *
               from Meter
               where Meter.AccountFK=%s and Meter.ServiceType in (%s)
            """
            result_set = self.fetch_all(query, account_pk, self.service_type)
        return [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    meter_number = datasource.meta.get("nveMeterNumber") if datasource.meta else None
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        NVEnergyDatasource(
            meter.utility_service.utility,
            meter.utility_account_id,
            meter.utility_service.service_id,
            meter_number,
            meter.commodity,
        ),
        NVEnergyTransformer(),
        task_id=task_id,
    )
