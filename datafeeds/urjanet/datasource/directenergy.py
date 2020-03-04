from collections import defaultdict
from operator import itemgetter
from typing import Optional, List


from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.exceptions import ScraperPreconditionError
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer


class DirectEnergyDatasource(UrjanetPyMySqlDataSource):
    """A simple Urjanet scraper that collects data for DirectEnergy meters

    Note that DirectEnergy is not a utility, but a retailer. But Urjanet
    displays them as a utility.

    WARNING: The scraper currently makes a variety of assumptions that
    likely limits its general applicability. It currently supports a
    handful of Shorenstien meters, and will need considerable work if
    it is to be used in a broader context.  Assumptions include:

    1) Each statement must contain data for a single POD id.

    2) The date interval on each statement accurately represents the
    billing period for the associated meter/POD id.

    3) The roll-up charge presented by Urjanet for each statement is
    an accurate portrayal of the billing period.

    I bring up (3) because other Urja scrapers compute the bill total
    by summing line items, as happens in other Urjanet scrapers
    (esp. the PGE scraper). Instead we simply use the roll-up from
    Urjanet (specifically, the "NewCharges" attribute of a statement).
    """

    def __init__(self, account_number: str, said: str):
        super().__init__(account_number)
        self.account_number = account_number
        self.said = said

    def load_accounts(self) -> List[Account]:
        """Load accounts based on the account id.

        Note: Currently this scraper does NOT filter by utility name.
        The reason for this is that some of the older datapoints for
        these meters have a different utility name, but the Urjanet
        data adheres to the same constraints. This may need to be
        revisited in the future.
        """
        query = """
            select *
            from Account, Meter
            where Account.PK=Meter.AccountFK and Meter.PODid=%s
        """
        result_set = self.fetch_all(query, self.account_number)

        # For each billing period, we extract the most recent statement,
        # to account for billing corrections.
        groups = defaultdict(list)
        for stmt in result_set:
            date_range = (stmt["IntervalStart"], stmt["IntervalEnd"])
            groups[date_range].append(stmt)
        get_date = itemgetter("StatementDate")

        return [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in [max(group, key=get_date) for group in groups.values()]
        ]

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load all meters for an account."""
        query = """
            select *
            from Meter
            where AccountFK=%s and ServiceType in ('electric', 'gas')
        """
        result_set = self.fetch_all(query, account_pk)
        # Check our requirement that each statement has data about a single POD id
        pod_ids = [mtr["PODid"] for mtr in result_set]
        if len(set(pod_ids)) > 1:
            msg = (
                "This scraper requires each statement to "
                "contain information for a single meter/PODid. "
                "Violated by: Statement PK=={1}"
            )
            msg = msg.format(account_pk)
            raise ScraperPreconditionError(msg)

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
        DirectEnergyDatasource(
            meter.utility_account_id, meter.utility_service.service_id
        ),
        UrjanetGridiumTransformer(),
        task_id=task_id,
    )
