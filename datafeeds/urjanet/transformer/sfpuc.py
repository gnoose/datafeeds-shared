"""San Francisco Public Utility Commission (SFPUC) Urjanet water transformer"""

import logging
from decimal import Decimal
from typing import List

from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import (
    GridiumBillingPeriod,
    GridiumBillingPeriodCollection,
    DateIntervalTree,
    Account, UrjanetData
)


log = logging.getLogger(__name__)


class SfpucWaterBillingPeriod:
    """Simple model of a SFPUC water billing period.

    This is an exceptionally simple model. Each billing period is represented by a Urjanet "account" object.
    This design is based on the limited billing data we've received so far through Urjanet, and might need to
    change as we support more SFPUC water customers.

    Currently, both water and sewer charges are included in the billing period.
    """

    def __init__(self, account: Account):
        self.account = account

    def get_total_charge(self):
        return self.account.NewCharges

    def iter_usages(self):
        # We only look at water meters because some bills also have "sewer" meters that also report usage,
        # but it's just some percentage of the water meter usage and therefore redundant.
        for meter in self.account.meters:
            if meter.ServiceType == "water":
                for usage in meter.usages:
                    yield usage

    def iter_charges(self):
        for meter in self.account.meters:
            for charge in meter.charges:
                yield charge
        for charge in self.account.floating_charges:
            yield charge

    def get_total_usage(self) -> Decimal:
        def filter_for_total(usage):
            return usage.RateComponent == '[total]'

        return sum([u.UsageAmount for u in self.iter_usages() if filter_for_total(u)])

    def get_source_urls(self) -> List[str]:
        """Return a list of URLs to source statements for this period (e.g. PDFs)"""
        return [self.account.SourceLink]


def log_sfpuc_water_billing_periods(bill_history: DateIntervalTree) -> None:
    """Helper function for logging data in an interval tree holding SFPUC water bill data"""
    log.debug("Billing periods")
    for ival in sorted(bill_history.intervals()):
        period_data = ival.data
        log.debug("\t{} to {} ({} days)".format(ival.begin, ival.end, (ival.end - ival.begin).days))
        log.debug("\t\tUtility Charges:")
        for chg in period_data.iter_charges():
            log.debug(
                "\t\t\tAmt=${0}\tName='{1}'\tPK={2}\t{3}\t{4}".format(chg.ChargeAmount, chg.ChargeActualName, chg.PK,
                                                                      chg.IntervalStart, chg.IntervalEnd))
        log.debug("\t\tTotal Charge: ${}".format(period_data.get_total_charge()))
        log.debug("\t\tUsages:")
        for usg in period_data.iter_usages():
            log.debug(
                "\t\t\tAmt={0}{1}\tComponent={2}\tPK={3}\t{4}\t{5}".format(
                    usg.UsageAmount, usg.EnergyUnit, usg.RateComponent, usg.PK, usg.IntervalStart, usg.IntervalEnd))
        log.debug("\t\tTotal Usage: {}".format(period_data.get_total_usage()))
        log.debug("\t\tStatements:")
        log.debug("\t\t\t{0}\tPK={1}".format(period_data.account.SourceLink, period_data.account.PK))


class SfpucWaterTransformer(UrjanetGridiumTransformer):
    def urja_to_gridium(self, urja_data: UrjanetData) -> GridiumBillingPeriodCollection:
        """Transform Urjanet data for SFPUC water bills into Gridium billing periods"""

        # Process the account objects in reverse order by statement date, in case their are corrections
        ordered_accounts = sorted(
            urja_data.accounts, key=lambda x: x.StatementDate, reverse=True)

        # For each account, create a billing period, taking care to detect overlaps (e.g. in the case that a
        # correction bill in issued)
        bill_history = DateIntervalTree()
        for account in ordered_accounts:
            if bill_history.overlaps(account.IntervalStart, account.IntervalEnd):
                log.debug("Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                    account.PK, account.IntervalStart, account.IntervalEnd))
            else:
                log.debug("Adding billing period: account_pk={}, start={}, end={}".format(
                    account.PK, account.IntervalStart, account.IntervalEnd))
                bill_history.add(account.IntervalStart, account.IntervalEnd, SfpucWaterBillingPeriod(account))

        # Log the billing periods we determined
        log_sfpuc_water_billing_periods(bill_history)

        # Compute the final set of gridium billing periods
        gridium_periods = []
        for ival in sorted(bill_history.intervals()):
            period_data = ival.data
            gridium_periods.append(
                GridiumBillingPeriod(
                    start=ival.begin,
                    end=ival.end,
                    total_charge=period_data.get_total_charge(),
                    peak_demand=None,  # No peak demand for water
                    total_usage=period_data.get_total_usage(),
                    source_urls=period_data.get_source_urls(),
                    line_items=list(period_data.iter_charges()),
                    tariff=None))
        return GridiumBillingPeriodCollection(periods=gridium_periods)
