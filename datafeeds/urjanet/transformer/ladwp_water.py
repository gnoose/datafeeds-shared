import functools
import logging
from decimal import Decimal
from collections import defaultdict
from typing import List, Dict

from intervaltree import Interval

from datafeeds.urjanet.transformer import (
    UrjanetGridiumTransformer,
    GenericBillingPeriod,
)

from datafeeds.urjanet.model import (
    GridiumBillingPeriod,
    GridiumBillingPeriodCollection,
    DateIntervalTree,
    log_charge,
    log_usage,
    Charge,
    Usage,
    Account,
    UrjanetData,
)

log = logging.getLogger(__name__)


class LosAngelesWaterBillingPeriod(GenericBillingPeriod):
    """A simple model of an LADWP water billing period"""

    def __init__(self, account: Account):
        super().__init__(account)
        self.utility_charges: List[Charge] = []
        self.usages: List[Usage] = []
        self.source_statements: List[Account] = []

    def add_utility_charge(self, charge: Charge) -> None:
        self.utility_charges.append(charge)

    def add_usage(self, usage: Usage) -> None:
        self.usages.append(usage)

    def has_utility_charges(self) -> bool:
        return bool(self.utility_charges)

    def has_usages(self) -> bool:
        return bool(self.usages)

    def add_source_statement(self, source: Account) -> None:
        self.source_statements.append(source)

    def get_total_usage(self) -> Decimal:
        def filter_for_total(usage):
            return usage.RateComponent == "[total]"

        return Decimal(sum([u.UsageAmount for u in self.usages if filter_for_total(u)]))

    def get_total_charge(self) -> Decimal:
        """Return the sum of all charges is this billing period"""
        charge_total = Decimal(0)
        charge_total += sum([c.ChargeAmount for c in self.utility_charges])
        return charge_total

    def get_source_urls(self) -> List[str]:
        """Return a list of URLs to source statements for this period (e.g. PDFs)"""
        return [stmt.SourceLink for stmt in self.source_statements]

    def merge(self, other: "LosAngelesWaterBillingPeriod") -> bool:
        modified = False
        if other.has_utility_charges():
            if not self.has_utility_charges():
                self.utility_charges = other.utility_charges
                modified = True

        if other.has_usages():
            if not self.has_usages():
                self.usages = other.usages
                modified = True

        return modified


class LosAngelesWaterTransformer(UrjanetGridiumTransformer):
    """This class supports transforming LADWP water Urjanet data into Gridium billing periods."""

    @staticmethod
    def billing_period(account: Account) -> LosAngelesWaterBillingPeriod:
        return LosAngelesWaterBillingPeriod(account)

    def urja_to_gridium(self, urja_data: UrjanetData) -> GridiumBillingPeriodCollection:
        """Transform urjanet data into Gridium billing periods"""

        # Process the account objects in reverse order by statement date. The main motivation here is corrections;
        # we want to process the most recent billing date first, and ignore earlier data for those same dates.
        ordered_accounts = sorted(
            urja_data.accounts, key=lambda x: x.StatementDate, reverse=True
        )

        # First, we rough out the billing period dates, by iterating through the ordered accounts and pulling out
        # usage periods
        bill_history = DateIntervalTree()
        for account in ordered_accounts:
            account_periods = self.get_account_billing_periods(account)
            for ival in account_periods.intervals():
                if bill_history.overlaps(ival.begin, ival.end):
                    log.debug(
                        "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                            account.PK, ival.begin, ival.end
                        )
                    )
                else:
                    log.debug(
                        "Adding billing period: account_pk={}, start={}, end={}".format(
                            account.PK, ival.begin, ival.end
                        )
                    )
                    bill_history.add(
                        ival.begin, ival.end, LosAngelesWaterBillingPeriod(account)
                    )

        # Next, we go through the accounts again and insert relevant charge/usage information into the computed
        # billing periods
        for account in ordered_accounts:
            self.merge_statement_data(bill_history, account)

        adjusted_history = DateIntervalTree.shift_endpoints(bill_history)

        gridium_periods = []
        for ival in sorted(adjusted_history.intervals()):
            period_data = ival.data
            gridium_periods.append(
                GridiumBillingPeriod(
                    start=ival.begin,
                    end=ival.end,
                    statement=period_data.statement(),
                    total_charge=period_data.get_total_charge(),
                    peak_demand=None,  # No peak demand for water
                    total_usage=period_data.get_total_usage(),
                    source_urls=period_data.get_source_urls(),
                    line_items=period_data.utility_charges,
                    tariff=period_data.tariff(),
                )
            )
        return GridiumBillingPeriodCollection(periods=gridium_periods)

    def get_account_billing_periods(self, account: Account) -> DateIntervalTree:
        ival_tree = DateIntervalTree()
        for meter in account.meters:
            ival_tree.add(meter.IntervalStart, meter.IntervalEnd)
        ival_tree.merge_overlaps()

        return ival_tree

    def merge_statement_data(
        self, bill_history: DateIntervalTree, urja_account: Account
    ) -> None:
        statement_data: Dict[Interval, LosAngelesWaterBillingPeriod] = defaultdict(
            functools.partial(LosAngelesWaterBillingPeriod, urja_account)
        )

        for meter in urja_account.meters:
            for charge in meter.charges:
                periods = bill_history.point_query(charge.IntervalStart)
                if len(periods) == 1:
                    period = periods[0]
                    statement_data[period].add_utility_charge(charge)
                elif not periods:
                    log.debug(
                        "Charge doesn't belong to a known billing period, skipping:"
                    )
                    log_charge(log, charge, indent=1)
                else:
                    log.debug("Charge maps to multiple billing periods, skipping:")
                    log_charge(log, charge, indent=1)

            for usage in meter.usages:
                periods = bill_history.point_query(usage.IntervalStart)
                if len(periods) == 1:
                    period = periods[0]
                    statement_data[period].add_usage(usage)
                elif not periods:
                    log.debug(
                        "Usage doesn't belong to a known billing period, skipping:"
                    )
                    log_usage(log, usage, indent=1)
                else:
                    log.debug("Usage maps to multiple billing periods, skipping:")
                    log_usage(log, usage, indent=1)

        for period, new_data in statement_data.items():
            cur_data = period.data
            data_added = cur_data.merge(new_data)
            if data_added:
                cur_data.add_source_statement(urja_account)
