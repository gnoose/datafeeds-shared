"""A Basic Urjanet water transformer for converting Water, Irrigation, and Sewer charges.

This transformer is applicable to many simple municipal utilities.
"""

import logging

from decimal import Decimal
from typing import List, Tuple
from datetime import date

from datafeeds.urjanet.transformer.base import log_generic_billing_periods
from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import (
    GridiumBillingPeriod,
    GridiumBillingPeriodCollection,
    DateIntervalTree,
    Account,
    UrjanetData,
)

log = logging.getLogger(__name__)

# Water conversions to CCF
CONVERSIONS = {
    "ccf": Decimal("1.0"),
    "gallons": Decimal("0.0013368"),
    "tgal": Decimal("1.3368"),  # 1000 / 748.052
}


class GenericWaterBillingPeriod(GenericBillingPeriod):
    """Simple model of a water billing period."""

    def get_total_charge(self):
        if self.account.NewCharges > Decimal(0.0):
            return self.account.NewCharges

        if (
            self.account.NewCharges == Decimal(0.0)
            and self.account.OutstandingBalance == Decimal(0.0)
            and self.account.TotalBillAmount > Decimal(0.0)
        ):
            return self.account.TotalBillAmount

        return Decimal(0.0)


class GenericWaterTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def filtered_accounts(
        urja_data: UrjanetData,
    ) -> List[Account]:  # pylint: disable=no-self-use
        return [
            account
            for account in urja_data.accounts
            if account.StatementDate is not None
        ]

    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        # Process the account objects in reverse order by statement date, in case there are corrections
        return sorted(filtered_accounts, key=lambda x: x.StatementDate, reverse=True)

    @staticmethod
    def billing_period(account: Account) -> GenericWaterBillingPeriod:
        return GenericWaterBillingPeriod(account)

    def get_account_period(self, account: Account) -> Tuple[date, date]:
        return account.IntervalStart, account.IntervalEnd

    def urja_to_gridium(self, urja_data: UrjanetData) -> GridiumBillingPeriodCollection:
        """Transform Urjanet data for water bills into Gridium billing periods"""

        filtered_accounts = self.filtered_accounts(urja_data)
        ordered_accounts = self.ordered_accounts(filtered_accounts)

        # For each account, create a billing period, taking care to detect overlaps (e.g. in the case that a
        # correction bill in issued)
        bill_history = DateIntervalTree()
        for account in ordered_accounts:
            period_start, period_end = self.get_account_period(account)
            if bill_history.overlaps(period_start, period_end):
                log.debug(
                    "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                        account.PK, period_start, period_end
                    )
                )
            else:
                log.debug(
                    "Adding billing period: account_pk={}, start={}, end={}".format(
                        account.PK, period_start, period_end
                    )
                )
                bill_history.add(period_start, period_end, self.billing_period(account))

        # Adjust date endpoints to avoid 1-day overlaps
        bill_history = DateIntervalTree.shift_endpoints(bill_history)

        # Log the billing periods we determined
        log_generic_billing_periods(bill_history)

        # Compute the final set of gridium billing periods
        gridium_periods = []
        for ival in sorted(bill_history.intervals()):
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
                    line_items=list(period_data.iter_charges()),
                    tariff=period_data.tariff(),
                )
            )
        return GridiumBillingPeriodCollection(periods=gridium_periods)
