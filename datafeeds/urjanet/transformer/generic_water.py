from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


# Water conversions to CCF
CONVERSIONS = {
    "ccf": Decimal("1.0"),
    "gallons": Decimal("0.0013368"),
    "tgal": Decimal("1.3368")  # 1000 / 748.052
}


class GenericWaterBillingPeriod(GenericBillingPeriod):
    """Simple model of a water billing period."""

    def __init__(self, account: Account):
        self.account = account

    def get_total_charge(self):
        if self.account.NewCharges > Decimal(0.0):
            return self.account.NewCharges

        if self.account.NewCharges == Decimal(0.0) \
                and self.account.OutstandingBalance == Decimal(0.0) \
                and self.account.TotalBillAmount > Decimal(0.0):
            return self.account.TotalBillAmount

        return Decimal(0.0)

    def iter_usages(self):
        for meter in self.account.meters:
            for usage in meter.usages:
                yield usage

    def iter_unique_usages(self):
        """Yield a set of unique usage readings for this billing period

        If a bill has multiple service types on it (e.g. water and sewer), usage readings show up multiple times in
        the database. This function attempts to filter out those duplicates.
        """
        seen = set()
        for meter in self.account.meters:
            for usage in meter.usages:
                key = (usage.UsageAmount, usage.EnergyUnit, usage.IntervalStart, usage.IntervalEnd)
                if key not in seen:
                    seen.add(key)
                    yield usage

    def iter_charges(self):
        for meter in self.account.meters:
            for charge in meter.charges:
                yield charge
        for charge in self.account.floating_charges:
            yield charge

    def get_total_usage(self) -> Decimal:
        usages = [u for u in self.iter_unique_usages() if u.RateComponent == "[total]"]

        units = set(u.EnergyUnit for u in usages)
        if len(units) != 1:
            conversion = Decimal("1.0")
        else:
            unit = units.pop().lower().strip()
            conversion = CONVERSIONS.get(unit, Decimal("1.0"))

        return sum([u.UsageAmount for u in usages]) * conversion

    def get_source_urls(self) -> List[str]:
        """Return a list of URLs to source statements for this period (e.g. PDFs)"""
        return [self.account.SourceLink]




class GenericWaterTransformer(UrjanetGridiumTransformer):
    def filtered_accounts(self, urja_data: UrjanetData) -> List[Account]:  # pylint: disable=no-self-use
        return [account for account in urja_data.accounts if account.StatementDate is not None]

    def ordered_accounts(self, filtered_accounts: List[Account]) -> List[Account]:
        # Process the account objects in reverse order by statement date, in case there are corrections
        return sorted(
            filtered_accounts, key=lambda x: x.StatementDate, reverse=True)

    def billing_period(self, account: Account) -> GenericWaterBillingPeriod:
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
                logger().debug("Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                    account.PK, period_start, period_end))
            else:
                logger().debug("Adding billing period: account_pk={}, start={}, end={}".format(
                    account.PK, period_start, period_end))
                bill_history.add(period_start, period_end, self.billing_period(account))

        # Adjust date endpoints to avoid 1-day overlaps
        bill_history = DateIntervalTree.shift_endpoints(bill_history)

        # Log the billing periods we determined
        log_generic_water_billing_periods(bill_history)

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

