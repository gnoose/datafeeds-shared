import functools
import re
import logging
from decimal import Decimal
from collections import defaultdict
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional

from intervaltree import Interval

from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import (
    GridiumBillingPeriod,
    GridiumBillingPeriodCollection,
    DateIntervalTree,
    Charge,
    Usage,
    Account,
    UrjanetData,
    log_usage,
    log_charge,
    Meter,
)

log = logging.getLogger(__name__)

# A correction charge occurs when a previous bill is nullified and replaced with a new set of charges
# This typically manifests in Urjanet as a charge labeled with the date range that is being corrected
# (along with some other details such as the usage total for that period). This regular expression looks
# for the date range piece in the line item name.
CORRECTION_REGEX = re.compile(r"(\d*/\d*/\d*) - (\d*/\d*/\d*).*")


class PacificGasElectricBillingPeriod(GenericBillingPeriod):
    """A data structure that stores various metadata for a PG&E billing period

    A billing period can contain:
        1) Some number of utility charges (charges directly to the utility company)
        2) Some number of third party charges (e.g. if a third party generated the electricity)
        3) Some number of "usage" measurements (here including both demand and usage amounts)
        4) A link to some number of PDF statements for the period
           Note: there can be multiple relevant statements e.g. when (1) and (2) are on different statements
    """

    def __init__(self, account: Account):
        super().__init__(account)
        self.utility_charges: List[Charge] = []
        self.third_party_charges: List[Charge] = []
        self.usages: List[Usage] = []
        self.source_statements: List[Account] = []

    def add_third_party_charge(self, charge: Charge) -> None:
        self.third_party_charges.append(charge)

    def add_utility_charge(self, charge: Charge) -> None:
        self.utility_charges.append(charge)

    def add_usage(self, usage: Usage) -> None:
        self.usages.append(usage)

    def add_source_statement(self, source: Account) -> None:
        self.source_statements.append(source)

    def has_third_party_charges(self) -> bool:
        return bool(self.third_party_charges)

    def has_utility_charges(self) -> bool:
        return bool(self.utility_charges)

    def has_usages(self) -> bool:
        return bool(self.usages)

    def get_total_charge(self) -> Decimal:
        """Return the sum of all charges is this billing period"""
        charge_total = Decimal(0)
        charge_total += sum([c.ChargeAmount for c in self.utility_charges])
        charge_total += sum([c.ChargeAmount for c in self.third_party_charges])
        return charge_total

    def get_service_id(self) -> Optional[str]:
        """Return the service id (Meter.PODid) associated with the urja meter
        Not yet persisted to bills, but persists to partial bills.
        """
        for meter in self.account.meters:
            if meter.PODid:
                return meter.PODid
        return None

    def get_utility(self) -> Optional[str]:
        """Returns the UtilityProvider attached to the account.  If available,
        returns the utility format in our db, otherwise returns the scraped version.

        Not yet persisted to bills, but persists to partial bills.
        """
        mapping = {
            "CleanPowerSF": "utility:cpsf",
            "EastBayCommunityEnergyCA": "utility:ebce",
            "MCE": "utility:mce",
            "PeninsulaCleanEnergyCA": "utility:pce",
            "SiliconValleyCleanEnergyCA": "utility:svce",
            "ValleyCleanEnergyAllianceCA": "utility:vce",
            "SanJoseCleanEnergy": "utility:sjce",
            "PioneerCommunityEnergyCA": "utility:pio",
            "MontereyBayCommunityPowerCA": "utility:mbcp",
            "PacGAndE": "utility:pge",
            "SonomaCleanPowerCA": "utility:scp",
        }
        return mapping.get(self.account.UtilityProvider, self.account.UtilityProvider)

    def get_utility_account_id(self) -> Optional[str]:
        """Return the RawAccountNumber attached to the account
        Not yet persisted to bills, but persists to partial bills.
        """
        return self.account.RawAccountNumber

    def get_total_usage(self) -> Decimal:
        """Return the total usage for this period

        We look for a usage element with RateComponent == '[total]'
        """

        def filter_for_total(usage):
            return usage.RateComponent == "[total]" and usage.EnergyUnit in [
                "kWh",
                "therms",
            ]

        return Decimal(sum([u.UsageAmount for u in self.usages if filter_for_total(u)]))

    def get_source_urls(self) -> List[str]:
        """Return a list of URLs to source statements for this period (e.g. PDFs)"""
        return [stmt.SourceLink for stmt in self.source_statements]

    def get_peak_demand(self) -> Decimal:
        """Find the peak demand for this period

        Simply, the max of all demand elements in the 'usages' list
        """

        def filter_for_peak(usage):
            return usage.EnergyUnit in ["kW"]

        return max(
            [u.UsageAmount for u in self.usages if filter_for_peak(u)],
            default=Decimal(0),
        )

    def merge(self, other: "PacificGasElectricBillingPeriod") -> bool:
        """Merge another set of billing period data into this billing period data

        Arguments:
            other: The PacificGasElectricBillingPeriod to merge into "self"

        Return:
            True if the merge added any data to this billing period, False otherwise.
        """

        modified = False
        if other.has_third_party_charges():
            if not self.has_third_party_charges():
                self.third_party_charges = other.third_party_charges
                modified = True
            else:
                log.debug(
                    "Billing period already has third party charges, not adding new data"
                )

        if other.has_utility_charges():
            if not self.has_utility_charges():
                self.utility_charges = other.utility_charges
                modified = True
            else:
                log.debug(
                    "Billing period already has utility charges, not adding new data"
                )

        if other.has_usages():
            # Checking if we already have usages, and that they are nonzero
            if not self.has_usages() or not self.get_total_usage():
                self.usages = other.usages
                modified = True
            else:
                log.debug("Billing period already has usages, not adding new data")
        return modified


class PacificGasElectricTransformer(UrjanetGridiumTransformer):
    """This class supports transforming Urjanet data into Gridium billing periods.

    This transformer attempts to sum PG&E charges with any third party provider charges if they exist,
    into a complete bill.
    """

    billing_period_class = PacificGasElectricBillingPeriod

    def shift_endpoints(self, history: DateIntervalTree):
        """Fixes periods where the start and end are the same"""
        return DateIntervalTree.shift_endpoints_start(history)

    @staticmethod
    def consider_usage(usage: Usage) -> bool:
        """Returns True if we're going to use this usage to calculate a billing period."""
        return usage.RateComponent == "[total]"

    def fill_in_usage_gaps(self, meter: Meter, ival_tree: DateIntervalTree):
        """Stub method to override if PG&E transformer should add an interval tree that
        has the same dates as Meter.IntervalStart and Meter.IntervalEnd.
        """
        return

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
            usage_periods = self.get_account_billing_periods(account)
            for ival in sorted(usage_periods.intervals(), reverse=True):
                if bill_history.overlaps(ival.begin, ival.end):
                    log.debug(
                        "Skipping overlapping usage period: account_pk={}, start={}, end={}".format(
                            account.PK, ival.begin, ival.end
                        )
                    )
                else:
                    log.debug("Adding usage period: %s - %s", ival.begin, ival.end)
                    bill_history.add(
                        ival.begin, ival.end, self.billing_period_class(account)
                    )
        # fix periods where start/end are the same
        bill_history = self.shift_endpoints(bill_history)

        # Next, we go through the accounts again and insert relevant charge/usage information into the computed
        # billing periods
        for account in ordered_accounts:
            self.merge_statement_data(bill_history, account)

        # Convert the billing periods into the expected "gridium" format
        gridium_periods = []
        for ival in sorted(bill_history.intervals()):
            period_data = ival.data
            gridium_periods.append(
                GridiumBillingPeriod(
                    start=ival.begin,
                    end=ival.end,
                    statement=period_data.statement(),
                    total_charge=period_data.get_total_charge(),
                    peak_demand=period_data.get_peak_demand(),
                    total_usage=period_data.get_total_usage(),
                    source_urls=period_data.get_source_urls(),
                    line_items=(
                        period_data.utility_charges + period_data.third_party_charges
                    ),
                    tariff=period_data.tariff(),
                    service_id=period_data.get_service_id(),
                    utility=period_data.get_utility(),
                    utility_account_id=period_data.get_utility_account_id(),
                )
            )
        return GridiumBillingPeriodCollection(periods=gridium_periods)

    def get_best_interval_fits(
        self, bill_history: DateIntervalTree, begin: date, end: date
    ) -> List[Interval]:
        """Find the best matches for a given date range in the given interval tree
        This is a little fuzzier than we might like, because PG&E tends to shift start/end dates by one day
        somewhat arbitrarily. We account for this by allowing a date range to match an interval in the bill
        history tree if the start/end dates are within one day of the tree interval.
        """
        overlaps = bill_history.range_query(begin, end)

        if not overlaps:
            adjusted_end = end + timedelta(days=1)
            # Try moving end date forward one day
            overlaps = bill_history.range_query(begin, adjusted_end)

        if not overlaps:
            # try moving start date back one day
            adjusted_start = begin - timedelta(days=1)
            overlaps = bill_history.range_query(adjusted_start, end)

        if not overlaps:
            return []

        candidates = []
        for overlap in overlaps:
            if begin >= overlap.begin and end <= overlap.end:
                candidates.append(overlap)
            else:
                start_delta = abs((overlap.begin - begin).days)
                end_delta = abs((overlap.end - end).days)
                if start_delta <= 1 and end_delta <= 1:
                    candidates.append(overlap)
        return candidates

    def merge_statement_data(
        self, bill_history: DateIntervalTree, urja_account: Account
    ) -> None:
        """Merge in data from a given statement into the overall bill history.

        Arguments:
            bill_history: An interval tree
            urja_account: An Urjanet Account object representing data from a given bill statement

        Returns:
            None. The bill_history object is updated in place.
        """

        # The statement_data dict maps billing periods (specifically, the elements of the bill_history datastructure)
        # to PacificGasElectricBillingPeriod objects, which hold the billing data available in the current statement for that
        # period.
        statement_data: Dict[Interval, PacificGasElectricBillingPeriod] = defaultdict(
            functools.partial(self.billing_period_class, urja_account)
        )

        # In a first pass, we iterate over all charges/usages associated with a meter, and try to insert them into the
        # right statement_data bucket. This involves looking at the date range on the charge/usage and determining
        # which existing billing period it falls into.
        for meter in urja_account.meters:
            for charge in meter.charges:
                periods = self.get_best_interval_fits(
                    bill_history, charge.IntervalStart, charge.IntervalEnd
                )
                if len(periods) == 1:
                    period = periods[0]
                    if charge.IntervalEnd <= period.end:
                        if self.is_correction_charge(charge):
                            log.debug("Found a correction charge, skipping:")
                            log_charge(log, charge, indent=1)
                        elif self.is_nem_charge(charge):
                            log.debug("Found an NEM charge, skipping:")
                            log_charge(log, charge, indent=1)
                        elif charge.ThirdPartyProvider:
                            statement_data[period].add_third_party_charge(charge)
                        else:
                            statement_data[period].add_utility_charge(charge)
                    else:
                        # CleanPowerSF and Peninsula Clean Power have date ranges that are off by
                        # 1; include these
                        """
                        mysql> SELECT IntervalStart, IntervalEnd, ThirdPartyProvider
                        from `Charge` where MeterFK=20335900;
                        +---------------+-------------+--------------------+
                        | IntervalStart | IntervalEnd | ThirdPartyProvider |
                        +---------------+-------------+--------------------+
                        | 2019-10-01    | 2019-10-31  |                    |
                        ...
                        | 2019-10-01    | 2019-11-01  | CleanPowerSF       |

                        https://snapmeter.com/api/v2/5a692e888df5cb756ed1ed0e/meters/4504960933350245/bills/13512160309428950/download
                          PG&E 5/25/2017 - 6/25/2017 32 billing days
                          Peninsula Clean Energy 5/25/2017 - 6/26/2017 33 billing days
                        https://snapmeter.com/api/v2/5a692e888df5cb756ed1ed0e/meters/4504960933350245/bills/22519539173272649/download
                          PG&E 6/26/2017 - 7/25/2017 30 billing days
                          Peninsula Clean Energy 6/26/2017 - 7/26/2017 31 billing days
                        """
                        date_diff = (charge.IntervalEnd - period.end).days
                        if charge.ThirdPartyProvider and date_diff == 1:
                            log.info(
                                "including 3rd party charge %s in %s",
                                charge.PK,
                                period.end,
                            )
                            log_charge(log, charge, indent=1)
                            statement_data[period].add_third_party_charge(charge)
                        else:
                            log.debug(
                                "Charge %s end date %s exceeds billing period %s, skipping:",
                                charge.PK,
                                charge.IntervalEnd,
                                period.end,
                            )
                            log_charge(log, charge, indent=1)
                elif not periods:
                    log.debug(
                        "Charge %s doesn't belong to a known billing period, skipping:",
                        charge.PK,
                    )
                    log_charge(log, charge, indent=1)
                else:
                    log.debug(
                        "Charge %s maps to multiple billing periods, skipping:",
                        charge.PK,
                    )
                    log_charge(log, charge, indent=1)

            for usage in meter.usages:
                periods = self.get_best_interval_fits(
                    bill_history, usage.IntervalStart, usage.IntervalEnd
                )
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

        # Second pass: iterate through the data we've collected for this statement, and try to merge it into what
        # we already know about each billing period
        for period, new_data in statement_data.items():
            cur_data = period.data
            data_added = cur_data.merge(new_data)
            if data_added:
                cur_data.add_source_statement(urja_account)

    def get_duration(self, start: datetime, end: datetime) -> int:
        return (end - start).days

    def get_account_billing_periods(
        self, account: Account, max_duration: int = 45
    ) -> DateIntervalTree:
        """Extract the usage periods for a given Urjanet Account object

        Recall that the Account object in Urjanet contains data about a given utility account within a single
        statement. This function takes an Account and attempts to compute the "local" billing timeline according
        to that account. The timeline is represented as a DateIntervalTree. The idea is that this "local" timeline
        will be merged with a "global" timeline representing the aggregated state of all Account objects.

        This function will filter out billing periods that are longer than a certain threshold, according
        to the max_duration argument.
        """
        ival_tree = DateIntervalTree()
        for meter in account.meters:
            for usage in meter.usages:
                if self.consider_usage(usage):
                    usage_start = usage.IntervalStart
                    usage_end = usage.IntervalEnd
                    duration = self.get_duration(usage_start, usage_end)
                    # if the total is a single day, use the Meter date range instead
                    if duration == 0:
                        log.debug(
                            "using meter data range for zero-length usage %s", usage.PK
                        )
                        usage_start = meter.IntervalStart
                        usage_end = meter.IntervalEnd
                        if self.get_duration(usage_start, usage_end) == 0:
                            log.info(
                                "Unable to use meter data range for zero-length usage %s",
                                usage.PK,
                            )
                            continue
                    if max_duration and duration > max_duration:
                        log.debug(
                            "Filtering long usage period: {} to {} ({} days)".format(
                                usage_start, usage_end, duration
                            )
                        )
                    ival_tree.add(usage_start, usage_end)
            self.fill_in_usage_gaps(meter, ival_tree)
        ival_tree.merge_overlaps()
        return ival_tree

    def is_nem_charge(self, charge: Charge) -> bool:
        """Determines whether an Urjanet charge object represents an NEM charge"""
        nem_charge_names = ["Total NEM Charges Before Taxes"]
        return any(
            charge.ChargeActualName.lower() == nem_name.lower()
            for nem_name in nem_charge_names
        )

    def is_correction_charge(self, charge: Charge) -> bool:
        """Determines whether an Urjanet charge object represents a correction.

        Corrections show up as negative line items that negative a charge from a previous statement. They have a
        distinctive name in the Urjanet database (see CORRECTION_REGEX).
        """
        return CORRECTION_REGEX.match(charge.ChargeActualName) is not None
