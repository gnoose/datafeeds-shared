import functools
import logging
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, Optional, Mapping
from intervaltree import Interval
from datafeeds.urjanet.model import (
    Usage,
    Meter,
    DateIntervalTree,
    log_charge,
    log_usage,
    Account,
    Charge,
)
from datafeeds.urjanet.transformer import PacificGasElectricTransformer

from datafeeds.urjanet.transformer.pge import PacificGasElectricBillingPeriod

log = logging.getLogger(__name__)


class PacificGasElectricXMLBillingPeriod(PacificGasElectricBillingPeriod):
    """
    PG&E Third Party Provider Billing Period formed from XML data instead of MySQL.

    Note: There will be no self.third_party_charges on this object; unlike the Urja SQL data,
    third party charges are on their own statements, so there are only self.utility_charges.
    """

    def get_total_usage(self) -> Decimal:
        """Return the total usage for this period -

        In Urjanet's "SQL" transform, if the data doesn't have a totalized record,
        they sum one for us. In the XML Third Party data, they don't totalize, so
        there is only a "total" record if one was supplied from the utility.
        Gas meters often have totalized records, and some of the kWh meters have them.

        If a "total" record isn't present, attempt to sum ourselves.
        """
        totalized = []

        for usage in self.usages:
            if usage.EnergyUnit in ["kWh", "therms"] and usage.RateComponent:
                if "total" in usage.RateComponent:
                    return Decimal(usage.UsageAmount)
                elif "peak" in usage.RateComponent:
                    totalized.append(usage)
        return Decimal(sum([usage.UsageAmount for usage in totalized]))


class PacificGasElectricUrjaXMLTransformer(PacificGasElectricTransformer):
    """This class supports transforming Urjanet XML data into Gridium billing periods.

    Overrides PacificGasElectricTransformer to designate the billing_period_class
    and considers all usages.  Unlike the PacificGasElectricTransformer, this transformer produces a single
    stream of billing data from a single billing source. It does not sum charges from different providers.
    """

    # Overrides PacificGasElectricTransformer.billing_period_class
    billing_period_class = PacificGasElectricXMLBillingPeriod

    # Overrides PacificGasElectricTransformer.shift_endpoints
    def shift_endpoints(self, history: DateIntervalTree):
        """Fixes periods where the start and end are the same.

        For CCA charges, shifting the end date backwards for disputes.  If shifting the end
        date backwards causes a one-day interval, shifts the start date backward as well.
        """
        return DateIntervalTree.shift_endpoints_end(history)

    @staticmethod
    # Overrides PacificGasElectricTransformer.consider_usage
    def consider_usage(usage: Usage) -> bool:
        """
        Whether to use a "usage" to calculate a billing period.
        Our XML data does not have "totalized" usages on kWh meters,
        so we're just considering all of our usages.
        """
        return True

    # Overrides PacificGasElectricTransformer.fill_in_usage_gaps
    def fill_in_usage_gaps(self, meter: Meter, ival_tree: DateIntervalTree):
        """Creates an IntervalTree for the missing usage.

        If missing usage data from Urja XML, create an Interval Tree with the Meter Date Range.
        We still might be able to extract Charge Data, even if the Usage is missing.

        With billing streams, the T&D bill's usage persists to the final bill, so for PG&E,
        SMD usage will be used anyway.
        """
        if not meter.usages:
            ival_tree.add(meter.IntervalStart, meter.IntervalEnd)

    # Overrides PacificGasElectricTransformer.get_best_interval_fits
    def get_best_interval_fits(
        self, bill_history: DateIntervalTree, begin: date, end: date
    ) -> Optional[Interval]:
        """Find the best matches for a given date range in the given interval tree

        If no immediate overlaps, we try adjusting the start date back one day, and then we try adjusting the end date
        forward one day. The Interval with the largest number of overlapping days is returned.
        """
        overlaps = bill_history.range_query(begin, end)

        if not overlaps:
            # try moving start date back one day
            adjusted_start = begin - timedelta(days=1)
            overlaps = bill_history.range_query(adjusted_start, end)

        if not overlaps:
            adjusted_end = end + timedelta(days=1)
            # Try moving end date forward one day
            overlaps = bill_history.range_query(begin, adjusted_end)

        if not overlaps:
            return None

        def days_overlap(start, stop, interval):
            start_overlap = max(interval.begin, start)
            end_overlap = min(interval.end, stop)
            return end_overlap - start_overlap + timedelta(days=1)

        max_overlapping = timedelta(0)
        candidate = None
        for overlap in overlaps:
            amt_overlap = days_overlap(begin, end, overlap)
            if amt_overlap > max_overlapping:
                candidate = overlap
                max_overlapping = amt_overlap

        return candidate

    # Overrides PacificGasElectricTransformer.merge_statement_data
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

        # The statement_data dict maps billing periods to (specifically, the elements of the bill_history datastructure)
        # to PacificGasElectricXMLBillingPeriod objects, which hold the billing data available in the current statement for that
        # period.
        statement_data: Dict[
            Interval, PacificGasElectricXMLBillingPeriod
        ] = defaultdict(functools.partial(self.billing_period_class, urja_account))

        # In a first pass, we iterate over all charges/usages associated with a meter, and try to insert them into the
        # right statement_data bucket.
        for meter in urja_account.meters:
            self.merge_charges(meter, bill_history, statement_data)
            self.merge_usages(meter, bill_history, statement_data)

        # Second pass: iterate through the data we've collected for this statement, and try to merge it into what
        # we already know about each billing period
        for period, new_data in statement_data.items():
            cur_data = period.data
            data_added = cur_data.merge(new_data)
            if data_added:
                cur_data.add_source_statement(urja_account)

    def merge_charges(
        self,
        meter: Meter,
        bill_history: DateIntervalTree,
        statement_data: Mapping[Interval, PacificGasElectricXMLBillingPeriod],
    ) -> None:
        """Merge charges into statement data buckets.  This differs from the PacificGasElectricTransformer in that it
        is not adding third party charges - all charges are top-level.

        CCA dates can differ a couple of days from the T&D dates, so we also add more flexibility here in
        shifting the charges, especially if we had to shift a single-day CCA bill backwards.
        """
        for charge in meter.charges:
            period = self.get_best_interval_fits(
                bill_history, charge.IntervalStart, charge.IntervalEnd
            )
            if period:
                date_diff = (charge.IntervalEnd - period.end).days
                if date_diff <= 2 and self.is_valid_charge(charge):
                    statement_data[period].add_utility_charge(charge)
            else:
                log.debug(
                    "Charge %s doesn't belong to a known billing period, skipping:",
                    charge.PK,
                )
                log_charge(log, charge, indent=1)

    def is_valid_charge(self, charge: Charge) -> bool:
        """Determines if charge should be added to billing period, with logging on error."""
        if self.is_correction_charge(charge):
            log.debug("Found a correction charge, skipping:")
            log_charge(log, charge, indent=1)
            return False
        elif self.is_nem_charge(charge):
            log.debug("Found an NEM charge, skipping:")
            log_charge(log, charge, indent=1)
            return False
        return True

    def merge_usages(
        self,
        meter: Meter,
        bill_history: DateIntervalTree,
        statement_data: Mapping[Interval, PacificGasElectricXMLBillingPeriod],
    ) -> None:
        """Determines if usage should be added to billing period, with logging on error."""
        for usage in meter.usages:
            period = self.get_best_interval_fits(
                bill_history, usage.IntervalStart, usage.IntervalEnd
            )
            if period:
                statement_data[period].add_usage(usage)
            else:
                log.debug("Usage doesn't belong to a known billing period, skipping:")
                log_usage(log, usage, indent=1)
