from datetime import timedelta
from decimal import Decimal
import logging
from typing import Optional

from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)

from datafeeds.urjanet.model import Account, UrjanetData, DateIntervalTree

log = logging.getLogger(__name__)


class NVEnergyBillingPeriod(GenericBillingPeriod):

    # Overrides GenericBillingPeriod.iter_charges
    def iter_charges(self):
        """
        Pulls all charges off of the attached meters, and then a portion of the floating charges,
        including certain charges that likely should have been attributed to the meter.
        """
        for meter in self.account.meters:
            for charge in meter.charges:
                yield charge

        # These are for green energy rider charges, whose usages are not incorporated into the bill, but the
        # charges are still attributed to the meter.
        included_floating_charges = [
            "ch.usage_charge",
            "ch.all_applicable_distribution_riders",
            "ch.universal_energy_charge",
            "ch.renewable_energy_surcharge",
            "ch.temp_green_power_financing",
            "ch.energy_efficiency_charge",
        ]

        for charge in self.account.floating_charges:
            # Only include certain floating charges from the bill.  If a service charge is "floating", for example,
            # it is likely to be for a different meter, such as a gas.
            if charge.ChargeId in included_floating_charges:
                yield charge

    # Overrides GenericBillingPeriod.get_total_usage
    def get_total_usage(self) -> Decimal:
        """
        Overrides get_total_usage to avoid demand and reactive_consumption (kVARH) totals being included.
        Also attempts to include usages in TOU periods if urja missed them.
        """
        tou_usages = [
            "WOTE",
            "WTOA",
            "WTOR",
            "WTOD",
            "SOTE",
            "SUMA",
            "SUMR",
            "SUMD",
            "SFTE",
            "SUFA",
            "SUFR",
            "SUFD",
            "SMTE",
        ]

        usages = [
            u
            for u in self.iter_unique_usages()
            if (u.RateComponent == "[total]" or u.UsageActualName in tou_usages)
            and u.MeasurementType == "general_consumption"
        ]

        """
        Urjanet does not always parse usages correctly when NVEnergy customers are on TOU rates.  The usage associated
        with the WOTE rate might be categorized as a total rate component, when in reality, the usages under the SOTE,
        SMTE, and SFTE rates also need to be included.

        WOTE - Winter TOU
        SOTE - Summer TOU On-Peak
        SMTE - Summer TOU Mid-Peak
        SFTE - Summer TOU Off-Peak

        We include the usages associated with TOU periods, and then remove if urja already included them in the total.
        """

        non_total = sum([u.UsageAmount for u in usages if u.RateComponent != "[total]"])
        for u in usages:
            if u.UsageAmount == non_total:
                usages.remove(u)

        return Decimal(sum([u.UsageAmount for u in usages]))

    # Overrides GenericBillingPeriod.get_total_charge
    def get_total_charge(self):
        """Sum the Charges on the Meter, along with selected floating charges"""
        charges = Decimal(0.0)
        for charge in self.iter_charges():
            charges += charge.ChargeAmount
        log.debug("\t%s summed charges\t%s", self.account.IntervalStart, charges)
        return charges

    # Overrides GenericBillingPeriod.get_peak_demand
    def get_peak_demand(self) -> Optional[Decimal]:
        """Attempt to determine peak demand from the set of usage entities associated with a billing period.

        This is not very straightforward for NVEnergy; demand charges from bills show up in a variety of configurations.
        The main confounding issue with this utility is facility charges. A facility charge on a given statement is
        based on the maximum demand over some trailing period of months (so that if you have a heavy peak one month, you
        will be penalized for several months into the future). This is distinct from the demand charge on a given bill.
        However, Urjanet is not very consistent when it comes to representing facility charges, and in many cases they
        are indistinguishable from demand charges. Thus there is some risk here of representing the facility charge as
        the demand charge. We try to filter out facility charges based on some simple heuristics, but this is not
        guaranteed to succeed for all bills.
        """

        # Collect demand measurements from the set of usages, attempting to filter out facility charges
        # In recent history, Urjanet has been labelling facility charges with "FAC" in the UsageActualName field.
        candidate_demand_usages = [
            u
            for u in self.iter_unique_usages()
            if u.MeasurementType == "demand"
            and u.EnergyUnit == "kW"
            and "fac" not in u.UsageActualName.lower()
        ]

        if candidate_demand_usages:
            return max([x.UsageAmount for x in candidate_demand_usages])

        # Note: this function returns 0 when no demand peak is found, opposed to 'None'.
        # This is a post-condition on the parent method. Otherwise, we fail in production
        # when posting to webapps.
        return Decimal(0)


class NVEnergyTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> NVEnergyBillingPeriod:
        return NVEnergyBillingPeriod(account)

    def bill_history(self, urja_data: UrjanetData) -> DateIntervalTree:
        filtered_accounts = self.filtered_accounts(urja_data)
        ordered_accounts = self.ordered_accounts(filtered_accounts)

        # For each account, create a billing period, taking care to detect overlaps
        # (e.g. in the case that a correction bill in issued)
        bill_history = DateIntervalTree()
        for account in ordered_accounts:
            # If no meters associated with the account, this might be the final bill.
            resource = account.meters[0] if account.meters else account
            if bill_history.overlaps(resource.IntervalStart, resource.IntervalEnd):
                log.debug(
                    "Skipping overlapping billing period: meter_pk={}, start={}, end={}".format(
                        resource.PK, resource.IntervalStart, resource.IntervalEnd
                    )
                )
            else:
                """
                NVE bills are issued with overlapping date ranges:
                    Nov 30, 2019 to Dec 31, 2019 31 days
                    Dec 31, 2019 to Jan 31, 2020 31 days

                Comparison with interval data shows that data matches the calendar month;
                adjust the start date.
                """
                bill_history.add(
                    resource.IntervalStart + timedelta(days=1),
                    resource.IntervalEnd,
                    self.billing_period(account),
                )

        return bill_history
