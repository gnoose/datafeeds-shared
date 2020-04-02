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
    def get_total_charge(self):
        """Get from account.NewCharges if available, else add up Charges."""
        if self.account.NewCharges:
            log.debug(
                "\t%s has NewCharges\t%s",
                self.account.IntervalStart,
                self.account.NewCharges,
            )
            return self.account.NewCharges
        charges = Decimal(0.0)
        for charge in self.iter_charges():
            charges += charge.ChargeAmount
        log.debug("\t%s summed charges\t%s", self.account.IntervalStart, charges)
        return charges

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
            if bill_history.overlaps(account.IntervalStart, account.IntervalEnd):
                log.debug(
                    "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                        account.PK, account.IntervalStart, account.IntervalEnd
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
                    account.IntervalStart + timedelta(days=1),
                    account.IntervalEnd,
                    self.billing_period(account),
                )

        return bill_history
