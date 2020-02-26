from datetime import timedelta
from decimal import Decimal
from typing import List, Optional

from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer import (
    UrjanetGridiumTransformer,
    GenericBillingPeriod,
)


class NationalGridBillingPeriod(GenericBillingPeriod):
    def get_peak_demand(self) -> Optional[Decimal]:
        """Find the peak demand for this period

        Get the max UsageAmount for charges with UsageUnit kW, or None if there are no kW
        charges.
        """
        peak = Decimal(-1)  # want to be able to tell if it was set; won't be for water
        for meter in self.account.meters:
            for usage in meter.usages:
                if usage.EnergyUnit == "kW":
                    peak = max(peak, usage.UsageAmount)
        return peak if peak >= 0.0 else None


class NationalGridTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account,) -> NationalGridBillingPeriod:
        return NationalGridBillingPeriod(account)

    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        """Process the account objects in reverse order by statement date.

        NationalGrid billing periods overlap: adjust the start date
        """
        accounts = []
        for account in sorted(
            filtered_accounts, key=lambda x: x.StatementDate, reverse=True
        ):
            account.IntervalStart += timedelta(days=1)
            accounts.append(account)
        return accounts

    def get_total_charge(self):
        statement = sorted(self.filtered_accounts, key=lambda x: x.StatementDate)[-1]
        stype = statement["StatementType"]
        if stype == "statement_type_bill":
            # This is a normal bill, so the charge for power is total cost minus whatever was
            # owed before.
            return statement.TotalBillAmount - statement.OutstandingBalance
        elif stype == "statement_type_adjustment":
            # When the statement is a correction, the outstanding
            # balance field is never right, because it reflects what
            # was owed on the incorrect prior bill.
            return statement.TotalBillAmount
        raise Exception("Found unexpected statement type: %s" % str(stype))
