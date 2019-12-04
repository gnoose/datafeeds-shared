from decimal import Decimal
from typing import List

from datafeeds.urjanet.transformer import GenericBillingPeriod
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import Account, UrjanetData
from datafeeds.urjanet.transformer.base import CONVERSIONS


class HecoBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        if self.account.NewCharges > 0:
            return self.account.NewCharges
        return self.account.TotalBillAmount - self.account.OutstandingBalance

    def get_total_usage(self) -> Decimal:
        usages = [u for u in self.iter_unique_usages()
                  if (u.MeasurementType == "general_consumption")]

        units = set(u.EnergyUnit for u in usages)
        if len(units) != 1:
            conversion = Decimal("1.0")
        else:
            unit = units.pop().lower().strip()
            conversion = CONVERSIONS.get(unit, Decimal("1.0"))

        return sum([u.UsageAmount for u in usages]) * conversion


class HecoTransformer(UrjanetGridiumTransformer):
    def filtered_accounts(self, urja_data: UrjanetData) -> List[Account]:  # pylint: disable=no-self-use
        """StatementDate is not set for some Heco"""
        return [account for account in urja_data.accounts
                if account.IntervalEnd is not None and account.IntervalStart is not None]

    @staticmethod
    def billing_period(account: Account) -> HecoBillingPeriod:
        return HecoBillingPeriod(account)

    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        """StatementDate is not set, so sort by IntervalEnd"""
        return sorted(
            filtered_accounts, key=lambda x: (x.IntervalEnd, -x.PK), reverse=True)
