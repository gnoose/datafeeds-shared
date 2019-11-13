from decimal import Decimal

from datafeeds.urjanet.transformer import GenericBillingPeriod
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer.base import CONVERSIONS


class HecoBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                total_charges += charges.ChargeAmount
        return total_charges

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
    @staticmethod
    def billing_period(account: Account) -> HecoBillingPeriod:
        return HecoBillingPeriod(account)
