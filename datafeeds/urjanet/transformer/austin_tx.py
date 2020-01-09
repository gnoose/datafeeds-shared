from decimal import Decimal

from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.transformer.base import CONVERSIONS
from datafeeds.urjanet.model import Account


class AustinTXBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                if charges.ChargeId == "ch.late_fee":
                    continue
                total_charges += charges.ChargeAmount
        return total_charges

    def get_total_usage(self) -> Decimal:
        """Get total usage from Usage records. Exclude solar by matching on name."""
        usages = [
            u
            for u in self.iter_unique_usages()
            if u.RateComponent == "[total]"
            and "Total Generation" not in u.UsageActualName
        ]

        units = set(u.EnergyUnit for u in usages)
        if len(units) != 1:
            conversion = Decimal("1.0")
        else:
            unit = units.pop().lower().strip()
            conversion = CONVERSIONS.get(unit, Decimal("1.0"))
        return sum([u.UsageAmount for u in usages]) * conversion


class AustinTXTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> AustinTXBillingPeriod:
        return AustinTXBillingPeriod(account)
