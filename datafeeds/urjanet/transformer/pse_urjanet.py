from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account
from decimal import Decimal


class PSEUrjanetBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                total_charges += charges.ChargeAmount
        return total_charges

    def get_total_usage(self) -> Decimal:
        """Return the total usage for this period
        We look for a usage element with RateComponent == '[total]'
        """

        def filter_for_total(usage):
            return usage.RateComponent == "[total]" and usage.EnergyUnit in [
                "kWh",
                "therms",
            ]

        return Decimal(
            sum(
                [
                    u.UsageAmount
                    for u in self.iter_unique_usages()
                    if filter_for_total(u)
                ]
            )
        )


class PseUrjanetTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> PSEUrjanetBillingPeriod:
        return PSEUrjanetBillingPeriod(account)
