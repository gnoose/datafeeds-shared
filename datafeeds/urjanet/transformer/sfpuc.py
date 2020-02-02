from decimal import Decimal
from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class SanFranciscoWaterBillingPeriod(GenericBillingPeriod):
    """Simple model of a SFPUC water billing period.

    This is an exceptionally simple model. Each billing period is represented by an Urjanet
    "account" object. This design is based on the limited billing data we've received so far through
    Urjanet, and might need to change as we support more SFPUC water customers.

    Currently, both water and sewer charges are included in the billing period.
    """

    def iter_usages(self):
        # We only look at water meters because some bills also have "sewer" meters that also
        # report usage, but it's just some percentage of the water meter usage and therefore
        # redundant.
        for meter in self.account.meters:
            if meter.ServiceType == "water":
                for usage in meter.usages:
                    yield usage

    def get_total_usage(self) -> Decimal:
        def filter_for_total(usage):
            return usage.RateComponent == "[total]"

        return Decimal(
            sum([u.UsageAmount for u in self.iter_usages() if filter_for_total(u)])
        )


class SanFranciscoWaterTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account,) -> SanFranciscoWaterBillingPeriod:
        return SanFranciscoWaterBillingPeriod(account)
