from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class SanFranciscoWaterBillingPeriod(GenericBillingPeriod):
    pass  # TODO: remove if not needed


class SanFranciscoWaterTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account,) -> SanFranciscoWaterBillingPeriod:
        return SanFranciscoWaterBillingPeriod(account)
