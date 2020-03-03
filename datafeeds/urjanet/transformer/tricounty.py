from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class TriCountyBillingPeriod(GenericBillingPeriod):
    pass  # TODO: remove if not needed


class TriCountyTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account,) -> TriCountyBillingPeriod:
        return TriCountyBillingPeriod(account)
