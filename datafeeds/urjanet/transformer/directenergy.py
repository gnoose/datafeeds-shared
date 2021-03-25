from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class DirectEnergyBillingPeriod(GenericBillingPeriod):
    pass  # TODO: remove if not needed


class DirectEnergyTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(
        account: Account,
    ) -> DirectEnergyBillingPeriod:
        return DirectEnergyBillingPeriod(account)
