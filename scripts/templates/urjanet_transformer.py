from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class _UtilityName_BillingPeriod(GenericBillingPeriod):
    pass  # TODO: remove if not needed


class _UtilityName_Transformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(
        account: Account,
    ) -> _UtilityName_BillingPeriod:
        return _UtilityName_BillingPeriod(account)
