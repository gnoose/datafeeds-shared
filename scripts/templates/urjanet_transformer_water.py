import logging

from datafeeds.urjanet.transformer.generic_water import (
    GenericWaterTransformer,
    GenericWaterBillingPeriod,
)
from datafeeds.urjanet.model import Account

log = logging.getLogger(__name__)


class _UtilityName_BillingPeriod(GenericWaterBillingPeriod):
    # TODO: remove if not needed
    pass


class _UtilityName_Transformer(GenericWaterTransformer):
    @staticmethod
    def billing_period(account: Account,) -> _UtilityName_BillingPeriod:
        return _UtilityName_BillingPeriod(account)

    # TODO: override methods from GenericWaterTransformer as needed
