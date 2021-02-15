from datetime import date, timedelta
from typing import Tuple
import logging

from datafeeds.urjanet.transformer.generic_water import (
    GenericWaterTransformer,
    GenericWaterBillingPeriod,
)
from datafeeds.urjanet.model import Account

log = logging.getLogger(__name__)


class CityOfBellevueBillingPeriod(GenericWaterBillingPeriod):
    # TODO: remove if not needed
    pass


class CityOfBellevueTransformer(GenericWaterTransformer):
    @staticmethod
    def billing_period(account: Account,) -> CityOfBellevueBillingPeriod:
        return CityOfBellevueBillingPeriod(account)

    def get_account_period(self, account: Account) -> Tuple[date, date]:
        return account.IntervalStart, account.IntervalEnd - timedelta(days=1)
