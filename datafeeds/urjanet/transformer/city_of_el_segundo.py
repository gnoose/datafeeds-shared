from datetime import date, timedelta
from typing import Tuple
import logging

from datafeeds.urjanet.transformer.generic_water import GenericWaterTransformer
from datafeeds.urjanet.model import Account

log = logging.getLogger(__name__)


class CityOfElSegundoTransformer(GenericWaterTransformer):
    def get_account_period(self, account: Account) -> Tuple[date, date]:
        account.IntervalEnd -= timedelta(days=1)
        return account.IntervalStart, account.IntervalEnd
