import logging
from datetime import date, timedelta
from typing import Tuple

from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer.generic_water import GenericWaterTransformer

log = logging.getLogger(__name__)


class ContraCostaWaterTransformer(GenericWaterTransformer):
    def get_account_period(self, account: Account) -> Tuple[date, date]:
        return account.IntervalStart, account.IntervalEnd - timedelta(days=1)
