from datetime import date
import logging
from typing import Tuple

from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer import GenericWaterTransformer

log = logging.getLogger(__name__)


class SanDiegoWaterTransformer(GenericWaterTransformer):
    def get_account_period(self, account: Account) -> Tuple[date, date]:
        """Get account period from a meter record with a MeterNumber.

        Some accounts (statements) contain multiple sections with overlapping date ranges:

        Account 2020-07-11    | 2020-08-13
           1610820 07-11-20 08-11-20
           1610822 07-11-20 08-11-20
           (blank) 07-15-20 08-13-20

        Account 2020-08-12    | 2020-09-14
           1610820 08-12-20 09-10-20
           1610822 08-12-20 09-10-20
           (blank) 08-14-20 09-14-20

        The Account IntervalStart, IntervalEnd range will encompass all, but this causes overlapping bill periods
        when a later one starts before the end of the previous one. Use the MeterNumber date range if available,
        otherwise the Account range.
        """
        for meter in account.meters:
            if meter.MeterNumber:
                log.info(
                    "using data range from MeterNumber %s: %s - %s",
                    meter.MeterNumber,
                    meter.IntervalStart,
                    meter.IntervalEnd,
                )
                return meter.IntervalStart, meter.IntervalEnd
        return account.IntervalStart, account.IntervalEnd
