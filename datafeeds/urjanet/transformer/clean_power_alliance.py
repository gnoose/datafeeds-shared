import logging

from datetime import timedelta

from datafeeds.urjanet.model import UrjanetData, DateIntervalTree
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.transformer.base import log_generic_billing_periods


log = logging.getLogger(__name__)


class CleanPowerAllianceTransformer(UrjanetGridiumTransformer):
    """
    Overrides UrjanetGridiumTransformer to adjust end date by one day. DateIntervalTree.shift_endpoints would largely
    take care of this, however, the last bill received would not be accurate.

    Clean Power Alliance bills overlap by one day, and the correct end date is one day earlier:
    PDF:
    5/17/19 - 6/18/19
    6/18/19 - 7/18/19
    7/18/19 - 8/16/19

    Correct:
    5/17/19 - 6/17/19
    6/18/19 - 7/17/19
    7/18/19 - 8/15/19
    """

    def bill_history(self, urja_data: UrjanetData) -> DateIntervalTree:
        filtered_accounts = self.filtered_accounts(urja_data)
        ordered_accounts = self.ordered_accounts(filtered_accounts)

        bill_history = DateIntervalTree()
        for account in ordered_accounts:
            if bill_history.overlaps(account.IntervalStart, account.IntervalEnd):
                log.debug(
                    "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                        account.PK, account.IntervalStart, account.IntervalEnd
                    )
                )
            else:
                log.debug(
                    "Adding billing period: account_pk={}, start={}, end={}".format(
                        account.PK, account.IntervalStart, account.IntervalEnd
                    )
                )
                # Clean Power Alliance End PDF dates are one day too far in the future.
                bill_history.add(
                    account.IntervalStart,
                    account.IntervalEnd - timedelta(days=1),
                    self.billing_period(account),
                )

        # Adjust date endpoints to avoid 1-day overlaps
        bill_history = DateIntervalTree.shift_endpoints(bill_history)

        # Log the billing periods we determined
        log_generic_billing_periods(bill_history)
        return bill_history
