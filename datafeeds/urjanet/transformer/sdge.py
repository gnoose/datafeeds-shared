from typing import List
import logging

from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import Account, DateIntervalTree, UrjanetData

log = logging.getLogger(__name__)


class SDGETransformer(UrjanetGridiumTransformer):
    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        """Process the account objects in reverse order by statement date.

        SDGE bills are end date inclusive, meaning that the previous bill period ends on
        the same day the next period begins.
        """
        accounts = []
        for account in sorted(
            filtered_accounts, key=lambda x: x.StatementDate, reverse=True
        ):
            accounts.append(account)
        return accounts

    def bill_history(self, urja_data: UrjanetData) -> DateIntervalTree:
        filtered_accounts = self.filtered_accounts(urja_data)
        ordered_accounts = self.ordered_accounts(filtered_accounts)

        # For each account, create a billing period, taking care to detect overlaps
        # (e.g. in the case that a correction bill in issued)
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
                bill_history.add(
                    account.IntervalStart,
                    account.IntervalEnd,
                    self.billing_period(account),
                )

        # Do not adjust date endpoints to avoid 1-day overlaps
        # bill_history = DateIntervalTree.shift_endpoints(bill_history)

        return bill_history
