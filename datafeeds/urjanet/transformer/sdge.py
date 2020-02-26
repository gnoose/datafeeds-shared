from datetime import timedelta
from typing import List

from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import Account


class SDGETransformer(UrjanetGridiumTransformer):
    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        """Process the account objects in reverse order by statement date.

        SDGE bills are end date inclusive, meaning that the previous bill period ends on
        the same day the next period begins. Adjust for it.
        """
        accounts = []
        for account in sorted(
            filtered_accounts, key=lambda x: x.StatementDate, reverse=True
        ):
            account.IntervalEnd -= timedelta(days=1)
            accounts.append(account)
        return accounts
