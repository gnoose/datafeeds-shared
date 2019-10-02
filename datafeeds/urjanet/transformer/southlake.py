from typing import List

from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.urjanet.model import Account, UrjanetData


class SouthlakeTransformer(GenericWaterTransformer):
    def filtered_accounts(self, urja_data: UrjanetData) -> List[Account]:  # pylint: disable=no-self-use
        """StatementDate is not set for Southlake"""
        return [account for account in urja_data.accounts
                if account.IntervalEnd is not None and account.IntervalStart is not None]

    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        """StatementDate is not set, so sort by IntervalEnd"""
        return sorted(
            filtered_accounts, key=lambda x: (x.IntervalEnd, -x.PK), reverse=True)
