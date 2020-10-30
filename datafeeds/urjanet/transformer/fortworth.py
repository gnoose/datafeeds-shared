from datetime import date
from typing import List

from datafeeds.urjanet.model import Account, UrjanetData
from datafeeds.urjanet.transformer import (
    GenericWaterTransformer,
    GenericWaterBillingPeriod,
)


class FortWorthBillingPeriod(GenericWaterBillingPeriod):
    def statement(self) -> date:
        return self.account.StatementDate or self.account.IntervalEnd


class FortWorthWaterTransformer(GenericWaterTransformer):
    @staticmethod
    def filtered_accounts(
        urja_data: UrjanetData,
    ) -> List[Account]:  # pylint: disable=no-self-use
        """Include all accounts; StatementDate stopped being set approximately June 2020."""
        return [account for account in urja_data.accounts]

    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        # Process the account objects in reverse order by statement date, in case there are corrections
        # StatementDate stopped being set, so fall back to IntervalEnd.
        return sorted(
            filtered_accounts,
            key=lambda x: x.StatementDate if x.StatementDate else x.IntervalEnd,
            reverse=True,
        )

    @staticmethod
    def billing_period(account: Account) -> FortWorthBillingPeriod:
        return FortWorthBillingPeriod(account)
