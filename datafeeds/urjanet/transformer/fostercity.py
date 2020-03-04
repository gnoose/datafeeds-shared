from decimal import Decimal
from typing import List

from datafeeds.urjanet.model import Account, UrjanetData
from datafeeds.urjanet.transformer import (
    GenericWaterTransformer,
    GenericWaterBillingPeriod,
)


class FosterCityWaterBillingPeriod(GenericWaterBillingPeriod):
    def get_total_charge(self):
        if self.account.NewCharges > Decimal(0.0):
            return self.account.NewCharges

        if (
            self.account.NewCharges == Decimal(0.0)
            and self.account.OutstandingBalance == Decimal(0.0)
            and self.account.TotalBillAmount > Decimal(0.0)
        ):
            return self.account.TotalBillAmount


class FosterCityWaterTransformer(GenericWaterTransformer):
    """A water transformer with some minor adaptations for the Foster City municipal utility"""

    @staticmethod
    def billing_period(account: Account) -> FosterCityWaterBillingPeriod:
        return FosterCityWaterBillingPeriod(account)

    @staticmethod
    def filtered_accounts(urja_data: UrjanetData) -> List[Account]:
        # Foster city has some "degenerate" statements with no charges and with
        # start date == end date
        # So we override this function to capture those cases
        return [
            account
            for account in urja_data.accounts
            if account.StatementDate is not None
            and account.IntervalStart != account.IntervalEnd
        ]
