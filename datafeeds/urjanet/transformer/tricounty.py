from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)


class TricountyBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        # Account.NewCharges is not set; collect from charges instead
        return sum([charge.ChargeAmount for charge in self.iter_charges()])


class TricountyTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> TricountyBillingPeriod:
        return TricountyBillingPeriod(account)
