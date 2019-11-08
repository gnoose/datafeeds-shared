from datafeeds.urjanet.transformer import GenericBillingPeriod
from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.urjanet.model import Account


class AmericanBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        if self.account.NewCharges > 0:
            return self.account.NewCharges
        return self.account.TotalBillAmount - self.account.OutstandingBalance


class AmericanTransformer(GenericWaterTransformer):
    @staticmethod
    def billing_period(account: Account) -> AmericanBillingPeriod:
        return AmericanBillingPeriod(account)
