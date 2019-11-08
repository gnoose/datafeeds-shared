from datafeeds.urjanet.transformer import GenericBillingPeriod
from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.urjanet.model import Account


class WataugaBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        # Account.NewCharges is not set; collect from floating_charges instead
        return sum([charge.ChargeAmount for charge in self.account.floating_charges])


class WataugaTransformer(GenericWaterTransformer):
    @staticmethod
    def billing_period(account: Account) -> WataugaBillingPeriod:
        return WataugaBillingPeriod(account)
