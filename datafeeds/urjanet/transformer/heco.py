from datafeeds.urjanet.transformer import GenericWaterBillingPeriod
from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.urjanet.model import Account


class HecoBillingPeriod(GenericWaterBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                total_charges += charges.ChargeAmount
        return total_charges


class HecoTransformer(GenericWaterTransformer):
    @staticmethod
    def billing_period(account: Account) -> HecoBillingPeriod:
        return HecoBillingPeriod(account)
