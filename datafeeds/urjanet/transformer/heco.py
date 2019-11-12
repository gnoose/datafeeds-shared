from datafeeds.urjanet.transformer import GenericBillingPeriod
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import Account


class HecoBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                total_charges += charges.ChargeAmount
        return total_charges


class HecoTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> HecoBillingPeriod:
        return HecoBillingPeriod(account)
