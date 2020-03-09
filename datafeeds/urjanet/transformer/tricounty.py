from datafeeds.urjanet.model import Account
from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)


class TriCountyBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        """Account.NewCharges is not set; collect from charges instead."""
        return sum([charge.ChargeAmount for charge in self.iter_charges()])

    def iter_charges(self):
        """Get only meter charges, not floating charges."""
        for meter in self.account.meters:
            for charge in meter.charges:
                yield charge


class TriCountyTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> TriCountyBillingPeriod:
        return TriCountyBillingPeriod(account)
