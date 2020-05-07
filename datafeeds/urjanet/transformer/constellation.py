from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class ConstellationBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        """Get cost by summing line items.

        Note: Summing line items is not guaranteed to give a total
        that matches the bill.  In the total history of this
        account, there has only been one discrepancy, of less than
        $2.  Since we're primarily concerned with capturing the
        cost, we'll use Urjanet's rollup.

        We can revisit this if we add more accounts for this utility
        # or more serious discrepancies show up.
        """
        return self.account.TotalBillAmount


class ConstellationTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account,) -> ConstellationBillingPeriod:
        return ConstellationBillingPeriod(account)
