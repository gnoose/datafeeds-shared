from decimal import Decimal

from datafeeds.urjanet.transformer import GenericWaterBillingPeriod
from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.urjanet.model import (
    Account,
    GridiumBillingPeriod,
    GridiumBillingPeriodCollection,
    UrjanetData
)


class AustinTXBillingPeriod(GenericWaterBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                total_charges += charges.ChargeAmount
        return total_charges

    def get_peak_demand(self) -> Decimal:
        """Find the peak demand for this period

        Get the max ChargeUnitsUsed for charges with UsageUnit kW
        """
        peak = -1  # want to be able to tell if it was set; won't be for water
        for meter in self.account.meters:
            for charges in meter.charges:
                if charges.UsageUnit == "kW":
                    peak = max(peak, charges.ChargeUnitsUsed)
        return peak if peak >= 0.0 else None


class AustinTXTransformer(GenericWaterTransformer):

    @staticmethod
    def billing_period(account: Account) -> AustinTXBillingPeriod:
        return AustinTXBillingPeriod(account)

    def urja_to_gridium(self, urja_data: UrjanetData) -> GridiumBillingPeriodCollection:
        """Transform Urjanet data for bills into Gridium billing periods.

        Get peak demand for periods, since bills may be for electric meters.
        """

        bill_history = self.bill_history(urja_data)
        # Compute the final set of gridium billing periods
        gridium_periods = []
        for ival in sorted(bill_history.intervals()):
            period_data = ival.data
            gridium_periods.append(
                GridiumBillingPeriod(
                    start=ival.begin,
                    end=ival.end,
                    total_charge=period_data.get_total_charge(),
                    peak_demand=period_data.get_peak_demand(),
                    total_usage=period_data.get_total_usage(),
                    source_urls=period_data.get_source_urls(),
                    line_items=list(period_data.iter_charges()),
                    tariff=None))
        return GridiumBillingPeriodCollection(periods=gridium_periods)
