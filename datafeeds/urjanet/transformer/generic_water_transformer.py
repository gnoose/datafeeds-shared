"""A Basic Urjanet water transformer for converting Water, Irrigation, and Sewer charges.

This transformer is applicable to many simple municipal utilities.
"""

import logging

from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.urjanet.model import (
    GridiumBillingPeriod,
    GridiumBillingPeriodCollection,
    UrjanetData,
)


log = logging.getLogger(__name__)


class GenericWaterTransformer(UrjanetGridiumTransformer):
    def urja_to_gridium(self, urja_data: UrjanetData) -> GridiumBillingPeriodCollection:
        """Transform Urjanet data for water bills into Gridium billing periods"""

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
                    peak_demand=None,  # No peak demand for water
                    total_usage=period_data.get_total_usage(),
                    source_urls=period_data.get_source_urls(),
                    line_items=list(period_data.iter_charges()),
                    tariff=None,
                )
            )
        return GridiumBillingPeriodCollection(periods=gridium_periods)
