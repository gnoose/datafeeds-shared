from typing import List

from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.urjanet.model import (
    Account, UrjanetData
)


class FosterCityTransformer(GenericWaterTransformer):
    """A water transformer with some minor adaptations for the Foster City municipal utility"""

    # pylint: disable=no-self-use
    def filtered_accounts(self, urja_data: UrjanetData) -> List[Account]:
        # Foster city has some "degenerate" statements with no charges and with start date == end date
        # So we override this function to capture those cases
        return [
            account
            for account in urja_data.accounts
            if account.StatementDate is not None and account.IntervalStart != account.IntervalEnd
        ]
