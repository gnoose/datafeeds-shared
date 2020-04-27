from datetime import timedelta
from typing import List
import logging

from datafeeds.urjanet.transformer import (
    UrjanetGridiumTransformer,
    GenericBillingPeriod,
)
from datafeeds.urjanet.model import Account, DateIntervalTree, UrjanetData

log = logging.getLogger(__name__)


class SDGEBillingPeriod(GenericBillingPeriod):
    def get_total_charge(self):
        total_charges = 0
        for meter in self.account.meters:
            for charges in meter.charges:
                if charges.ChargeId == "ch.late_fee":
                    continue
                total_charges += charges.ChargeAmount
        return total_charges


class SDGETransformer(UrjanetGridiumTransformer):
    @staticmethod
    def ordered_accounts(filtered_accounts: List[Account]) -> List[Account]:
        """Process the account objects in reverse order by statement date.

        SDGE bills are end date inclusive, meaning that the previous bill period ends on
        the same day the next period begins.
        """
        accounts = []
        for account in sorted(
            filtered_accounts, key=lambda x: x.StatementDate, reverse=True
        ):
            accounts.append(account)
        return accounts

    def bill_history(self, urja_data: UrjanetData) -> DateIntervalTree:
        filtered_accounts = self.filtered_accounts(urja_data)
        ordered_accounts = self.ordered_accounts(filtered_accounts)

        # For each account, create a billing period, taking care to detect overlaps
        # (e.g. in the case that a correction bill in issued)
        bill_history = DateIntervalTree()
        for account in ordered_accounts:
            if bill_history.overlaps(account.IntervalStart, account.IntervalEnd):
                log.debug(
                    "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                        account.PK, account.IntervalStart, account.IntervalEnd
                    )
                )
            else:
                log.debug(
                    "Adding billing period: account_pk={}, start={}, end={}".format(
                        account.PK, account.IntervalStart, account.IntervalEnd
                    )
                )
                """
                SDGE bills are issued with overlapping date ranges:
                    Billing Period: 9/30/19 - 10/31/19 Total Days: 31
                    Billing Period: 10/31/19 - 11/30/19 Total Days: 30

                Comparing one meter with interval data:
                    select sum(reading::text::decimal)/4
                    from (
                        select json_array_elements(mr.readings) reading
                        from meter_reading mr
                        where meter=1971049865238 and occurred > '2019-09-30' and occurred <= '2019-10-31'
                    ) r;

                The interval data for 2019-10-01 - 2019-10-31 closely matches the bill;
                2019-09-30 - 2019-10-30 does not.
                """
                bill_history.add(
                    account.IntervalStart + timedelta(days=1),
                    account.IntervalEnd,
                    SDGEBillingPeriod(account),
                )

        return bill_history

    @staticmethod
    def billing_period(account: Account) -> SDGEBillingPeriod:
        return SDGEBillingPeriod(account)
