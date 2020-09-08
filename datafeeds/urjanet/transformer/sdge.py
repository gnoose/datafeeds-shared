import copy
from datetime import timedelta
from decimal import Decimal
from typing import List, Optional
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

    def get_peak_demand(self) -> Optional[Decimal]:
        """Find the peak demand for this period

        Get the max ChargeUnitsUsed for charges with UsageUnit kW, or None if there are no kW
        charges.
        Exclude Capacity Reservation Demand; this charge is based on kW, but is not a usage value.
        """
        peak = Decimal(-1)  # want to be able to tell if it was set; won't be for water
        for meter in self.account.meters:
            for charges in meter.charges:
                if charges.ChargeActualName == "Capacity Reservation Demand":
                    log.info(
                        "skipping charge %s %s when calculating peak",
                        charges.PK,
                        charges.ChargeActualName,
                    )
                    continue
                if charges.UsageUnit == "kW":
                    peak = max(peak, charges.ChargeUnitsUsed)
        return peak if peak >= 0.0 else None


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

            Add +1 to the the IntervalStart for the billing period start date
            """
            start = account.IntervalStart + timedelta(days=1)
            end = account.IntervalEnd
            # account.meters is the list of bill parts that apply to this Gridium meter
            # Meter.MeterNumber like %meter.utility_service.service_id%
            # if there aren't any, this bill doesn't apply; skip it
            if not account.meters:
                log.debug(
                    "Skipping billing period; no data for this meter: account_pk={}, start={}, "
                    "end={}".format(account.PK, start, end)
                )
                continue

            if bill_history.overlaps(start, end, strict=False):
                # try getting date range from Meter
                start = account.meters[0].IntervalStart + timedelta(days=1)
                end = account.meters[0].IntervalEnd
                log.debug(
                    "Billing period overlap: {} - {}; trying with date range from usage: {} "
                    "- {}".format(
                        account.IntervalStart + timedelta(days=1),
                        account.IntervalEnd,
                        start,
                        end,
                    )
                )
                # if it still overlaps, skip it
                if bill_history.overlaps(start, end, strict=False):
                    log.debug(
                        "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                            account.PK, start, end
                        )
                    )
                    continue

            if (end - start).days > 45:
                # if bill is too long, it's likely a correction; get billing periods from usages
                for meter in account.meters:
                    for usage in [
                        u for u in meter.usages if u.RateComponent == "[total]"
                    ]:
                        log.debug(
                            "Adding billing period from usage: account_pk={}, "
                            "usage_pk={} start={}, end={}".format(
                                account.PK,
                                usage.PK,
                                usage.IntervalStart,
                                usage.IntervalEnd,
                            )
                        )
                        # copy the account; keep only the relevant charges and usages
                        account_copy = copy.copy(account)
                        start = usage.IntervalStart
                        end = usage.IntervalEnd
                        meter_copy = [
                            m for m in account_copy.meters if m.PK == meter.PK
                        ][0]
                        meter_copy.usages = [
                            u
                            for u in meter.usages
                            if u.IntervalStart == start and u.IntervalEnd == end
                        ]
                        meter_copy.charges = [
                            c
                            for c in meter.charges
                            if c.IntervalStart == start and c.IntervalEnd == end
                        ]
                        bill_history.add(
                            usage.IntervalStart
                            + timedelta(days=1),  # prevent overlapping
                            usage.IntervalEnd,
                            SDGEBillingPeriod(account_copy),
                        )
            else:
                log.debug(
                    "Adding billing period: account_pk={}, start={}, end={}".format(
                        account.PK, start, end
                    )
                )
                bill_history.add(
                    start, end, SDGEBillingPeriod(account),
                )

        return bill_history

    @staticmethod
    def billing_period(account: Account) -> SDGEBillingPeriod:
        return SDGEBillingPeriod(account)
