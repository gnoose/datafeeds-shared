import logging
from decimal import Decimal

from typing import List, Optional

from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account, DateIntervalTree

log = logging.getLogger(__name__)


class LADWPBillingPeriod(GenericBillingPeriod):
    def __init__(self, account: Account):
        self.account = account

    def get_total_charge(self):
        return sum([c.ChargeAmount for c in self.iter_charges()])

    def get_total_usage(self) -> Decimal:
        total = Decimal(0.0)
        for usage in self.iter_unique_usages():
            if usage.RateComponent == "[total]" and usage.EnergyUnit == "kWh":
                total += usage.UsageAmount
        return total

    def get_peak_demand(self) -> Optional[Decimal]:
        """Find the peak demand for this period

        LADWP tracks multiple peaks; for now, just get the highest.

        mysql> select PK, EnergyUnit, UsageActualName, RateComponent, UsageAmount from `Usage`
        where MeterFK=20384264 and EnergyUnit='kW';
        +----------+------------+-----------------+---------------+-------------+
        | PK       | EnergyUnit | UsageActualName | RateComponent | UsageAmount |
        +----------+------------+-----------------+---------------+-------------+
        | 73372283 | kW         | High Peak kW    | [on_peak]     |    235.2000 |
        | 73372284 | kW         | Low Peak kW     | [mid_peak]    |    273.6000 |
        | 73372285 | kW         | Base kW         | [off_peak]    |    232.8000 |
        +----------+------------+-----------------+---------------+-------------+
        """
        peak = Decimal(-1)  # want to be able to tell if it was set
        for usage in self.iter_unique_usages():
            if usage.EnergyUnit == "kW":
                peak = max(peak, usage.UsageAmount)
        return peak if peak >= 0.0 else None


class LADWPTransformer(UrjanetGridiumTransformer):
    @staticmethod
    def billing_period(account: Account) -> LADWPBillingPeriod:
        return LADWPBillingPeriod(account)

    @staticmethod
    def update_date_range_from_charges(account: Account) -> Account:
        """Fix date range for bills that cross the winter/summer boundary.

        When a bill crosses the winter/summary boundary (9/1), charges are reported in two
        batches: the summer portion and the winter portion. The account and meter IntervalStart
        and IntervalEnd may encompass just one of these date ranges; fix if needed.

        Summer/winter example:
        meter oid 1707479190338
        +-----------+---------------+-------------+----------+
        | accountPK | IntervalStart | IntervalEnd | meterPK  |
        +-----------+---------------+-------------+----------+
        |   5494320 | 2015-09-01    | 2015-09-11  | 19729463 |
        |   5498442 | 2015-09-11    | 2015-10-09  | 19740313 |

        PDF (https://sources.o2.urjanet.net/sourcewithhttpbasicauth?id=1e55ab22-7795-d6a4
        -a229-22000b849d83)
        has two two sections for charges:
          - 8/13/15 - 8/31/15 (summer)
          - 9/1/15 - 9/11/15 (winter)
        Meter record has IntervalStart = 9/1/15 and IntervalEnd = 9/1/15
        The Charge records have IntervalStart and IntervalEnd for both date ranges.
        """
        account_range = DateIntervalTree()
        account_range.add(account.IntervalStart, account.IntervalEnd)
        for meter in account.meters:
            meter_range = DateIntervalTree()
            meter_range.add(meter.IntervalStart, meter.IntervalEnd)
            charge_range = DateIntervalTree()
            for charge in meter.charges:
                charge_range.add(charge.IntervalStart, charge.IntervalEnd)
            if len(charge_range.intervals()) > 1:
                min_charge_dt = min([r.begin for r in charge_range.intervals()])
                max_charge_dt = max([r.end for r in charge_range.intervals()])
                log.debug(
                    "Updating meter date range from charges to %s - %s (was %s - %s)",
                    min(meter.IntervalStart, min_charge_dt),
                    max(account.IntervalEnd, max_charge_dt),
                    meter.IntervalStart,
                    meter.IntervalEnd,
                )
                meter.IntervalStart = min(meter.IntervalStart, min_charge_dt)
                meter.IntervalEnd = max(meter.IntervalEnd, max_charge_dt)
                log.debug(
                    "Updating account date range from charges to %s - %s (was %s - %s)",
                    min(account.IntervalStart, min_charge_dt),
                    max(account.IntervalEnd, max_charge_dt),
                    account.IntervalStart,
                    account.IntervalEnd,
                )
                account.IntervalStart = min(account.IntervalStart, min_charge_dt)
                account.IntervalEnd = max(account.IntervalEnd, max_charge_dt)
        return account

    @staticmethod
    def ordered_accounts(accounts: List[Account]) -> List[Account]:
        """Collapse short bills. accounts should be sorted descending.

        LADWP once had a massive billing screw
        up, which means we had to get really complicated in terms of resolving
        when bills actually happened. To accomplish this we don't use the
        statement IntervalStart-IntervalEnd (these spanned several months)
        during the cock-up. Instead we pull the actual line items and use their
        IntervalStart-IntervalEnd to derive the correct bill periods. This
        works EXCEPT for when we run into the case where LADWP splits billing
        across summer/winter rates. In thise case we get two bills because
        the line item start/end is split along those boundaries.

        We don't have much to work with here in clues as to when this happens.
        What we're going to do is look for bills less than 28 days, if we find
        one look on both sides and find the shorter period and then combine those
        two bills into one. This should fix the issue (hopefully).
        """
        rval = []
        for idx, account in enumerate(
            sorted(accounts, key=lambda x: x.StatementDate, reverse=True)
        ):
            current_bill = LADWPTransformer.update_date_range_from_charges(account)
            current_duration = (
                current_bill.IntervalEnd - current_bill.IntervalStart
            ).days
            if current_duration >= 27:
                rval.append(current_bill)
                continue

            prev_bill = None
            prev_duration = None
            if (
                idx < len(accounts) - 1
            ):  # we are reverse sorted, so previous bill comes  AFTER
                prev_bill = accounts[idx + 1]
                prev_duration = (prev_bill.IntervalEnd - prev_bill.IntervalStart).days

            next_bill = None
            next_duration = None
            if idx > 0:
                next_bill = accounts[idx - 1]
                next_duration = (next_bill.IntervalEnd - next_bill.IntervalStart).days

            combine_bill = None
            if not prev_bill and next_bill is not None:
                combine_bill = next_bill
            elif not next_bill and prev_bill is not None:
                combine_bill = prev_bill
            elif prev_duration <= next_duration:
                combine_bill = prev_bill
            else:
                combine_bill = next_bill

            combine_duration = (
                combine_bill.IntervalEnd - combine_bill.IntervalStart
            ).days

            # we probably don't want to combine these over some amount of days
            if combine_duration + current_duration < 35:
                log.debug(
                    "Combining bill %s - %s with %s - %s",
                    current_bill.IntervalStart,
                    current_bill.IntervalEnd,
                    combine_bill.IntervalStart,
                    combine_bill.IntervalEnd,
                )

                combine_bill.IntervalEnd = max(
                    current_bill.IntervalEnd, combine_bill.IntervalEnd
                )
                combine_bill.IntervalStart = min(
                    current_bill.IntervalStart, combine_bill.IntervalStart
                )
                combine_bill.meters += current_bill.meters
                rval.append(combine_bill)
            else:
                rval.append(current_bill)  # this one turned out to be ok

        return rval
