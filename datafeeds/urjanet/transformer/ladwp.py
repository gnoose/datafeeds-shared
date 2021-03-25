import copy
import logging
from datetime import timedelta, date
from decimal import Decimal

from typing import List, Optional, Tuple, Set

from datafeeds.models import Meter
from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account, DateIntervalTree, UrjanetData
from datafeeds.urjanet.transformer.base import log_generic_billing_periods


log = logging.getLogger(__name__)


class LADWPBillingPeriod(GenericBillingPeriod):
    def __init__(self, account: Account):
        self.account = account

    def iter_unique_usages(
        self,
        unit: Optional[str] = None,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ):
        """Yield a set of unique usage readings for this billing period

        If a bill has multiple service types on it (e.g. water and sewer), usage readings show up
        multiple times in the database. This function attempts to filter out those duplicates.

        Sometimes multiple billing periods show up on one statement.
        PDF: https://sources.o2.urjanet.net/sourcewithhttpbasicauth?id=1e9ce461-3fef-d813-ab99-0e73509642c4
        shows two billing ranges: 2018-08-27 - 2018-09-26 and 2018-09-27 - 2018-10-26

        IntervalStart is inconsistent in Usage:
        mysql> select PK, IntervalStart, IntervalEnd, UsageActualName, RateComponent, EnergyUnit, UsageAmount
        from `Usage` where MeterFK=20292611 and EnergyUnit='kWh'
        order by IntervalStart, UsageActualName, EnergyUnit;
        +----------+---------------+-------------+-----------------+---------------+------------+-------------+
        | PK       | IntervalStart | IntervalEnd | UsageActualName | RateComponent | EnergyUnit | UsageAmount |
        +----------+---------------+-------------+-----------------+---------------+------------+-------------+
        | 73074594 | 2018-08-27    | 2018-10-26  |                 | [total]       | kWh        |  44960.0000 |
        | 73074595 | 2018-08-28    | 2018-09-26  | Base kWh        | [off_peak]    | kWh        |  12800.0000 |
        | 73074600 | 2018-08-28    | 2018-09-26  | High Peak kWh   | [on_peak]     | kWh        |   6080.0000 |
        | 73074598 | 2018-08-28    | 2018-09-26  | Low Peak kWh    | [mid_peak]    | kWh        |   6240.0000 |
        | 73074596 | 2018-09-27    | 2018-10-26  | Base kWh        | [off_peak]    | kWh        |   9920.0000 |
        | 73074599 | 2018-09-27    | 2018-10-26  | High Peak kWh   | [on_peak]     | kWh        |   4800.0000 |
        | 73074597 | 2018-09-27    | 2018-10-26  | Low Peak kWh    | [mid_peak]    | kWh        |   5120.0000 |
        +----------+---------------+-------------+-----------------+---------------+------------+-------------+

        If start/end date are set, return usages that match that range, +=1 day; otherwise return
        all.
        Sometimes the date range spans the whole statement; may want to get all de-duped instead of
        filtering by date range.

        mysql> select PK, IntervalStart, IntervalEnd, UsageActualName, RateComponent, EnergyUnit, UsageAmount
        from `Usage`
        where MeterFK=20292611 and EnergyUnit='kW' order by IntervalStart, UsageActualName, EnergyUnit;
        +----------+---------------+-------------+-----------------+---------------+------------+-------------+
        | PK       | IntervalStart | IntervalEnd | UsageActualName | RateComponent | EnergyUnit | UsageAmount |
        +----------+---------------+-------------+-----------------+---------------+------------+-------------+
        | 73074608 | 2018-08-27    | 2018-10-26  | Base kW         | [off_peak]    | kW         |     83.2000 |
        | 73074609 | 2018-08-27    | 2018-10-26  | Base kW         | [off_peak]    | kW         |     83.2000 |
        | 73074612 | 2018-08-27    | 2018-10-26  | High Peak kW    | [on_peak]     | kW         |     92.8000 |
        | 73074613 | 2018-08-27    | 2018-10-26  | High Peak kW    | [on_peak]     | kW         |     92.8000 |
        | 73074610 | 2018-08-27    | 2018-10-26  | Low Peak kW     | [mid_peak]    | kW         |     91.2000 |
        | 73074611 | 2018-08-27    | 2018-10-26  | Low Peak kW     | [mid_peak]    | kW         |     91.2000 |
        +----------+---------------+-------------+-----------------+---------------+------------+-------------+
        """
        seen: Set[Tuple] = set()
        log.debug("get usages for %s - %s", start, end)
        for meter in self.account.meters:
            for usage in meter.usages:
                if usage.UsageAmount == 0.0:
                    continue
                if unit and usage.EnergyUnit != unit:
                    continue
                if start and abs(usage.IntervalStart - start).days >= 1:
                    continue
                if end and abs(usage.IntervalEnd - end).days >= 1:
                    continue
                key = (
                    usage.UsageAmount,
                    usage.EnergyUnit,
                    usage.RateComponent,
                    usage.IntervalStart,
                    usage.IntervalEnd,
                )
                if key not in seen:
                    seen.add(key)
                    yield usage

    def get_total_charge(self):
        """Sum charges, excluding floating charges.

        | 232215214 | Electric Service Charges - Over Estimated Read CANCELLED AMOUNT | -233576.7500 |
        | 237036447 | Deposit Charges *          |    3500.0000 |
        | 237036448 | Electric Start Service Fee |      19.0000 |
        | 230630634 | Total Corrections                                                   |  -34555.3000 |
        | 230630635 | Electric Service Charges - Previous Estimated Read CANCELLED AMOUNT |   16578.6900 |
        | 230630636 | Electric Service Charges - Previous Estimated Read CANCELLED AMOUNT |   17976.6100 |
        | 237691231 | Sewer Service Charge* 85.56000 HCF x $5.44/HCF |     465.4500 |
        | 237691232 | Sewer Service Low Income Surcharge*            |       2.3500 |
        | 230397478 | | 10/9/13 Electric Late Payment Charge         |     371.6800 |
        """
        charges = Decimal(0.0)
        for meter in self.account.meters:
            charges += sum([charge.ChargeAmount for charge in meter.charges])
        return charges

    def total_or_line_items(self, start: Optional[date], end: Optional[date]):
        """On statements with multiple billing periods, the total sometimes includes both periods.

        If the total is much higher than the line item sum, prefer the line item sum.
        mysql> select PK, IntervalStart, IntervalEnd, UsageActualName, RateComponent, UsageAmount
        from `Usage`
        where MeterFK=20141293 and EnergyUnit='kWh'
        order by IntervalStart, UsageActualName, EnergyUnit;
        +----------+---------------+-------------+-----------------+---------------+-------------+
        | PK       | IntervalStart | IntervalEnd | UsageActualName | RateComponent | UsageAmount |
        +----------+---------------+-------------+-----------------+---------------+-------------+
        | 72609082 | 2018-07-07    | 2018-08-06  | Base kWh        | [off_peak]    |  50640.0000 |
        | 72609086 | 2018-07-07    | 2018-08-06  | High Peak kWh   | [on_peak]     |  20880.0000 |
        | 72609084 | 2018-07-07    | 2018-08-06  | Low Peak kWh    | [mid_peak]    |  26400.0000 |
        | 72609080 | 2018-08-06    | 2018-09-06  |                 | [total]       | 192960.0000 |
        | 72609081 | 2018-08-07    | 2018-09-06  | Base kWh        | [off_peak]    |  47760.0000 |
        | 72609085 | 2018-08-07    | 2018-09-06  | High Peak kWh   | [on_peak]     |  20640.0000 |
        | 72609083 | 2018-08-07    | 2018-09-06  | Low Peak kWh    | [mid_peak]    |  26640.0000 |
        +----------+---------------+-------------+-----------------+---------------+-------------+
        """
        total = Decimal(0.0)
        line_items = Decimal(0.0)
        for usage in self.iter_unique_usages("kWh", start, end):
            if usage.RateComponent == "[total]":
                total += usage.UsageAmount
            else:
                line_items += usage.UsageAmount
        if line_items == 0.0:
            return total
        if total == 0.0:
            return line_items
        variance = int((total - line_items) / line_items * 100)
        log.debug("\ttotal=%s line_items=%s variance=%s", total, line_items, variance)
        if variance < 50:
            return total
        else:
            log.debug(
                "\tusing line items sum %s instead of total %s", line_items, total
            )
            return line_items

    def get_total_usage(self) -> Decimal:
        meter = self.account.meters[0]
        log.debug("get_total_usage %s - %s", meter.IntervalStart, meter.IntervalEnd)
        usage = self.total_or_line_items(meter.IntervalStart, meter.IntervalEnd)
        if usage != 0.0:
            return usage
        """
        if usage not found, try with a relaxed date range; this bill contains two billing periods
        but the date range for the total is includes only the first one:
        mysql> select PK, IntervalStart, IntervalEnd, UsageActualName, RateComponent, UsageAmount
        from `Usage`
        where MeterFK=19729463 and EnergyUnit='kWh'
        order by IntervalStart, UsageActualName, EnergyUnit;
        +----------+---------------+-------------+-----------------+---------------+-------------+
        | PK       | IntervalStart | IntervalEnd | UsageActualName | RateComponent | UsageAmount |
        +----------+---------------+-------------+-----------------+---------------+-------------+
        | 70875367 | 2015-09-01    | 2015-09-11  |                 | [total]       |   7360.0000 |
        | 70875370 | 2015-09-01    | 2015-09-11  | Base kWh        | [off_peak]    |   3720.0000 |
        | 70875369 | 2015-09-01    | 2015-09-11  | High Peak kWh   | [on_peak]     |   1920.0000 |
        | 70875368 | 2015-09-01    | 2015-09-11  | Low Peak kWh    | [mid_peak]    |   1720.0000 |
        +----------+---------------+-------------+-----------------+---------------+-------------+
        """
        return self.total_or_line_items(None, None)

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
        meter = self.account.meters[0]
        peak = Decimal(-1)  # want to be able to tell if it was set
        for usage in self.iter_unique_usages(
            "kW", meter.IntervalStart, meter.IntervalEnd
        ):
            peak = max(peak, usage.UsageAmount)
        if peak >= 0.0:
            return peak
        """
        if peak not found, try with a relaxed date range; this bill contains two billing periods
        and should have one IntervalStart - IntervalEnd range for each:
          2018-08-27    | 2018-09-26
          2018-09-27   | 2018-10-26
        """
        for usage in self.iter_unique_usages("kW"):
            peak = max(peak, usage.UsageAmount)
        return peak if peak >= 0.0 else None


class LADWPTransformer(UrjanetGridiumTransformer):
    def copy_account_data(self, account: Account, meter: Meter, start: date, end: date):
        """Copy charges and usages from an account to a new object.

        Use this to create multiple billing periods where Urjanet incorrectly combines them.
        Match on start date += 1.
        """
        account_copy = copy.copy(account)
        orig_meters = [m for m in account_copy.meters if m.PK == meter.PK]
        if not orig_meters:
            return account_copy
        meter_copy = orig_meters[0]
        # set the meter date range
        meter_copy.IntervalStart = start
        meter_copy.IntervalEnd = end
        # keep all the usages because some might have incorrect date ranges; sort this out when
        # iterating over them in the BillingPeriod
        # haven't seen the date range issue with charges (yet), so keep only matching
        meter_copy.charges = []
        for charge in meter.charges:
            if charge.IntervalStart == start and charge.IntervalEnd == end:
                meter_copy.charges.append(charge)
        return account_copy

    @staticmethod
    def billing_period(account: Account) -> LADWPBillingPeriod:
        return LADWPBillingPeriod(account)

    def bill_history(self, urja_data: UrjanetData) -> DateIntervalTree:
        """LADWP has two versions of where end dates fall: inclusive and exclusive.

        Currently, the end date is exclusive. The shift_endpoints will fix up the
        dates as needed within a series, but we also need to exclude the date of the
        latest bill.
        """
        filtered_accounts = self.filtered_accounts(urja_data)
        ordered_accounts = self.ordered_accounts(filtered_accounts)

        # For each account, create a billing period, taking care to detect overlaps
        # (e.g. in the case that a correction bill in issued)
        bill_history = DateIntervalTree()
        for idx, account in enumerate(ordered_accounts):
            start_date = account.IntervalStart
            end_date = (
                account.IntervalEnd - timedelta(days=1)
                if idx == 0
                else account.IntervalEnd
            )
            # account.meters is the list of bill parts that apply to this Gridium meter
            # if there aren't any, this bill doesn't apply; skip it
            if not account.meters:
                log.debug(
                    "Skipping billing period; no data for this meter: account_pk={}, start={}, "
                    "end={}".format(
                        account.PK, account.IntervalStart, account.IntervalEnd
                    )
                )
                continue
            meter = account.meters[0]
            if (
                bill_history.overlaps(start_date, end_date)
                and meter.IntervalEnd > meter.IntervalStart
            ):
                # try using date range from Meter instead
                log.debug(
                    "Account date range overlaps ({} - {}); trying Meter ({} - {})".format(
                        start_date, end_date, meter.IntervalStart, meter.IntervalEnd
                    )
                )
                start_date = meter.IntervalStart
                end_date = meter.IntervalEnd

            if bill_history.overlaps(start_date, end_date):
                log.debug(
                    "Skipping overlapping billing period: account_pk={}, start={}, end={}".format(
                        account.PK, start_date, end_date
                    )
                )
                continue
            # can be a correction or multiple billing periods on one statement
            # get billing periods from charges -- dates on first half of usages spans the whole statement
            # but don't create single day periods
            if (end_date - start_date).days > 45:
                log.debug(
                    "Splitting long billing period: %s - %s",
                    start_date,
                    end_date,
                )
                for meter in account.meters:
                    seen: Set[Tuple] = set()
                    # add the long billing period we're trying to split to seen:
                    # sometimes there's a charge with this same too-long range
                    seen.add((start_date, end_date))
                    for charge in meter.charges:
                        if (charge.IntervalStart, charge.IntervalEnd) in seen:
                            continue
                        if (charge.IntervalEnd - charge.IntervalStart).days <= 1:
                            continue
                        seen.add((charge.IntervalStart, charge.IntervalEnd))
                        log.debug(
                            "Adding billing period from charge: account={} meter={}, "
                            "charge_pk={} start={}, end={}".format(
                                account.PK,
                                meter.PK,
                                charge.PK,
                                charge.IntervalStart,
                                charge.IntervalEnd,
                            )
                        )
                        # copy the account and set the date range on the meter
                        account_copy = self.copy_account_data(
                            account, meter, charge.IntervalStart, charge.IntervalEnd
                        )
                        bill_history.add(
                            charge.IntervalStart,
                            charge.IntervalEnd,
                            LADWPBillingPeriod(account_copy),
                        )
                    # if the long range is the only one, use it
                    if {(start_date, end_date)} == seen and meter.charges:
                        charge = meter.charges[0]
                        log.debug(
                            "Adding long billing period from charges: account={} meter={}, "
                            "start={}, end={}".format(
                                account.PK,
                                meter.PK,
                                start_date,
                                end_date,
                            )
                        )
                        # copy the account and set the date range on the meter
                        account_copy = self.copy_account_data(
                            account, meter, charge.IntervalStart, charge.IntervalEnd
                        )
                        bill_history.add(
                            charge.IntervalStart,
                            charge.IntervalEnd,
                            LADWPBillingPeriod(account_copy),
                        )
            else:
                log.debug(
                    "Adding billing period: account_pk={}, start={}, end={}".format(
                        account.PK, start_date, end_date
                    )
                )
                bill_history.add(
                    start_date,
                    end_date,
                    self.billing_period(account),
                )

        # Adjust date endpoints to avoid 1-day overlaps
        bill_history = DateIntervalTree.shift_endpoints(bill_history)
        # Log the billing periods we determined
        log_generic_billing_periods(bill_history)
        return bill_history

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
        log.debug(
            "account interval range: %s to %s",
            account.IntervalStart,
            account.IntervalEnd,
        )
        if account.IntervalEnd > account.IntervalStart:
            account_range.add(account.IntervalStart, account.IntervalEnd)
        for meter in account.meters:
            meter_range = DateIntervalTree()
            log.debug(
                "meter interval range: %s to %s", meter.IntervalStart, meter.IntervalEnd
            )
            if meter.IntervalEnd > meter.IntervalStart:
                meter_range.add(meter.IntervalStart, meter.IntervalEnd)
            charge_range = DateIntervalTree()
            for charge in meter.charges:
                # don't create single day periods
                if (charge.IntervalEnd - charge.IntervalStart).days <= 1:
                    continue
                log.debug(
                    "charge %s interval range: %s to %s",
                    charge.PK,
                    charge.IntervalStart,
                    charge.IntervalEnd,
                )
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
        """Collapse short bills; accounts should be sorted descending.

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
