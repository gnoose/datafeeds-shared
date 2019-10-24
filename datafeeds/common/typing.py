from datetime import datetime
from enum import Enum
from typing import Dict, List, NamedTuple, Optional
from collections import OrderedDict
from datetime import date, timedelta
from functools import reduce
import logging


log = logging.getLogger(__name__)


class Status(Enum):
    SUCCEEDED = 0
    FAILED = 1


class IntervalRange(NamedTuple):
    start: datetime
    end: datetime


# dict of {"2017-04-02" : [59.1, 30.2, None, ...], ...}
IntervalReadings = Dict[str, List[Optional[float]]]


class IntervalIssue(NamedTuple):
    interval_dt: datetime
    error: str
    value: float


class NonContiguousBillingDataDateRangeError(Exception):
    pass


class OverlappedBillingDataDateRangeError(Exception):
    pass


class BillingDatumItemsEntry(NamedTuple):
    description: str
    quantity: float
    rate: float
    total: float
    kind: str  # usually "demand" or "use"
    unit: str  # usually "kW" or "kWh"


# Platform bill attachments have three fields: an S3 key, a "format", and a "kind".
# The "format" field is used by platform to represent the file format of the attachment. At the time of writing,
# platform only supports the "PDF" file format type. We sometimes resort to this field to identify an attachment
# type, though we also currently look at the extension on the S3 key. The "kind" field should generally be
# "bill" ("explanation" is another option, though that isn't generally used)


class AttachmentEntry(NamedTuple):
    key: str
    format: str
    kind: str


class BillingDatum(NamedTuple):
    start: date
    end: date
    cost: float
    used: Optional[float]
    peak: Optional[float]
    items: Optional[List[BillingDatumItemsEntry]]
    attachments: Optional[List[AttachmentEntry]]


BillingData = List[BillingDatum]

BillingRange = NamedTuple("BillingRange", [("start", date), ("end", date)])


def make_billing_pdf_attachment(key):
    return [AttachmentEntry(key=key, kind="bill", format="PDF")]


def is_contiguous(bd: BillingData) -> bool:
    """A contiguous date range with neither overlaps nor gaps"""

    def comparator(acc, elem: BillingDatum):
        return elem.end \
            if acc is True or acc == elem.start - timedelta(days=1) \
            else False

    chronologically = sorted(bd, key=lambda b: b.start)

    return reduce(comparator, chronologically, True) is not False


def is_without_overlaps(bd: BillingData) -> bool:
    """A date range that's permissive of gaps but not overlaps or dupes"""

    def comparator(acc, elem: BillingDatum):
        return elem.end \
            if acc is True or (isinstance(acc, date) and acc <= elem.start - timedelta(days=1)) \
            else False

    chronologically = sorted(bd, key=lambda b: b.start)

    return reduce(comparator, chronologically, True) is not False


def _log_invalid_date_range(bd: BillingData, log) -> None:
    def overlaps_for(b):
        for c in chronologically:
            if c is b or b.end < c.start or c.end < b.start:
                continue

            yield c

    def gap_or_acc(acc, elem: BillingDatum):
        new_gap = lambda e: '%s - %s' % (fmt(acc['prev'].end), fmt(e.start))

        return {'prev': elem, 'gaps': acc['gaps'] + [new_gap(elem)]} \
            if acc['prev'] and elem.start > (acc['prev'].end + timedelta(days=1)) \
            else {'prev': elem, 'gaps': acc['gaps']}

    def full_log_msg():
        full_range_msg = '\n\nFULL RANGE:\n%s\n' % '\n'.join(periods)

        dupes_msg = '\n\nDUPLICATES:\n%s\n' % '\n'.join(
            ('%s: %s' % (k, v) for k, v in dupes.items())) \
            if dupes \
            else '\n\nDUPLICATES: 0\n'

        overlaps_msg = '\n\nOVERLAPS:\n%s\n' % '\n'.join(
            ('%s: %s' % (k, len(v)) for k, v in overlaps.items() if len(v)))

        gaps_msg = '\n\nGAPS:\n%s\n' % '\n'.join(gaps) \
            if gaps \
            else '\n\nGAPS: 0\n'

        return ('INVALID BILL HISTORY DATES!%s%s%s%s' %
                (full_range_msg, dupes_msg, gaps_msg, overlaps_msg))

    fmt = lambda d: d.strftime('%Y/%m/%d')
    fmt_range = lambda b: '%s - %s' % (fmt(b.start), fmt(b.end))

    chronologically = sorted(bd, key=fmt_range)

    periods = [fmt_range(b) for b in chronologically]

    dupes = OrderedDict(
        (p, periods.count(p)) for p in periods if periods.count(p) > 1)

    overlaps = OrderedDict(
        (fmt_range(b), list(overlaps_for(b))) for b in chronologically)

    gaps = reduce(gap_or_acc, chronologically, {
        'prev': None,
        'gaps': []
    })['gaps']

    log(full_log_msg())


def assert_is_contiguous(bd: BillingData, log=None) -> None:
    if is_contiguous(bd):
        return

    if log:
        _log_invalid_date_range(bd, log)

    raise NonContiguousBillingDataDateRangeError()


def assert_is_without_overlaps(bd: BillingData, log=None) -> None:
    if is_without_overlaps(bd):
        return

    if log:
        _log_invalid_date_range(bd, log)

    raise OverlappedBillingDataDateRangeError()


def _overlap(a: BillingDatum, b: BillingDatum):
    c_start = max(a.start, b.start)
    c_end = min(a.end, b.end)
    return max(c_end - c_start, timedelta())


def adjust_bill_dates(bills: BillingData) -> BillingData:
    """Ensure that the input list of bills is sorted by date and no two bills have overlapping dates."""
    bills.sort(key=lambda x: x.start)

    final_bills = []
    for b in bills:
        for other in final_bills:
            if _overlap(b, other) > timedelta() or b.start == other.end:
                b = b._replace(start=max(b.start, other.end + timedelta(days=1)))
        final_bills.append(b)

    return final_bills


def show_bill_summary(bills: List[BillingDatum], title=None):  # FIXME: Move this to a better location.
    """Save our results to the log for easy reference."""

    if title:
        log.info("=" * 80)
        log.info(title)
        log.info("=" * 80)

    fields = ("Start", "End", "Cost", "Use", "Peak", "Has PDF")
    fmt = "%-10s  %-10s  %-10s  %-10s  %-10s %-10s"
    log.info(fmt % fields)
    for b in bills:
        entries = [str(x) for x in b[:5]] + [b.attachments is not None]
        log.info(fmt % tuple(entries))
    log.info("=" * 80)
