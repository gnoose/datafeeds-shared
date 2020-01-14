from collections import namedtuple
from csv import DictReader
from datetime import datetime, timedelta
from io import StringIO
from typing import Optional, Dict, List, Tuple

from datafeeds.common.timeline import Timeline


IntervalDatum = namedtuple("IntervalDatum", ["begin", "end", "kw"])


def interval_times(row: dict) -> Optional[Tuple[datetime, datetime]]:
    try:
        day = datetime.strptime(row.get("USAGE_DATE"), "%Y-%m-%d")

        begin_str = row.get("USAGE_START_TIME").split(":")
        begin_hour = int(begin_str[0])
        begin_minute = int(begin_str[1])

        end_str = row.get("USAGE_END_TIME").split(":")
        end_hour = int(end_str[0])
        end_minute = int(end_str[1])

        begin = day + timedelta(hours=begin_hour, minutes=begin_minute)
        if end_hour == 0 and end_minute == 0:
            end = day + timedelta(hours=24)
        else:
            end = day + timedelta(hours=end_hour, minutes=end_minute)

        return begin, end

    except (ValueError, IndexError, AttributeError):
        return None


def interval_demand_kw(row: dict) -> Optional[float]:
    try:
        start, end = interval_times(row)
        delta = (end - start).total_seconds()
        return float(row.get("USAGE_KWH")) * (3600 / delta)
    except (ValueError, TypeError):
        return None


def gather_intervals(file: StringIO, history: Dict[str, List[IntervalDatum]]) -> None:
    """Update the input history based upon the content of the input file."""
    reader = DictReader(file)
    ii = 0
    for row in reader:
        ii += 1
        times = interval_times(row)
        demand = interval_demand_kw(row)
        esiid = row.get("ESIID")

        if (times is None) or (demand is None) or (esiid is None):
            continue  # No valid data on this row.

        if esiid not in history:
            history[esiid] = []

        history[esiid].append(IntervalDatum(times[0], times[1], demand))


def prepare_timeline(intervals: List[IntervalDatum]) -> Optional[Timeline]:
    """Convert a list of interval data to a Timeline for platform serialization."""
    begin = min(i.begin for i in intervals)
    end = max(i.begin for i in intervals) + timedelta(days=1)
    durations = set(i.end - i.begin for i in intervals)

    if len(durations) != 1:
        return None  # Durations are inconsistent, we cannot construct a timeline.

    duration = durations.pop()
    minutes = duration.total_seconds() / 60

    t = Timeline(begin.date(), end.date(), minutes)
    for i in intervals:
        t.insert(i.begin, i.kw)

    return t
