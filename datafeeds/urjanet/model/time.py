from datetime import date, timedelta
from collections import deque
from typing import Any, List, Callable

from intervaltree import Interval, IntervalTree


class DateIntervalTree:
    """A slight adaption of the intervaltree library to support python dates

    The intervaltree data structure stores integer ranges, fundamentally. Therefore, if we want to
    store dates, we must fist convert them to integers, in a way that preserves inequalities.
    Luckily, the toordinal() function on datetime.date satisfies this requirement.

    It's important to note that this interval tree structure is, unless otherwise noted inclusive of
    lower bounds and exclusive of upper bounds. That is to say, an interval from A to B includes the
    value A and excludes the value B.
    """

    def __init__(self):
        self.tree = IntervalTree()

    @staticmethod
    def to_date_interval(begin: date, end: date, data: Any) -> Interval:
        """Convert a date interval (and associated date, if any) into an ordinal interval"""
        return Interval(begin.toordinal(), end.toordinal(), data)

    @staticmethod
    def from_date_interval(ival: Interval) -> Interval:
        """Convert an ordinal interval to a date interval"""
        return Interval(
            date.fromordinal(ival.begin), date.fromordinal(ival.end), ival.data
        )

    def add(self, begin: date, end: date, data: Any = None):
        """Add a date interval to the interval tree, along with any associated date"""
        self.tree.add(DateIntervalTree.to_date_interval(begin, end, data))

    def merge_overlaps(self, reducer: Callable = None, strict: bool = True):
        """Merge overlapping date intervals in the tree.

        A reduce function can be specified to determine how data elements are combined for overlapping intervals.
        The strict argument determines whether "kissing" intervals are merged. If true (the default), only "strictly"
        overlapping intervals are merged, otherwise adjacent intervals will also be merged.

        See the intervaltree library documentation for the merge_overlaps function for a more complete description.
        """
        self.tree.merge_overlaps(data_reducer=reducer, strict=strict)

    def intervals(self) -> List[Interval]:
        """Return all date intervals in this tree"""

        # Note we convert from ordinal values to actual date objects
        return [DateIntervalTree.from_date_interval(ival) for ival in self.tree.items()]

    def overlaps(self, begin: date, end: date) -> bool:
        """Determine whether the given date interval (strictly) overlaps with any interval in the tree"""
        ival = DateIntervalTree.to_date_interval(begin, end, None)
        return self.tree.overlaps(ival.begin, ival.end)

    def range_query(self, begin: date, end: date) -> List[Interval]:
        """Return all intervals in the tree that strictly overlap with the given interval"""
        ival = DateIntervalTree.to_date_interval(begin, end, None)
        return [
            DateIntervalTree.from_date_interval(ival)
            for ival in self.tree.overlap(ival.begin, ival.end)
        ]

    def point_query(self, point: date) -> List[Interval]:
        return [
            DateIntervalTree.from_date_interval(ival)
            for ival in self.tree.at(point.toordinal())
        ]

    @staticmethod
    def shift_endpoints(date_tree: "DateIntervalTree") -> "DateIntervalTree":
        """Produce a new tree where adjacent intervals are guaranteed to not match at a boundary

        E.g., the intervals
            (1/1/2000, 1/10/2000), (1/10/2000, 1/20/2000)
        become
            (1/1/2000, 1/9/2000), (1/10/2000, 1/20/2000)
                         ^--A day was subtracted here to avoid matching exactly with the next interval
        """
        adjusted = DateIntervalTree()
        work_list = deque(sorted(date_tree.intervals()))
        while work_list:
            cur_ival = work_list.popleft()
            if work_list:
                next_ival = work_list[0]
                if cur_ival.end == next_ival.begin:
                    cur_ival = Interval(
                        cur_ival.begin, cur_ival.end - timedelta(days=1), cur_ival.data
                    )
            adjusted.add(cur_ival.begin, cur_ival.end, cur_ival.data)
        return adjusted
