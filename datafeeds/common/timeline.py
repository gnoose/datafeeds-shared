"""timeline

This module allows us to collect data (typically usage measurements)
within a date range (inclusive), and then format them for storage in
Platform.

The data structure performs some basic checks on the input data, and
verifies we have the right number of measurments per day at
serialization time.

Any intervals that are not filled are prepopulated to None.
"""

from collections import defaultdict, OrderedDict
from datetime import timedelta, datetime


class SerializationError(Exception):
    pass


class Timeline:
    def __init__(self, start, end, interval=15):
        """Create a timeline of interval data between the two input dates
        (inclusive), where the interval is in minutes."""

        self._start = start
        self._end = end
        self._interval = interval

        self.index = defaultdict(dict)

        self._initially_populate(start, end)

    def _initially_populate(self, start_populate: datetime, end_populate: datetime):
        """Prepopulates the index with Nones"""
        current = datetime(
            start_populate.year, start_populate.month, start_populate.day
        )
        stop = datetime(end_populate.year, end_populate.month, end_populate.day)

        while current < stop + timedelta(hours=24):
            self.index[current.date()][current.time()] = None
            current = current + timedelta(minutes=self._interval)

    def extend_timeline(self, new_start, new_end):
        """Widens the timeline, if applicable.  Does not shrink the timeline, to avoid dropping
        interval data that may already be present"""
        if self._start > new_start:
            # Shifts the start of the timeline to an earlier date
            self._initially_populate(new_start, self._start - timedelta(hours=24))
            self._start = new_start

        if self._end < new_end:
            # Extends the end of the timeline to a later date
            self._initially_populate(self._end + timedelta(hours=24), new_end)
            self._end = new_end

    def insert(self, dt, value):
        """Insert a value at the input datetime."""
        d = dt.date()
        t = dt.time()
        if self._start <= d <= self._end:
            self.index[d][t] = value

    def lookup(self, dt):
        """Lookup a value at the input datetime."""
        d = dt.date()
        t = dt.time()
        if self._start <= d <= self._end:
            return self.index[d].get(t)

        return None

    def serialize(self, include_empty=True):
        """Write this datastructure to a JSON record of the form:
        {
            '%Y-%m-%d' : [ N float or null ]
        }
        where N is the number of intervals in a day.
        """
        result = OrderedDict()

        expected = 24 * 60 // self._interval

        for day in sorted(self.index.keys()):
            data = self.index[day]
            temp = list(data.items())
            temp.sort(key=lambda x: x[0])
            values = [v for (k, v) in temp]
            if not include_empty and set(values) == {None}:
                continue

            if len(temp) != expected:
                msg = "Expected %d values for date %s, but found %d."
                raise SerializationError(msg % (expected, str(day), len(temp)))

            result[str(day)] = values

        return result
