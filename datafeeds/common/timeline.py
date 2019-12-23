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

        current = datetime(start.year, start.month, start.day)
        stop = datetime(end.year, end.month, end.day)

        while current < stop + timedelta(hours=24):
            self.index[current.date()][current.time()] = None
            current = current + timedelta(minutes=interval)

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
            return self.index[d][t]

        return None

    def serialize(self):
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

            if len(temp) != expected:
                msg = "Expected %d values for date %s, but found %d."
                raise SerializationError(msg % (expected, str(day), len(temp)))

            result[str(day)] = [v for (k, v) in temp]

        return result
