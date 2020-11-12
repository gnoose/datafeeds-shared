from datetime import datetime

from dateutil.rrule import rrule, YEARLY, SU

# Daylight savings time starts on the second Sunday in March.
DST_STARTS = set(
    dt.date()
    for dt in rrule(
        YEARLY, bymonth=3, byweekday=SU(2), dtstart=datetime(2000, 1, 1), count=100
    )
)

# Daylight savings time ends on the first Sunday in November.
DST_ENDS = set(
    dt.date()
    for dt in rrule(
        YEARLY, bymonth=11, byweekday=SU(1), dtstart=datetime(2000, 1, 1), count=100
    )
)
