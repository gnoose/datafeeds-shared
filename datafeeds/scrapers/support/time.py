from datetime import datetime
from dateutil import tz


def date_to_datetime(d, tzstr=None) -> datetime:
    """Convert the input date to a UTC or other datetime"""
    if tzstr is None:
        return datetime(d.year, d.month, d.day, tzinfo=tz.tzutc())
    else:
        return datetime(d.year, d.month, d.day, tzinfo=tz.gettz(tzstr))


def dt_to_epoch_ms(dt):
    """Convert the input datetime to epoch milliseconds."""
    return int(dt.timestamp() * 1000)


def dt_to_platform_pst(dt):
    """Convert the input datetime to a platform-friendly PST time."""
    p_tz = tz.gettz("US/Pacific")
    return dt.replace(second=0, microsecond=0).astimezone(p_tz)
