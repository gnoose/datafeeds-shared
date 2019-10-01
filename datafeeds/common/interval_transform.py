import copy
from enum import Enum
from datetime import timedelta
from functools import partial
from typing import List, Optional, Tuple
import logging

from dateutil import parser as date_parser
from dateutil import tz

from datafeeds import db
from datafeeds.models import Meter, SnapmeterAccount, SnapmeterAccountMeter
from datafeeds.common.typing import IntervalReadings, IntervalIssue
from datafeeds.common import index


log = logging.getLogger(__name__)

PERCENTILE_MIN_DAYS = 10


def _interval_percentile(meter_id: int, readings: IntervalReadings):
    """Get the 95th percentile reading over the previous 10 days."""
    first_dt = date_parser.parse(min(readings.keys())).date()
    min_dt = first_dt - timedelta(days=PERCENTILE_MIN_DAYS)
    # make sure there is enough data for the check
    query = "select count(*) from meter_reading where meter=:meter and occurred >= :dt"
    days = db.session.execute(query, {"meter": meter_id, "dt": min_dt}).first()[0]
    if days < PERCENTILE_MIN_DAYS:
        log.info("meter %s has %s days since %s; skipping outlier check", meter_id, days, min_dt)
        return None
    # get 95th percentile over previous 10 days
    # use json_array_elements to flatten readings array into values, then cast to numeric
    # (cast to text, then numeric since JSON data only goes directly text or int)
    query = """
        select percentile_cont(0.95)
        within group (order by r.val) from (
            select val::text::numeric from (
                select json_array_elements(readings) val
                from meter_reading
                where meter=:meter and occurred >= :min_dt and occurred < :first_dt
            ) as readings_val
            where not val::text = "null"
        ) as r
    """
    params = {"meter": meter_id, "min_dt": min_dt, "first_dt": first_dt}
    return db.session.execute(query, params).first()[0]


def remove_outliers(readings: IntervalReadings, meter: Meter) -> Tuple[IntervalReadings, List[IntervalIssue]]:
    """Replace readings > 10x the 95th percentile with None.

    Return updated readings and list of issues.
    """
    max_val = _interval_percentile(meter.oid, readings)
    if max_val is None:
        log.info("percentile for meter %s (%s) is None", meter.oid, readings.keys())
        return readings, []
    max_val = max_val * 10
    transformed = copy.deepcopy(readings)
    issues = []
    for day in readings:
        bad_indexes = set()
        day_dt = date_parser.parse(day)
        for idx, val in enumerate(readings[day]):
            if not val or val < max_val:
                continue
            bad_indexes.add(idx)
            interval_dt = day_dt + timedelta(minutes=(idx * meter.interval))
            meter_tz = tz.gettz(meter.timezone)
            issues.append(IntervalIssue(
                interval_dt=interval_dt.replace(tzinfo=meter_tz),
                error="interval value > 10x 95th percentile",
                value=val))
        for idx in bad_indexes:
            transformed[day][idx] = None

    return transformed, issues


class Transforms(Enum):
    outliers = partial(remove_outliers)


def transform(transforms: List[Transforms], task_id: Optional[str], scraper: str, meter_id: int,
              readings: IntervalReadings) -> IntervalReadings:
    """Transform interval readings data before sending to webapps.

    Readings data looks like {'2017-04-02' : [59.1, 30.2,...]}

    Transform as needed, and return the same format.
    """
    if not readings or not transforms:
        return readings
    # get meter and account
    meter = db.session.query(Meter).get(meter_id)
    account = db.session.query(SnapmeterAccount).\
        filter(SnapmeterAccountMeter.meter == meter_id).\
        filter(SnapmeterAccount.oid == SnapmeterAccountMeter.account).\
        first()
    if not meter or not account:
        log.error("cannot load meter or account for meter %s", meter_id)
        return readings
    # run transforms and collect issues
    transformed = copy.deepcopy(readings)
    all_issues = []
    for action in transforms:
        try:
            log.info("running transform %s on meter %s", action.name, meter_id)
            (transformed, issues) = action.value(transformed, meter)
            all_issues += issues
        except Exception as e:
            # don't want to lose the data if something goes wrong
            log.error("error transforming %s meter %s: %s %s", action, meter_id, readings, e)
    if all_issues:
        index.index_etl_interval_issues(task_id, account.hex_id, account.name, meter.oid, meter.name, scraper,
                                        all_issues)

    return transformed
