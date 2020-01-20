import re

from datetime import timedelta

from datafeeds.parsers.base import (
    validate,
    epoch_to_dt,
    DataIntegrityFailure,
    KwInterval as Interval,
)


interval_schema = {
    "definitions": {
        "endpoint_type": {
            "type": "object",
            "properties": {
                "first": {"type": "number"},
                "last": {"type": "number"},
                "name": {"type": "string"},
                "values": {"type": "array", "items": {"type": ["number", "null"]}},
            },
            "required": ["first", "last", "name", "values"],
            "additionalProperties": True,
        }
    },
    "type": "object",
    "properties": {
        "window": {"type": "string"},
        "endPoints": {
            "type": "array",
            "items": {"$ref": "#/definitions/endpoint_type"},
        },
    },
    "required": ["window", "endPoints"],
    "additionalProperties": True,
}


"""
Example:
{
    "first": "1546329600000",
    "last": "1546416000000",
    "level": 1,
    "window": "900,900",
    "endPoints": [
        {
            "key": 1234567890,
            "name": "xstfy7hq.virtual.building",
            "first": 1546329600000,
            "last": 1546415100000,
            "values": [3055680, 3047040, 3029760, ... ]
        }
    ]
}
"""


def parse_intervals(site_id, text):
    result = validate(text, interval_schema, "interval")

    try:
        match = re.search(r"(\d+),(\d+)", result.window)
        if not match:
            raise DataIntegrityFailure(
                "Failed to locate interval width from API response. Received: %s"
                % result.window
            )
        width = int(match.group(1))
    except ValueError:
        raise DataIntegrityFailure(
            "Failed to parse interval width from API response. Received: %s"
            % result.window
        )

    valid_series = [s for s in result.endPoints if s.name == site_id]

    if not valid_series:
        raise DataIntegrityFailure(
            "Could not obtain a valid timeseries for site id %s." % site_id
        )

    if len(valid_series) > 1:
        msg = "Failed to determine a canonical timeseries for site id %s, %d timeseries present."
        raise DataIntegrityFailure(msg % (site_id, len(valid_series)))

    series = valid_series[0]
    start_dt = epoch_to_dt(series.first)
    end_dt = epoch_to_dt(series.last)
    num_intervals = len(
        series["values"]
    )  # Namespace conflict means we cannot write series.values here.

    if (end_dt - start_dt).total_seconds() != width * (num_intervals - 1):
        msg = (
            "Timeseries start/end dates do not match the number of intervals. "
            "Start: %s, End: %s, Interval Seconds: %d, Intervals: %d"
        )
        raise DataIntegrityFailure(msg % (start_dt, end_dt, width, num_intervals))

    interval_data = []
    current_dt = start_dt
    step = timedelta(seconds=width)
    for datum in series["values"]:
        next_dt = current_dt + step

        if datum is not None:
            kw = datum / 1000.0  # Engie API data is in Watts
            interval_data.append(Interval(start=current_dt, end=next_dt, kw=kw))

        current_dt = next_dt

    return interval_data
