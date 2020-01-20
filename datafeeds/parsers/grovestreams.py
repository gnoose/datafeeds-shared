from datetime import datetime

from dateutil.tz import tzutc

from datafeeds.parsers.base import validate, KwhInterval as Interval


UTC = tzutc()


class GrovestreamsOrganizationNotFoundFailure(Exception):
    pass


class GrovestreamsDataIntegrityException(Exception):
    """This exception signifies that the data from the API is not internally consistent."""

    pass


# Define the login response objects we'll receive from the API.
login_schema = {
    "definitions": {
        "organization": {
            "type": "object",
            "properties": {
                "uid": {"type": "string"},
                "name": {"type": "string"},
                "type": {"type": "string"},
            },
            "required": ["uid", "name", "type"],
            "additionalProperties": False,
        }
    },
    "type": "object",
    "properties": {
        "organization": {
            "type": "array",
            "items": {"$ref": "#/definitions/organization"},
        }
    },
    "required": ["organization"],
    "additionalProperties": True,
}

"""
Example:
{
    "success": true,
    "userLastName": "M*********",
    "sessionUid": "1f76f217-6c55-3169-95cb-050604f4fb4a",
    "organization": [
        {
            "uid": "a2803e43-97eb-3353-b4ee-51dd3ec97e36",
            "name": "Mets",
            "type": "organization"
        }
    ],
    "userFirstName": "Tom",
    "message": "Login succeeded.",
    "userUid": "376a50fc-acbb-3b7b-8b2e-4bb97c7b07ec"
}
"""


interval_schema = {
    "definitions": {
        "component_type": {
            "type": "object",
            "properties": {
                "stream": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/stream_type"},
                }
            },
        },
        "stream_type": {
            "type": "object",
            "properties": {
                "lastValueStartDate": {"type": "number"},
                "lastValueEndDate": {"type": "number"},
                "streamUid": {"type": "string"},
                "statistic": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/statistic_type"},
                },
            },
            "required": [
                "lastValueStartDate",
                "lastValueEndDate",
                "streamUid",
                "statistic",
            ],
            "additionalProperties": True,
        },
        "statistic_type": {
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {"type": ["number", "null"]}}
            },
        },
    },
    "type": "object",
    "properties": {
        "feed": {
            "type": "object",
            "properties": {
                "component": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/component_type"},
                },
                "requestEndDate": {"type": "number"},
                "requestStartDate": {"type": "number"},
            },
            "required": ["component", "requestEndDate", "requestStartDate"],
            "additionalProperties": True,
        }
    },
    "required": ["feed"],
    "additionalProperties": True,
}

"""
Example:
{
    "feed": {
        "component": [
            {
                "stream": [
                    {
                        "lastUpdated": 1548696813647,
                        "lastValue": 0,
                        "statistic": [
                            {
                                "data": [0, 0, 0, 0, ...],
                                "name": "BASE",
                                "type": "DOUBLE"
                            }
                        ],
                        "streamType": "intvl_stream",
                        "lastValueStartDate": 1548661500000,
                        "cycleUid": "c9d95a69-b7a8-3db1-84a7-2c085d4bbaad",
                        "lastValueType": "DOUBLE",
                        "completedDate": 1548661500001,
                        "lastValueEndDate": 1548662400000,
                        "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                    }
                ],
                "componentUid": "1a2c2769-ad79-3c49-bf67-0a8d02cccee1"
            }
        ],
        "requestEndDate": 1547640000000,
        "requestStartDate": 1547553600000
    },
    "success": true,
    "message": ""
}
"""


def parse_login(organization_name, text):
    result = validate(text, login_schema, "login")

    for org in result.organization:
        if org.type == "organization" and org.name == organization_name:
            return org.uid

    msg = "No organization %s associated with this login."
    raise GrovestreamsOrganizationNotFoundFailure(msg % organization_name)


def _ts_to_dt(ts):
    """Convert an epoch time in milliseconds to a datetime object."""
    return datetime.fromtimestamp(ts / 1000.0, tz=UTC)


def parse_intervals(text):
    result = validate(text, interval_schema, "interval")

    if not result.feed.component:
        return None, []
    if len(result.feed.component) > 1:
        error = "The parser should only receive one component-record at a time."
        raise GrovestreamsDataIntegrityException(error)

    comp = result.feed.component[0]

    if not comp.stream:
        return None, []
    if len(comp.stream) > 1:
        error = "The parser should only receive one stream record at a time."
        raise GrovestreamsDataIntegrityException(error)

    stream = comp.stream[0]

    if not stream.statistic:
        return stream.streamUid, []
    if len(stream.statistic) > 1:
        error = "The parser should only receive one statistic at a time."
        raise GrovestreamsDataIntegrityException(error)

    statistic = stream.statistic[0]

    start_dt = _ts_to_dt(result.feed.requestStartDate)
    end_dt = _ts_to_dt(result.feed.requestEndDate)

    interval = _ts_to_dt(stream.lastValueEndDate) - _ts_to_dt(stream.lastValueStartDate)

    actual = len(statistic.data)
    expected = (end_dt - start_dt).total_seconds() // interval.total_seconds()

    if expected != actual:
        msg = "Expected %d interval data entries, found %s."
        raise GrovestreamsDataIntegrityException(msg % (expected, actual))

    interval_data = []

    current_dt = start_dt
    for datum in statistic.data:
        next_dt = current_dt + interval

        if datum is not None:
            # If the data is null, don't bother incorporating it.
            interval_data.append(Interval(start=current_dt, end=next_dt, kwh=datum))

        current_dt = next_dt

    return stream.streamUid, interval_data
