from collections import namedtuple
from datetime import datetime

from dateutil.tz import tzutc

from datafeeds.parsers.base import (
    SchemaValidationFailure,
    KwInterval as Interval,
    validate,
)


UTC = tzutc()


# Define the client objects we'll receive from the API.
client_list_schema = {
    "definitions": {
        "client": {
            "type": "object",
            "properties": {
                "link": {"type": "string"},
                "id": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": ["link", "id", "name"],
            "additionalProperties": False,
        }
    },
    "type": "object",
    "properties": {
        "total": {"type": "number"},
        "page": {"type": "string"},
        "clients": {"type": "array", "items": {"$ref": "#/definitions/client"}},
    },
    "required": ["total", "page", "clients"],
    "additionalProperties": False,
}

"""
Example:
{
    "total": 1,
    "clients": [
        {
            "link": "/api/v1/clients/487f19a0-5cd2-11e7-89fb-22000a1e2dd3",
            "id": "487f19a0-5cd2-11e7-89fb-22000a1e2dd3",
            "name": "John Hancock Insurance"
        }
    ],
    "page": 1
}
"""

# Define the Site List objects we'll receive from the API:
site_list_schema = {
    "definitions": {
        "site": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "link": {"type": "string"},
                "name": {"type": "string"},
                "stream_start_times": {
                    "type": "object",
                    "properties": {
                        "CONVERTER": {"type": "string", "format": "date-time"},
                        "INTERVAL_REVENUE": {"type": "string", "format": "date-time"},
                        "MONITOR": {"type": "string", "format": "date-time"},
                    },
                },
            },
            "required": ["id", "link", "name", "stream_start_times"],
            "additionalProperties": False,
        }
    },
    "type": "object",
    "properties": {
        "total": {"type": "number"},
        "page": {"type": "string"},
        "sites": {"type": "array", "items": {"$ref": "#/definitions/site"}},
    },
    "required": ["total", "sites", "page"],
    "additionalProperties": False,
}

"""
Example:
{
    "total": 1,
    "sites": [
        {
            "stream_start_times": {
                "MONITOR": "2016-10-27T00:37:31Z",
                "CONVERTER": "2016-10-27T01:41:58Z"
            },
            "link": "/api/v1/sites/487f162c-5cd2-11e7-89fb-22000a1e2dd3",
            "id": "487f162c-5cd2-11e7-89fb-22000a1e2dd3",
            "name": "John Hancock Insurance"
        }
    ],
    "page": 1
}
"""

# Define the interval streams we'll receive from the API:
stream_schema = {
    "definitions": {
        "interval": {
            "type": "object",
            "properties": {
                "start_datetime": {"type": "string", "format": "datetime"},
                "end_datetime": {"type": "string", "format": "datetime"},
                "kw_total_avg": {"type": ["number", "null"]},
            },
            "required": ["start_datetime", "end_datetime", "kw_total_avg"],
        },
        "stream_type": {
            "type": "object",
            "properties": {
                "stream_type": {"type": "string"},
                "streams": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/interval"},
                },
            },
            "required": ["stream_type", "streams"],
        },
    },
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "stream_types": {
            "type": "array",
            "items": {"$ref": "#/definitions/stream_type"},
        },
    },
    "required": ["id", "name", "stream_types"],
}

"""
Example:

{
    "stream_types": [
        {
            "streams": [
                {
                    "start_datetime": "2018-01-01T00:00:01Z",
                    "kw_total_avg": 411.899,
                    "end_datetime": "2018-01-01T00:15:00Z"
                },
                {
                    "start_datetime": "2018-01-01T00:15:01Z",
                    "kw_total_avg": 411.82892,
                    "end_datetime": "2018-01-01T00:30:00Z"
                },
                {
                    "start_datetime": "2018-01-01T00:30:01Z",
                    "kw_total_avg": 411.60704,
                    "end_datetime": "2018-01-01T00:45:00Z"
                }
            ],
            "stream_type": "MONITOR"
        }
    ],
    "id": "487f162c-5cd2-11e7-89fb-22000a1e2dd3",
    "name": "John Hancock Insurance"
}
"""

Client = namedtuple("Client", "link, id, name")
Site = namedtuple("Site", "link, id, name, start")


def parse_clients(text):
    record = validate(text, client_list_schema, "Client")
    clients = [Client(link=c.link, id=c.id, name=c.name) for c in record.clients]
    return clients


def _parse_datetime(text):
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        msg = "Interval data contained malformed date-time. text: "
        raise SchemaValidationFailure(msg + text)


def parse_sites(text):
    record = validate(text, site_list_schema, "Site")
    sites = [
        Site(
            id=s.id,
            link=s.link,
            name=s.name,
            start=_parse_datetime(s.stream_start_times.MONITOR),
        )
        for s in record.sites
        if s.stream_start_times.MONITOR
    ]
    return sites


def parse_intervals(text, stream_type):
    record = validate(text, stream_schema, "Interval")

    intervals = []
    for st in record.stream_types:
        if st.stream_type == stream_type:
            for rec in st.streams:
                ivl = Interval(
                    start=_parse_datetime(rec.start_datetime),
                    end=_parse_datetime(rec.end_datetime),
                    kw=rec.kw_total_avg,
                )
                intervals.append(ivl)

    return intervals
