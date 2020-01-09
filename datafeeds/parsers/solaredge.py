from collections import namedtuple
from datetime import datetime

from datafeeds.parsers.base import validate, SchemaValidationFailure


# site_details_schema = {
#     "type": "object",
#     "properties": {
#         "meterEnergyDetails": {
#             "type": "object",
#
#         }
#     }
# }

site_details_schema = {
    "definitions": {
        "site": {
            "type": "object",
            "properties": {
                "id": {"type": "number"},
                "name": {"type": "string"},
                "accountId": {"type": "number"},
                "status": {"type": "string"},
                "peakPower": {"type": "number"},
                "lastUpdateTime": {"type": "string", "format": "date"},
                "currency": {"type": "string"},
                "installationDate": {"type": "string", "format": "date"},
                "ptoDate": {"type": "string", "format": "date"},
                "notes": {"type": "string"},
                "type": {"type": "string"},
                "location": {
                    "type": "object",
                    "properties": {
                        "country": {"type": "string"},
                        "state": {"type": "string"},
                        "city": {"type": "string"},
                        "address": {"type": "string"},
                        "address2": {"type": "string"},
                        "zip": {"type": "string"},
                        "timeZone": {"type": "string"},
                        "countryCode": {"type": "string"},
                        "stateCode": {"type": "string"},
                    },
                    "required": ["timeZone"],
                },
                "primaryModule": {
                    "type": "object",
                    "properties": {
                        "manufacturerName": {"type": "string"},
                        "modelName": {"type": "string"},
                        "maximumPower": {"type": "number"},
                    },
                },
                "uris": {
                    "type": "object",
                    "properties": {
                        "SITE_IMAGE": {"type": "string"},
                        "DATA_PERIOD": {"type": "string"},
                        "INSTALLER_IMAGE": {"type": "string"},
                        "DETAILS": {"type": "string"},
                        "OVERVIEW": {"type": "string"},
                    },
                },
                "publicSettings": {
                    "type": "object",
                    "properties": {"isPublic": {"type": "boolean"}},
                },
            },
            "required": ["id", "name", "installationDate"],
        }
    },
    "type": "object",
    "properties": {"details": {"$ref": "#/definitions/site"}},
    "required": ["details"],
}


meter_energy_details_schema = {
    "definitions": {
        "interval": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "format": "datetime"},
                "value": {"type": "number"},
            },
            "required": ["date"],
        },
        "meter": {
            "type": "object",
            "properties": {
                "meterSerialNumber": {"type": "string"},
                "connectedSolaredgeDeviceSN": {"type": "string"},
                "model": {"type": "string"},
                "meterType": {"type": "string"},
                "values": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/interval"},
                },
            },
            "required": ["meterSerialNumber"],
        },
        "site_readings": {
            "type": "object",
            "properties": {
                "timeUnit": {"type": "string"},
                "unit": {"type": "string"},
                "meters": {"type": "array", "items": {"$ref": "#/definitions/meter"}},
            },
        },
    },
    "type": "object",
    "properties": {"meterEnergyDetails": {"$ref": "#/definitions/site_readings"}},
}

Site = namedtuple("Site", "id, name, installation_date, link, time_zone, address")
# no end date here, so not importing from base
Interval = namedtuple("Interval", "start, kwh, serial_number")


def _parse_datetime(text):
    try:
        # Time zone info is found in a different API call,
        # and should be added by the scraper
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        msg = "Interval data contained malformed date-time. text: "
        raise SchemaValidationFailure(msg + text)


def parse_site(text):
    record = validate(text, site_details_schema, "Site")
    rd = record.details
    site = Site(
        id=rd.id,
        name=rd.name,
        installation_date=rd.installationDate,
        time_zone=rd.location.timeZone,
        address=rd.location.address,
        link=rd.uris.OVERVIEW,
    )
    return site


def parse_intervals(text):
    record = validate(text, meter_energy_details_schema, "Interval")
    intervals = []
    for meter in record.meterEnergyDetails.meters:
        if meter.meterType == "Production":
            for reading in meter["values"]:
                # Sometimes it doesn't have 'value'
                if not reading.value:
                    reading.value = float("nan")
                ivl = Interval(
                    start=_parse_datetime(reading.date),
                    kwh=reading.value / 1000,
                    serial_number=meter.meterSerialNumber,
                )
                intervals.append(ivl)
    return intervals
