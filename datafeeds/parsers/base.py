"""This module contains functions and types that are common to all JSON parsers."""
from datetime import datetime
import dateutil
from collections import namedtuple
import json

from jsonschema import validate as js_validate, ValidationError, FormatChecker
from addict import Dict


KwhInterval = namedtuple('Interval', 'start, end, kwh')
KwInterval = namedtuple('Interval', 'start, end, kw')


class JsonParseFailure(Exception):
    pass


class SchemaValidationFailure(Exception):
    pass


class DataIntegrityFailure(Exception):
    """This exception covers semantic errors in API data.

    For example, when one part of the record says 96 points of interval data are present,
    but actually only 3 numbers are in the list.
    """
    pass


def validate(text, schema, schema_name):
    """Verify the input text parses as JSON and satisfies the input schema."""
    try:
        record = json.loads(text)
    except json.decoder.JSONDecodeError:
        raise JsonParseFailure("The input text failed to parse as JSON.")

    try:
        js_validate(record, schema, format_checker=FormatChecker())
    except ValidationError as ve:
        msg = "The input text did not match the %s schema. Reason: %s "
        raise SchemaValidationFailure(msg % (schema_name, ve.message))

    return Dict(record)


def epoch_to_dt(ts):
    """Convert an epoch time in milliseconds to a UTC datetime object."""
    return datetime.fromtimestamp(ts / 1000.0, tz=dateutil.tz.tzutc())
