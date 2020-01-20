"""This module captures a handful of types that are relevant to buildings with batteries. In this context we are
interested in multiple timeseries, capturing the battery bank's charge and discharge. """

from enum import Enum


class InvalidTimeSeriesTypeError(Exception):
    pass


class TimeSeriesType(Enum):
    CHARGE = 1
    DISCHARGE = 2
    SYNTHETIC_BUILDING_LOAD = 3

    @staticmethod
    def parse(ts_type_str):
        """Convert the string meter type to an enum, regardless of case or spacing."""
        try:
            sanitized = ts_type_str.upper().replace(" ", "_")
            return TimeSeriesType[sanitized]
        except KeyError:
            msg = "Invalid meter type: %s. Must be one of 'charge', 'discharge', or 'synthetic building load"
            raise InvalidTimeSeriesTypeError(msg % ts_type_str)
