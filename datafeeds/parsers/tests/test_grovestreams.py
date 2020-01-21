from datetime import datetime, timedelta
import os
import unittest

from dateutil.tz import tzutc

from datafeeds.parsers.base import JsonParseFailure, SchemaValidationFailure
from datafeeds.parsers import grovestreams as p
from datafeeds.parsers.grovestreams import (
    GrovestreamsOrganizationNotFoundFailure,
    GrovestreamsDataIntegrityException,
)


TEST_DATA_DIR = os.path.dirname(os.path.abspath(__file__))


class LoginParseTests(unittest.TestCase):
    def test_login_json_parse_failure(self):
        """The parser raises an exception when the input isn't valid JSON."""
        bad_text = '{"userUid": }'
        with self.assertRaises(JsonParseFailure):
            p.parse_login("Mets", bad_text)

    def test_login_schema_failure_org_uuid_1(self):
        """Schema checking verifies an organization key is present."""
        bad_text = '{"foo": "bar"}'
        with self.assertRaises(SchemaValidationFailure):
            p.parse_login("Mets", bad_text)

    def test_login_schema_failure_org_uuid_2(self):
        """Schema checking verifies that the organization key points to a list."""
        bad_text = '{"organization": "bad", "sessionUid": "dfd14fe0-b9da-3cea-9396-8b67bb57d1ad"}'
        with self.assertRaises(SchemaValidationFailure):
            p.parse_login("Mets", bad_text)

    def test_login_schema_failure_org_uuid_3(self):
        """Schema checking verifies that the organization record has a UID field."""
        bad_text = """
            {"organization": [
                {
                   "name": "Mets",
                   "type": "organization"
                }
            ]}
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_login("Mets", bad_text)

    def test_login_schema_failure_org_uuid_4(self):
        """Schema checking verifies that the organization record has a name field."""
        bad_text = """
            {"organization": [
                {
                   "uid": "abcdefg1234",
                   "type": "organization"
                }
            ]}
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_login("Mets", bad_text)

    def test_login_schema_failure_org_uuid_5(self):
        """Schema checking verifies that the organization record has a type field."""
        bad_text = """
            {"organization": [
                {
                   "uid": "abcdefg1234",
                   "name": "Mets"
                }
            ],
             "sessionUid": "dfd14fe0-b9da-3cea-9396-8b67bb57d1ad"}
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_login("Mets", bad_text)

    def test_login_missing_org_failure(self):
        """Schema checking verifies that the organization key points to a nonempty list."""
        bad_text = (
            '{"organization": [], "sessionUid": "dfd14fe0-b9da-3cea-9396-8b67bb57d1ad"}'
        )
        with self.assertRaises(GrovestreamsOrganizationNotFoundFailure):
            p.parse_login("Mets", bad_text)

    def test_login_parse(self):
        """The parser recovers the correct organization ID when multiple records are present."""

        text = """
        {
            "success": true,
            "userLastName": "efgh",
            "sessionUid": "dfd14fe0-b9da-3cea-9396-8b67bb57d1ad",
            "organization": [
                {
                    "uid": "abcd12345",
                    "name": "Yankees",
                    "type": "organization"
                },
                {
                    "uid": "efgh6789",
                    "name": "Mets",
                    "type": "organization"
                },
                {
                    "uid": "ijkl0123",
                    "name": "Phillies",
                    "type": "organization"
                }
            ],
            "userFirstName": "abcd",
            "message": "Login succeeded.",
            "userUid": "376a50fc-acbb-3b7b-8b2e-4bb97c7b07ec"
        }
        """
        org_id = p.parse_login("Mets", text)
        self.assertEqual(org_id, "efgh6789")


example_interval = """
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
                                "data": [
                                    0,
                                    0,
                                    0,
                                    0,

                                ],
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


class IntervalParseTests(unittest.TestCase):
    def test_interval_parse_failure(self):
        """The parser raises an exception when the input is not valid JSON."""
        bad_text = '{"feed": }'
        with self.assertRaises(JsonParseFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_no_request_start_date(self):
        """The parser raises an exception if a time series is missing a start date."""
        bad_text = """
        {
            "feed": {
                "component": [],
                "requestEndDate": 1547640000000
            },
            "success": true,
            "message": ""
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_no_request_end_date(self):
        """The parser raises an exception if a time series is missing an end date."""
        bad_text = """
        {
            "feed": {
                "component": [],
                "requestStartDate": 1547553600000
            },
            "success": true,
            "message": ""
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_no_last_value_start_date(self):
        """The parser raises an exception if interval-size metadata is missing."""
        bad_text = """
        {
            "feed": {
                "component": [
                    {
                        "stream": [
                            {
                                "statistic": [ { "data": [0, 0, 0, 0] } ],
                                "lastValueEndDate": 1548662400000,
                                "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                            }
                        ]
                    }
                ],
                "requestEndDate": 1547640000000,
                "requestStartDate": 1547553600000
            }
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_no_last_value_end_date(self):
        """The parser raises an exception if interval-size metadata is missing."""
        bad_text = """
           {
               "feed": {
                   "component": [
                       {
                           "stream": [
                               {
                                   "statistic": [ { "data": [0, 0, 0, 0] } ],
                                   "lastValueStartDate": 1548661500000,
                                   "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                               }
                           ]
                       }
                   ],
                   "requestEndDate": 1547640000000,
                   "requestStartDate": 1547553600000
               }
           }
           """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_no_stream_uid(self):
        """The parser raises an exception if timeseries identifier is missing."""
        bad_text = """
           {
               "feed": {
                   "component": [
                       {
                           "stream": [
                               {
                                   "statistic": [ { "data": [0, 0, 0, 0] } ],
                                   "lastValueStartDate": 1548661500000,
                                   "lastValueEndDate": 1548662400000
                               }
                           ]
                       }
                   ],
                   "requestEndDate": 1547640000000,
                   "requestStartDate": 1547553600000
               }
           }
           """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_bad_date_type_1(self):
        """The parser raises an exception if the timeseries start time is not in epoch format."""
        bad_text = """
        {
            "feed": {
                "component": [],
                "requestStartDate": "foo",
                "requestEndDate": 1547640000000
            },
            "success": true,
            "message": ""
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_bad_date_type_2(self):
        """The parser raises an exception if the timeseries end time is not in epoch format."""
        bad_text = """
          {
              "feed": {
                  "component": [],
                  "requestStartDate": 1547553600000,
                  "requestEndDate": "foo"
              },
              "success": true,
              "message": ""
          }
          """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_bad_date_type_3(self):
        """The parser raises an exception if the interval metadata is not in epoch format."""
        bad_text = """
        {
            "feed": {
                "component": [
                    {
                        "stream": [
                            {
                                "statistic": [ { "data": [0, 0, 0, 0] } ],
                                "lastValueStartDate": "abcd",
                                "lastValueEndDate": 1548662400000,
                                "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                            }
                        ]
                    }
                ],
                "requestEndDate": 1547640000000,
                "requestStartDate": 1547553600000
            }
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_bad_date_type_4(self):
        """The parser raises an exception if the interval metadata is not in epoch format."""
        bad_text = """
           {
               "feed": {
                   "component": [
                       {
                           "stream": [
                               {
                                   "statistic": [ { "data": [0, 0, 0, 0] } ],
                                   "lastValueStartDate": 1548661500000,
                                   "lastValueEndDate": "abcd",
                                   "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                               }
                           ]
                       }
                   ],
                   "requestEndDate": 1547640000000,
                   "requestStartDate": 1547553600000
               }
           }
           """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_interval_schema_failure_bad_interval_data_type(self):
        """The parser raises an exception if the timeseries data is not of type Option(number)."""
        bad_text = """
        {
            "feed": {
                "component": [
                    {
                        "stream": [
                            {
                                "statistic": [ { "data": [0, "bad", 0, 0] } ],
                                "lastValueStartDate": 1548661500000,
                                "lastValueEndDate": 1548662400000,
                                "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                            }
                        ]
                    }
                ],
                "requestEndDate": 1547640000000,
                "requestStartDate": 1547553600000
            }
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals(bad_text)

    def test_incoherent_stream_dates(self):
        """The parser raises an exception if there is a conflict between the metadata and interval data."""
        bad_text = """
        {
            "feed": {
                "component": [
                    {
                        "stream": [
                            {
                                "statistic": [ { "data": [0, 0, 0, null] } ],
                                "lastValueStartDate": 1548661500000,
                                "lastValueEndDate": 1548662400000,
                                "streamUid": "bb809707-e759-34bd-bb3c-1e13d1ab3b7c"
                            }
                        ]
                    }
                ],
                "requestEndDate": 1547640000000,
                "requestStartDate": 1547553600000
            }
        }
        """
        with self.assertRaises(GrovestreamsDataIntegrityException):
            p.parse_intervals(bad_text)

    def test_interval_parse(self):
        """The parser returns a chronologically sorted list of Interval tuples for valid data."""
        with open(
            os.path.join(TEST_DATA_DIR, "fixtures", "test_grovestreams_data.json")
        ) as dataset:
            text = dataset.read()

        stream_id, intervals = p.parse_intervals(text)

        self.assertEqual(stream_id, "bb809707-e759-34bd-bb3c-1e13d1ab3b7c")

        expected_kwhs = [0, 1, 2, 3, 4] + 123 * [0]
        actual_kwhs = [ivl.kwh for ivl in intervals]
        self.assertEqual(expected_kwhs, actual_kwhs)

        start_dt = datetime(2019, 1, 27, tzinfo=tzutc())
        expected_starts = [
            start_dt + timedelta(minutes=15 * ii) for ii in range(0, 128)
        ]
        actual_starts = [ivl.start for ivl in intervals]
        self.assertEqual(expected_starts, actual_starts)

        expected_ends = [start_dt + timedelta(minutes=15 * ii) for ii in range(1, 129)]
        actual_ends = [ivl.end for ivl in intervals]
        self.assertEqual(expected_ends, actual_ends)
