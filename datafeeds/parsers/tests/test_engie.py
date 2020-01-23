from datetime import datetime, timedelta
import os
import unittest

from dateutil.tz import tzutc

from datafeeds.parsers.base import (
    JsonParseFailure,
    SchemaValidationFailure,
    DataIntegrityFailure,
    KwInterval as Interval,
)
from datafeeds.parsers import engie as p

TEST_DATA_DIR = os.path.dirname(os.path.abspath(__file__))


class IntervalParseTestCase(unittest.TestCase):
    def test_json_parse_failure(self):
        """The parser raises an exception for text that doesn't parse as JSON."""
        bad_text = '{"first": }'
        with self.assertRaises(JsonParseFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_no_window(self):
        """The parser raises a schema exception when interval size is not reported."""
        bad_text = """
        {
           "endPoints": [{
                "key": 1234567890,
                "name": "xstfy7hq.virtual.building",
                "first": 1546329600000,
                "last": 1546415100000,
                "values": [3055680, 3047040, 3029760]
            }]
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_no_endpoints(self):
        """The parser raises a schema exception if no timeseries are present."""
        bad_text = """
        {
           "first": "1546329600000",
           "last": "1546416000000",
           "level": 1,
           "window": "900,900"
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_no_name_in_endpoint(self):
        """The parser raises a schema exception if a timeseries doesn't have a name."""
        bad_text = """
        {
           "first": "1546329600000",
           "last": "1546416000000",
           "level": 1,
           "window": "900,900",
           "endPoints": [{
                "key": 1234567890,
                "first": 1546329600000,
                "last": 1546415100000,
                "values": [3055680, 3047040, 3029760]
            }]
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_no_first_in_endpoint(self):
        """The parser raises an exception if the timeseries start timestamp isn't available."""
        bad_text = """
        {
           "first": "1546329600000",
           "last": "1546416000000",
           "level": 1,
           "window": "900,900",
           "endPoints": [{
                "key": 1234567890,
                "name": "xstfy7hq.virtual.building",
                "last": 1546415100000,
                "values": [3055680, 3047040, 3029760]
            }]
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_no_last_in_endpoint(self):
        """The parser raises an exception if the timeseries end timestamp isn't available."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "xstfy7hq.virtual.building",
                 "first": 1546329600000,
                 "values": [3055680, 3047040, 3029760]
             }]
         }
         """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_first_has_wrong_type(self):
        """The parser raises an exception if the timeseries end timestamp isn't available."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "xstfy7hq.virtual.building",
                 "first": "1546329600000",
                 "last": 1546415100000,
                 "values": [3055680, 3047040, 3029760]
             }]
         }
         """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_last_has_wrong_type(self):
        """The parser raises an exception if the timeseries end timestamp isn't available."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "xstfy7hq.virtual.building",
                 "first": 1546329600000,
                 "last": "1546415100000",
                 "values": [3055680, 3047040, 3029760]
             }]
         }
         """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_no_values_in_endpoint(self):
        """The parser raises an exception if the timeseries doesn't contain interval data."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "xstfy7hq.virtual.building",
                 "first": 1546329600000,
                 "last": 1546415100000
             }]
         }
         """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_schema_validation_failure_bad_values_in_endpoint(self):
        """The parser raises an exception if interval data isn't Option(number) valued."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "xstfy7hq.virtual.building",
                 "first": 1546329600000,
                 "last": 1546415100000,
                 "values": [3055680, 3047040, "foo"]
             }]
         }
         """
        with self.assertRaises(SchemaValidationFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_incoherent_interval_dates(self):
        """The parser raises an exception if the number of intervals doesn't match the time window."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "xstfy7hq.virtual.building",
                 "first": 1546329600000,
                 "last": 1546415100000,
                 "values": [3055680, 3047040, 3029760]
             }]
         }
         """
        with self.assertRaises(DataIntegrityFailure) as context:
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

        self.assertTrue(
            "Timeseries start/end dates do not match the number of intervals."
            in str(context.exception)
        )
        self.assertTrue("Intervals: 3" in str(context.exception))

    def test_incoherent_endpoints(self):
        """The parser raises an exception if there is no timeseries for the input site identifier."""
        bad_text = """
         {
            "first": "1546329600000",
            "last": "1546416000000",
            "level": 1,
            "window": "900,900",
            "endPoints": [{
                 "key": 1234567890,
                 "name": "abcdef.virtual.building",
                 "first": 1546329600000,
                 "last": 1546415100000,
                 "values": [3055680, 3047040, 3029760]
             }]
         }
         """
        with self.assertRaises(DataIntegrityFailure):
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

    def test_empty_endpoints(self):
        """The parser raises an exception if there is no timeseries for the input site identifier."""
        bad_text = """
            {
               "first": "1546329600000",
               "last": "1546416000000",
               "level": 1,
               "window": "900,900",
               "endPoints": []
            }
            """
        with self.assertRaises(DataIntegrityFailure) as context:
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

        self.assertTrue("Could not obtain a valid timeseries" in str(context.exception))

    def test_multiple_endpoints(self):
        """The parser raises an exception if there is no timeseries for the input site identifier."""
        bad_text = """
            {
               "first": "1546329600000",
               "last": "1546416000000",
               "level": 1,
               "window": "900,900",
               "endPoints": [{
                    "key": 1234567890,
                    "name": "xstfy7hq.virtual.building",
                    "first": 1546329600000,
                    "last": 1546415100000,
                    "values": [3055680, 3047040, 3029760]
                },
                {
                     "key": 1234567890,
                     "name": "xstfy7hq.virtual.building",
                     "first": 1546329600000,
                     "last": 1546415100000,
                     "values": [1, 2, 3]
                }]
            }
            """
        with self.assertRaises(DataIntegrityFailure) as context:
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

        self.assertTrue(
            "Failed to determine a canonical timeseries" in str(context.exception)
        )

    def test_invalid_window(self):
        """The parser raises an exception if the interval width parameter can't be processed."""
        bad_text = """
            {
               "first": "1546329600000",
               "last": "1546416000000",
               "level": 1,
               "window": "abcd",
               "endPoints": []
            }
            """
        with self.assertRaises(DataIntegrityFailure) as context:
            p.parse_intervals("xstfy7hq.virtual.building", bad_text)

        self.assertTrue("Failed to locate interval width" in str(context.exception))

    def test_parse(self):
        """The parser reports a chronologically ordered list of interval tuples for valid data."""

        with open(
            os.path.join(TEST_DATA_DIR, "fixtures", "test_engie_data.json")
        ) as dataset:
            text = dataset.read()

        watts = [
            3055680,
            3029760,
            3021120,
            2796480,
            3047040,
            3055680,
            3029760,
            3035520,
            3041280,
            3038400,
            3061440,
            3026880,
            3021120,
            2989440,
            2995200,
            3078720,
            3052800,
            3044160,
            3110400,
            3127680,
            3159359.988,
            3110400,
            3119040,
            3193920,
            3191040,
            3240000,
            3222720,
            3248640,
            3254400,
            3280320,
            3196799.988,
            3130560,
            3096000,
            3055680,
            3058560,
            2995200,
            3000960,
            2992320,
            2934720,
            2897280,
            2877120,
            2865600,
            2825280,
            2816640,
            2779200,
            2764800,
            2770560,
            2773440,
            2796480,
            2770560,
            2782080,
            2808000,
            2819520,
            2822400,
            2894400,
            2894400,
            2894400,
            2937600,
            2972160,
            3015360,
            3038400,
            3107519.988,
            3110400,
            3421440,
            3467520,
            3392640,
            3297600,
            3343680,
            3340800,
            3335040,
            3329280,
            3274560,
            3329280,
            3312000,
            3291840,
            3297600,
            3280320,
            3277440,
            3265920,
            3271680,
            3242880,
            3231360,
            3283200,
            3179519.988,
            3162239.988,
            3153600,
            3130560,
            3119040,
            3104640,
            3096000,
            3107519.988,
            3090239.988,
            3107519.988,
            3075840,
            3113280,
        ]

        start_dt = datetime(2019, 1, 1, 8, tzinfo=tzutc())
        kws = [w / 1000.0 for w in watts]
        starts = [
            start_dt + timedelta(minutes=15 * ii) for ii in range(0, 96) if ii != 1
        ]
        ends = [
            start_dt + timedelta(minutes=15 * (ii + 1))
            for ii in range(0, 96)
            if ii != 1
        ]

        expected_intervals = [
            Interval(start=x, end=y, kw=z) for x, y, z in zip(starts, ends, kws)
        ]
        actual_intervals = p.parse_intervals("xstfy7hq.virtual.building", text)

        self.assertEqual(actual_intervals, expected_intervals)
