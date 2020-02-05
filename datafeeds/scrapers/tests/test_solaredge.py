from datetime import datetime
import json
import unittest
from unittest.mock import patch

from datafeeds.common.exceptions import ApiError
from datafeeds.parsers.base import JsonParseFailure, SchemaValidationFailure
from datafeeds.parsers import solaredge as parsers
from datafeeds.scrapers.solaredge import Session
from datafeeds.scrapers.tests.data.solaredge import meter_example, site_details


class SiteParseTests(unittest.TestCase):
    def test_parse_fail_json(self):
        """The parser raises an exception when the response is not JSON."""
        data = """{abc123:"""
        with self.assertRaises(JsonParseFailure):
            parsers.parse_site(data)

    def test_parse_fail_schema1(self):
        """Additional properties is set to false"""
        data = """
        {
            "details": {
                "name": "Easy St",
                "accountId": 123456,
                "status": "Active",
                "schema_violation": "Yes",
                "installationDate": "2019-09-24"
                }
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_site(data)

    def test_parse_fail_schema2(self):
        """Missing required timeZone field"""
        data = """
        {
            "details": {
                "id": "1234",
                "name": "Easy St",
                "accountId": 123456,
                "status": "Active",
                "installationDate": "2019-09-24"
            }
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_site(data)

    def test_parse_fail_schema3(self):
        """Missing required name field"""
        data = """
        {
            "details": {
                "id": "1234",
                "accountId": 123456,
                "status": "Active",
                "installationDate": 42,
                "location": {
                    "country": "United States",
                    "state": "California",
                    "city": "San Francisco"
                }
            }
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_site(data)

    def test_parse_empty(self):
        """Time zone field is present but empty"""
        data = """
        {
            "details": {
                "id": 1234,
                "name": "Bob",
                "accountId": 123456,
                "status": "Active",
                "installationDate": "2019-09-24",
                "location": {
                    "country": "United States",
                    "state": "California",
                    "city": "San Francisco",
                    "timeZone": ""
                }
            }
        }"""

        sites = parsers.parse_site(data)
        self.assertEqual(len(sites.time_zone), 0)

    def test_success(self):
        data = """
            {
                "details": {
                    "id": 1234,
                    "name": "Bob",
                    "accountId": 123456,
                    "status": "Active",
                    "installationDate": "2019-09-24",
                    "location": {
                        "country": "United States",
                        "state": "California",
                        "city": "San Francisco",
                        "timeZone": "America/Los_Angeles"
                    }
                }
            }"""

        site_details = parsers.parse_site(data)
        self.assertEqual(site_details.id, 1234)
        self.assertEqual(site_details.time_zone, "America/Los_Angeles")
        self.assertEqual(site_details.installation_date, "2019-09-24")
        self.assertEqual(site_details.name, "Bob")


class IntervalParseTests(unittest.TestCase):
    def test_parse_fail_json(self):
        """The parser raises an exception when the response is not JSON."""
        data = """{abc123:"""
        with self.assertRaises(JsonParseFailure):
            parsers.parse_intervals(data)

    def test_parse_fail_schema1(self):
        """Missing time on date."""
        data = """
        {
            "meterEnergyDetails": {
                "timeUnit": "QUARTER_OF_AN_HOUR",
                "unit": "Wh",
                "meters": [
                    {
                        "meterSerialNumber": "4154666",
                        "connectedSolaredgeDeviceSN": "670041DC-84",
                        "model": "RWND-3D-480-MB",
                        "meterType": "Production",
                        "values": [
                            {
                                "date": "2019-11-01",
                                "value": 1.2417916E7
                            }
                        ]
                    }
                ]
            }
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data)

    def test_parse_fail_schema2(self):
        """kwh not a number - fails during conversion from wh to kwh"""
        data = """
        {
            "meterEnergyDetails": {
                "timeUnit": "QUARTER_OF_AN_HOUR",
                "unit": "Wh",
                "meters": [
                    {
                        "meterSerialNumber": "4154666",
                        "connectedSolaredgeDeviceSN": "670041DC-84",
                        "model": "RWND-3D-480-MB",
                        "meterType": "Production",
                        "values": [
                            {
                                "date": "2019-11-01 11:04:16",
                                "value": "not a number"
                            }
                        ]
                    }
                ]
            }
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data)

    def test_parse_fail_schema3(self):
        """Must have meterSerialNumber"""
        data = """
        {
            "meterEnergyDetails": {
                "timeUnit": "QUARTER_OF_AN_HOUR",
                "unit": "Wh",
                "meters": [
                    {
                        "meterNumber": "4154666",
                        "connectedSolaredgeDeviceSN": "670041DC-84",
                        "model": "RWND-3D-480-MB",
                        "meterType": "Production",
                        "values": [
                            {
                                "date": "2019-11-01",
                                "value": 1.2417916E7
                            }
                        ]
                    }
                ]
            }
        }"""
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data)

    def test_parse_fail_schema4(self):
        """Missing date field"""
        data = """
        {
            "meterEnergyDetails": {
                "timeUnit": "QUARTER_OF_AN_HOUR",
                "unit": "Wh",
                "meters": [
                    {
                        "meterSerialNumber": "4154666",
                        "connectedSolaredgeDeviceSN": "670041DC-84",
                        "model": "RWND-3D-480-MB",
                        "meterType": "Production",
                        "values": [
                            {
                                "value": 1.2417916E7
                            }
                        ]
                    }
                ]
            }
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data)

    def test_parse_success(self):
        """Valid data passes"""
        data = """
        {
            "meterEnergyDetails": {
                "timeUnit": "QUARTER_OF_AN_HOUR",
                "unit": "Wh",
                "meters": [
                    {
                        "meterSerialNumber": "4154666",
                        "connectedSolaredgeDeviceSN": "670041DC-84",
                        "model": "RWND-3D-480-MB",
                        "meterType": "Production",
                        "values": [
                            {
                                "date": "2019-11-01 11:04:16",
                                "value": 1.5655772E7
                            },
                            {
                                "date": "2019-11-01 11:19:15",
                                "value": 1.5682206E7
                            }
                        ]
                    },
                    {
                        "meterSerialNumber": "4161538",
                        "connectedSolaredgeDeviceSN": "7E14969E-C6",
                        "model": "RWND-3D-480-MB",
                        "meterType": "Production",
                        "values": [
                            {
                                "date": "2019-11-01 11:01:34",
                                "value": 1.2396539E7
                            },
                            {
                                "date": "2019-11-01 11:16:34",
                                "value": 1.2417916E7
                            }
                        ]
                    }
                ]
            }
        }"""

        intervals = parsers.parse_intervals(data)
        # 2 readings from each meter are loaded
        self.assertEqual(len(intervals), 4)

        self.assertEqual(intervals[0].start, datetime(2019, 11, 1, 11, 4, 16))
        # kwh is a number
        self.assertEqual(intervals[0].kwh / 1000, 15.655772)


class MockHttpResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.text = body


class SessionTests(unittest.TestCase):
    @patch("requests.get")
    def test_site_status_exceptions(self, requests_get):
        """The module throws an exception on an unexpected status code."""
        requests_get.side_effect = [MockHttpResponse(207, "some nonsense")]

        sess = Session("API_BASE", "API_KEY")
        with self.assertRaises(ApiError):
            sess.site()

    @patch("requests.get")
    def test_site_returns_data(self, requests_get):
        """Module returns site data"""
        sd = json.loads(site_details.site_details)

        requests_get.side_effect = [MockHttpResponse(200, json.dumps(sd))]
        sess = Session("API_BASE", "API_KEY")
        site = sess.site()
        self.assertEqual(site.id, 12345678)

    @patch("requests.get")
    def test_interval_returns_data(self, requests_get):
        """Module returns interval data"""
        mx = json.loads(meter_example.meter_example)

        requests_get.side_effect = [MockHttpResponse(200, json.dumps(mx))]
        sess = Session("API_BASE", "API_KEY")
        ivls = sess.get_intervals(
            "api_base",
            datetime(2019, 11, 1, 11, 4, 16),
            datetime(2019, 11, 2, 11, 4, 16),
            "2019-01-01",
        )
        self.assertEqual(ivls[0][0].kwh, 15655.772)

    @patch("requests.get")
    def test_interval_status_exceptions(self, requests_get):
        """The module throws an exception on an unexpected status code."""
        requests_get.side_effect = [MockHttpResponse(207, "some nonsense")]

        sess = Session("API_BASE", "API_KEY")
        with self.assertRaises(ApiError):
            sess.get_intervals(
                "api_base",
                datetime(2019, 11, 1, 11, 4, 16),
                datetime(2019, 11, 2, 11, 4, 16),
                "2019-01-01",
            )

    @patch("requests.get")
    def test_interval_returns_data_multiple_months(self, requests_get):
        """For larger date ranges, the module gathers interval data month by
        month."""
        mx = json.loads(meter_example.meter_example)
        sess = Session("API_BASE", "API_KEY")
        datetimes = [datetime(2019, ii, 1) for ii in range(1, 10)]
        requests_get.side_effect = [
            MockHttpResponse(200, json.dumps(mx)) for dt in datetimes
        ]
        ivls = sess.get_intervals(
            "api_base",
            datetime(2019, 1, 1, 11, 4, 16),
            datetime(2019, 6, 2, 11, 4, 16),
            "2019-01-01",
        )
        # Example will produce 4 readings for each month
        self.assertEqual(len(ivls), 24)
