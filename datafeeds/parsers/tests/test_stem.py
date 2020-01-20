import unittest
from unittest.mock import patch
import json

from datetime import datetime

from datafeeds.scrapers.stem import Session, ApiError
from datafeeds.parsers.base import JsonParseFailure, SchemaValidationFailure
from datafeeds.parsers import stem as parsers
from datafeeds.parsers.stem import Site, UTC


class ClientParseTests(unittest.TestCase):
    def test_parse_fail_json(self):
        """The STEM parser raises an exception when the response is not JSON."""
        data = """{abc123:"""
        with self.assertRaises(JsonParseFailure):
            parsers.parse_clients(data)

    def test_parse_fail_schema1(self):
        data = """
        {
            "total": 1,
            "clients": [
                {
                    "link": "/api/v1/clients/guid1",
                    "id": "12345",
                    "name": "Dave's Oyster Barn",
                    "foo": "Schema violation."
                }
            ],
            "page": "1"
        }"""

        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_clients(data)

    def test_parse_fail_schema2(self):
        data = """
        {
            "total": 1,
            "clients": [
                {
                    "link": "/api/v1/clients/guid1",
                    "id": "12345",
                    "name": "Dave's Oyster Barn"
                }
            ]
        }"""

        # Missing required page field.
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_clients(data)

    def test_parse_fail_schema3(self):
        data = """
        {
            "total": 1,
            "clients": [
                {
                    "link": "/api/v1/clients/guid1",
                    "id": "12345"
                }
            ],
            "page": "1"
        }"""

        # Missing required name field.
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_clients(data)

    def test_parse_empty(self):
        data = """
        {
            "total": 1,
            "clients": [],
            "page": "1"
        }"""

        clients = parsers.parse_clients(data)
        self.assertEqual(len(clients), 0)

    def test_parse_success(self):
        """The STEM parser generates a list of clients for JSON passing schema."""
        data = """
        {
            "total": 1,
            "clients": [
                {
                    "link": "/api/v1/clients/guid1",
                    "id": "12345",
                    "name": "Dave's Oyster Barn"
                },
                {
                    "link": "/api/v1/clients/guid2",
                    "id": "abcde",
                    "name": "Orlando's House of Chicken"
                }
            ],
            "page": "1"
        }"""

        clients = parsers.parse_clients(data)
        self.assertEqual(len(clients), 2)

        self.assertEqual(clients[0].id, "12345")
        self.assertEqual(clients[0].link, "/api/v1/clients/guid1")
        self.assertEqual(clients[0].name, "Dave's Oyster Barn")


class SiteParseTests(unittest.TestCase):
    def test_parse_fail_json(self):
        """The STEM parser raises an exception when the response is not JSON."""
        data = """{abc123:"""
        with self.assertRaises(JsonParseFailure):
            parsers.parse_sites(data)

    def test_parse_fail_schema1(self):
        data = """
        {
            "total": 1,
            "sites": [
                {
                    "stream_start_times": {
                        "MONITOR": "2016-10-27T00:37:31Z",
                        "CONVERTER": "2016-10-27T01:41:58Z"
                    },
                    "link": "/api/v1/sites/guid3",
                    "name": "Gunther's Sushi Wagon"
                }
            ],
            "page": "1"
        }
        """
        # Missing id field
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_sites(data)

    def test_parse_fail_schema2(self):
        data = """
        {
            "total": 1,
            "sites": {
                    "stream_start_times": {
                        "MONITOR": "2016-10-27T00:37:31Z",
                        "CONVERTER": "2016-10-27T01:41:58Z"
                    },
                    "id": "12345",
                    "link": "/api/v1/sites/guid3",
                    "name": "Gunther's Sushi Wagon"
            },
            "page": "1"
        }
        """
        # Missing id field
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_sites(data)

    def test_parse_fail_schema3(self):
        data = """
        {
            "total": 1,
            "sites": {
                    "stream_start_times": {
                        "CONVERTER": "2016-10-27T01:41:58Z"
                    },
                    "id": "12345",
                    "link": "/api/v1/sites/guid3",
                    "name": "Gunther's Sushi Wagon"
            },
            "page": "1"
        }
        """
        # Missing MONITOR field
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_sites(data)

    def test_parse_fail_schema4(self):
        data = """
        {
            "total": 1,
            "sites": [
                {
                    "stream_start_times": {
                        "MONITOR": "Not a date"
                    },
                    "link": "/api/v1/sites/guid3",
                    "id": "abcd1234",
                    "name": "Megan's Pancake Hut"
                }
            ],
            "page": "1"
        }
        """
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_sites(data)

    def test_parse_success(self):
        """The STEM parser generates a list of sites for JSON passing schema."""
        data = """
        {
            "total": 1,
            "sites": [
                {
                    "stream_start_times": {
                        "MONITOR": "2016-10-27T00:37:31Z",
                        "CONVERTER": "2016-10-27T01:41:58Z"
                    },
                    "link": "/api/v1/sites/guid3",
                    "id": "abcd1234",
                    "name": "Leroy's Pizza Shack"
                },
                {
                    "stream_start_times": {
                        "CONVERTER": "2016-10-27T01:41:58Z"
                    },
                    "link": "/api/v1/sites/guid3",
                    "id": "abcd1234",
                    "name": "Arshed's Wing Wagon"
                }
            ],
            "page": "1"
        }
        """

        sites = parsers.parse_sites(data)

        # Sites without MONITOR should be dropped; they have no
        # interval data..
        self.assertEqual(len(sites), 1)

        self.assertEqual(sites[0].link, "/api/v1/sites/guid3")
        self.assertEqual(sites[0].name, "Leroy's Pizza Shack")
        self.assertEqual(sites[0].start, datetime(2016, 10, 27, 00, 37, 31, tzinfo=UTC))


class StreamParseTests(unittest.TestCase):
    def test_parse_fail_json(self):
        """The STEM parser raises an exception when the response is not JSON."""
        data = """{abc123:"""
        with self.assertRaises(JsonParseFailure):
            parsers.parse_intervals(data, "MONITOR")

    def test_parse_fail_schema1(self):
        data = """
        {
            "stream_types": [
                {
                    "streams": [
                        {
                            "start_datetime": "2018-01-01T00:00:01Z",
                            "kw_total_avg": 411.899,
                            "end_datetime": "2018-01-01"
                        }
                    ],
                    "stream_type": "MONITOR"
                }
            ],
            "id": "abcd9876",
            "name": "Anodyne Industries LLC"
        }
        """

        # Missing time on end datetime.
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data, "MONITOR")

    def test_parse_fail_schema2(self):
        data = """
        {
            "stream_types": [
                {
                    "streams": [
                        {
                            "start_datetime": "2018-01-01T00:00:01Z",
                            "end_datetime": "2018-01-01T00:15:00Z"
                        }
                    ],
                    "stream_type": "MONITOR"
                }
            ],
            "id": "abcd9876",
            "name": "Anodyne Industries LLC"
        }
        """
        # Missing KW
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data, "MONITOR")

    def test_parse_fail_schema3(self):
        data = """
        {
            "stream_types": [
                {
                    "streams": [
                        {
                            "start_datetime": "2018-01-01T00:00:01Z",
                            "kw_total_avg": "Help, I'm trapped in an interval factory...",
                            "end_datetime": "2018-01-01T00:15:00Z"
                        }
                    ],
                    "stream_type": "MONITOR"
                }
            ],
            "id": "abcd9876",
            "name": "Anodyne Industries LLC"
        }
        """
        # KW not a number
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data, "MONITOR")

    def test_parse_fail_schema4(self):
        data = """
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
                    "stream": "MONITOR"
                }
            ],
            "id": "abcd9876",
            "name": "Anodyne Industries LLC"
        }
        """
        # stream should be stream_type
        with self.assertRaises(SchemaValidationFailure):
            parsers.parse_intervals(data, "MONITOR")

    def test_parse_success(self):
        """The STEM parser generates a list of intervals for JSON passing schema."""
        data = """
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
                },
                {
                    "streams": [
                        {
                            "start_datetime": "2018-01-01T00:00:01Z",
                            "kw_total_avg": 1234,
                            "end_datetime": "2018-01-01T00:15:00Z"
                        },
                        {
                            "start_datetime": "2018-01-01T00:15:01Z",
                            "kw_total_avg": 6789,
                            "end_datetime": "2018-01-01T00:30:00Z"
                        },
                        {
                            "start_datetime": "2018-01-01T00:30:01Z",
                            "kw_total_avg": 10123,
                            "end_datetime": "2018-01-01T00:45:00Z"
                        }
                    ],
                    "stream_type": "CONVERTER"
                },
                {
                    "streams": [
                        {
                            "start_datetime": "2018-02-01T00:00:01Z",
                            "kw_total_avg": 422.899,
                            "end_datetime": "2018-01-01T00:15:00Z"
                        },
                        {
                            "start_datetime": "2018-02-01T00:15:01Z",
                            "kw_total_avg": 433.82892,
                            "end_datetime": "2018-01-01T00:30:00Z"
                        },
                        {
                            "start_datetime": "2018-02-01T00:30:01Z",
                            "kw_total_avg": 444.60704,
                            "end_datetime": "2018-01-01T00:45:00Z"
                        }
                    ],
                    "stream_type": "MONITOR"
                }
            ],
            "id": "abcd9876",
            "name": "Anodyne Industries LLC"
        }
        """

        intervals = parsers.parse_intervals(data, "MONITOR")

        # Parser concatenates monitor records and drops all others.
        self.assertEqual(len(intervals), 6)

        self.assertEqual(intervals[1].start, datetime(2018, 1, 1, 0, 15, 1, tzinfo=UTC))
        self.assertEqual(intervals[1].end, datetime(2018, 1, 1, 0, 30, 0, tzinfo=UTC))
        self.assertEqual(intervals[1].kw, 411.82892)


class MockHttpResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.text = body


class SessionTests(unittest.TestCase):
    @patch("requests.get")
    def test_utc_check(self, requests_get):
        """The module checks that all datetimes are UTC."""
        site = Site(link="", id="", name="", start=datetime(2018, 1, 1, tzinfo=UTC))
        sess = Session("API_BASE", "API_KEY")

        good_date = datetime(2018, 2, 1, tzinfo=UTC)
        bad_date = datetime(2018, 2, 28)
        with self.assertRaises(ValueError):
            sess.get_stream(site, good_date, bad_date, "MONITOR")

    @patch("requests.get")
    def test_client_paging(self, requests_get):
        """The module gathers client data over multiple pages."""

        def _make_page(id_start, id_stop):
            return json.dumps(
                {
                    "total": 1,
                    "clients": [
                        {
                            "link": "/api/v1/clients/guid1",
                            "id": "%d" % ii,
                            "name": "Dave's Oyster Barn",
                        }
                        for ii in range(id_start, id_stop)
                    ],
                    "page": "1",
                }
            )

        requests_get.side_effect = [
            MockHttpResponse(200, _make_page(*p)) for p in [(0, 100), (100, 150)]
        ]

        sess = Session("API_BASE", "API_KEY")
        clients = sess.clients()
        self.assertEqual(len(clients), 150)

        # Check we really did get every record back.
        for ii in range(0, 150):
            self.assertEqual(clients[ii].id, str(ii))

    @patch("requests.get")
    def test_client_status_exceptions(self, requests_get):
        """The module throws an exception on an unexpected status code."""
        requests_get.side_effect = [MockHttpResponse(207, "some nonsense")]

        sess = Session("API_BASE", "API_KEY")
        with self.assertRaises(ApiError):
            sess.clients()

    @patch("requests.get")
    def test_site_status_exceptions(self, requests_get):
        """The module throws an exception on an unexpected status code."""
        requests_get.side_effect = [MockHttpResponse(207, "some nonsense")]

        sess = Session("API_BASE", "API_KEY")
        with self.assertRaises(ApiError):
            sess.sites("mock_client_id")

    @patch("requests.get")
    def test_site_paging(self, requests_get):
        """The module gathers site data over multiple pages."""

        def _make_page(id_start, id_stop):
            return json.dumps(
                {
                    "total": 1,
                    "sites": [
                        {
                            "stream_start_times": {
                                "MONITOR": "2016-10-27T00:37:31Z",
                                "CONVERTER": "2016-10-27T01:41:58Z",
                            },
                            "link": "/api/v1/sites/guid3",
                            "id": "%d" % ii,
                            "name": "Leroy's Pizza Shack",
                        }
                        for ii in range(id_start, id_stop)
                    ],
                    "page": "1",
                }
            )

        requests_get.side_effect = [
            MockHttpResponse(200, _make_page(*p)) for p in [(0, 100), (100, 150)]
        ]

        sess = Session("API_BASE", "API_KEY")
        sites = sess.sites("mock_client_id")
        self.assertEqual(len(sites), 150)

        # Check we really did get every record back.
        for ii in range(0, 150):
            self.assertEqual(sites[ii].id, str(ii))

    @patch("requests.get")
    def test_interval_paging(self, requests_get):
        """For larger date ranges, the module gathers interval data month by
        month."""

        # For this test, dates in the dummy data don't matter.
        page = """
        {
            "stream_types": [{
                "streams": [
                    {
                        "start_datetime": "2018-01-01T00:15:00Z",
                        "kw_total_avg": 411.899,
                        "end_datetime": "2018-01-01T00:15:00Z"
                    }
                ],
                "stream_type": "MONITOR"
            }],
            "id": "abcd9876",
            "name": "Anodyne Industries LLC"
        }
        """

        datetimes = [datetime(2018, ii, 1) for ii in range(1, 10)]
        requests_get.side_effect = [MockHttpResponse(200, page) for _ in datetimes]

        sess = Session("API_BASE", "API_KEY")
        site = Site(link="", id="", name="", start=datetime(2017, 1, 1, tzinfo=UTC))
        start = datetime(2018, 1, 1, tzinfo=UTC)
        end = datetime(2018, 6, 15, tzinfo=UTC)
        intervals = sess.get_stream(site, start, end, "MONITOR")

        # 10 months of interval data are mocked, but we should only request 6.
        self.assertEqual(len(intervals), 6)

    @patch("requests.get")
    def test_intervals_status_exceptions(self, requests_get):
        """The module throws an exception on an unexpected status code."""
        requests_get.side_effect = [MockHttpResponse(207, "some nonsense")]

        sess = Session("API_BASE", "API_KEY")
        site = Site(link="", id="", name="", start=datetime(2017, 1, 1, tzinfo=UTC))
        start = datetime(2018, 1, 1, tzinfo=UTC)
        end = datetime(2018, 6, 15, tzinfo=UTC)
        with self.assertRaises(ApiError):
            sess.get_stream(site, start, end, "MONITOR")
