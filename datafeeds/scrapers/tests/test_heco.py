import logging
import unittest
from unittest import mock

from datetime import datetime, timedelta
from datafeeds.scrapers import heco_interval as heco

TEST_DATE_FORMAT = "%Y-%m-%d"
TEST_TIME_FORMAT = "%H:%M"


class SetupBase(object):
    def setUp(self):
        self.driver = mock.Mock()
        self.start_date = self._build_datetime("2019-01-01")
        self.end_date = self._build_datetime("2019-12-31")
        heco.logger = logging.getLogger()

    def _build_datetime(self, desired_date):
        return datetime.strptime(desired_date, TEST_DATE_FORMAT)


class TestHecoIntervalPageObject(SetupBase, unittest.TestCase):
    def test_format_date(self):
        self.assertEqual(heco.IntervalForm._format_date(self.start_date), "01/01/2019")

    def test_backup_start_date(self):
        self.assertEqual(heco.IntervalForm._backup_start_date(self.start_date), self._build_datetime(
            "2018-12-31"
        ))


class TestHecoAvailableDatesPageObject(SetupBase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.available_dates_comp = heco.AvailableDateComponent(self.driver)

    def _extract_available_dates(self):
        # Mock Method for AvailableDateComponent._extract_available_dates
        return self.start_date, self._build_datetime("2019-12-31")

    def test_adjust_start_and_end_dates(self):
        too_small_date = self._build_datetime("2000-06-30")
        too_large_date = self._build_datetime("2030-01-01")
        acceptable_start = self._build_datetime("2019-06-01")
        acceptable_end = self._build_datetime("2019-07-01")

        with mock.patch.object(heco.AvailableDateComponent, "_extract_available_dates",
                               new=self._extract_available_dates):
            self.assertEqual(self.available_dates_comp.adjust_start_and_end_dates(
                too_small_date, too_large_date), self._extract_available_dates())

            adj_start, adj_end = self.available_dates_comp.adjust_start_and_end_dates(
                acceptable_start, acceptable_end)

            self.assertEqual(adj_start, acceptable_start)
            self.assertEqual(adj_end, acceptable_end)


class HECOScraperTest(SetupBase, unittest.TestCase):

    def _build_time_array(self):
        quarter_hour = datetime.strptime('00:00', TEST_TIME_FORMAT)

        times = ['00:00']
        for _ in range(0, 96):
            quarter_hour = quarter_hour + timedelta(minutes=15)
            times.append(
                datetime.strftime(quarter_hour, TEST_TIME_FORMAT)
            )
        return times

    def _build_demand_array(self):
        return [500 for i in range(96)]

    def test_get_header_position(self):
        test_header = [
            "  Date / Time ",
            "KW(ch:1 set:0)   ",
            "   KVA(0(ch: 2  set:0)",
            "PF(0(ch: 3  set:0)"]

        self.assertEqual(heco.HECOScraper._get_header_position(
            test_header, "kw"
        ), 1)
        self.assertEqual(heco.HECOScraper._get_header_position(
            test_header, "date"
        ), 0)

    def test_remove_incomplete_demand_data(self):
        time_array = self._build_time_array()
        demand_array = self._build_demand_array()

        all_present = "2019-01-01"
        demands_missing = "2019-02-02"
        time_incorrect = "2019-03-03"

        response = {
            all_present: {
                heco.TIME: time_array,
                heco.DEMAND: demand_array
            },
            demands_missing: {
                heco.TIME: time_array,
                heco.DEMAND: [1001]
            },
            time_incorrect: {
                heco.TIME: time_array.pop(),
                heco.DEMAND: demand_array
            }
        }

        heco.HECOScraper._remove_incomplete_demand_data(response, all_present)
        self.assertIn(all_present, response)

        heco.HECOScraper._remove_incomplete_demand_data(response, demands_missing)
        self.assertNotIn(demands_missing, response)

        heco.HECOScraper._remove_incomplete_demand_data(response, time_incorrect)
        self.assertNotIn(time_incorrect, response)

    def test_format_time(self):
        current_time = datetime.strptime('1:15', TEST_TIME_FORMAT)
        self.assertEqual(heco.HECOScraper._format_time(current_time), '01:15')

    def test_finalize_readings(self):
        times = self._build_time_array()
        demands = self._build_demand_array()

        demand_date = "2019-01-01"

        response = {
            demand_date: {
                heco.TIME: times,
                heco.DEMAND: demands
            }
        }

        final_readings = heco.HECOScraper._finalize_readings(response)

        self.assertIn(demand_date, final_readings)
        self.assertEqual(len(final_readings[demand_date]), 96)
        self.assertEqual(final_readings[demand_date][0], 500)
