import unittest
from unittest import mock

from datetime import datetime, timedelta
from datafeeds.common.support import DateRange, Configuration
from datafeeds.scrapers import solren

TEST_DATE_FORMAT = "%Y-%m-%d"
TEST_TIME_FORMAT = "%H:%M"


class SetupBase(unittest.TestCase):
    def setUp(self):
        self.driver = mock.Mock()
        self.start_date = self._build_datetime("2019-01-01")
        self.end_date = self._build_datetime("2019-12-31")

    def _build_datetime(self, desired_date):
        return datetime.strptime(desired_date, TEST_DATE_FORMAT)


class TestCSVParser(SetupBase):
    def setUp(self):
        super().setUp()
        self.inverter_id = "12345"
        self.filepath = "/"
        self.csv_parser = solren.CSVParser(self.inverter_id, self.filepath)

    def test_get_header_position(self):
        header_row = ["Label", "Inverter 1 [12345 PVI 60TL] - AC Power (kW)"]
        self.assertEqual(self.csv_parser._get_header_position(header_row, "label"), 0)
        self.assertEqual(self.csv_parser._get_header_position(header_row, "kw"), 1)

    def test_csv_str_to_date(self):
        csv_date = "2019-Jan-01 6:25 am"
        date_obj = self.csv_parser.csv_str_to_date(csv_date)
        self.assertEqual(date_obj.month, 1)
        self.assertEqual(date_obj.day, 1)
        self.assertEqual(date_obj.year, 2019)
        self.assertEqual(date_obj.hour, 6)
        self.assertEqual(date_obj.minute, 25)

        csv_date = "2019-Jan-01 6:25 pm"
        date_obj = self.csv_parser.csv_str_to_date(csv_date)
        self.assertEqual(date_obj.hour, 18)
        self.assertEqual(date_obj.minute, 25)

    def test_date_to_final_str(self):
        self.assertEqual(self.csv_parser.date_to_final_str(self.start_date), "2019-01-01")
        self.assertEqual(self.csv_parser.date_to_final_str(self.end_date), "2019-12-31")

    def test_date_to_intermediate_time_str(self):
        now = datetime.now()
        date_obj = now.replace(hour=5, minute=30, second=0, microsecond=0)
        self.assertEqual(self.csv_parser.date_to_intermediate_time_str(date_obj), "05:30")

        date_obj = now.replace(hour=13, minute=59, second=0, microsecond=0)
        self.assertEqual(self.csv_parser.date_to_intermediate_time_str(date_obj), "13:59")

    def test_build_intermediate_dict(self):
        intermediate_dict = self.csv_parser.build_intermediate_dict()
        self.assertEqual(len(intermediate_dict.keys()), 96)
        self.assertEqual(intermediate_dict["00:00"], 0.0)
        self.assertEqual(intermediate_dict["13:30"], 0.0)
        self.assertEqual(intermediate_dict["23:45"], 0.0)

    def test_round_up_to_quarter_hour(self):
        start_time = self.start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time), "00:00")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=1)), "00:15")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=14)), "00:15")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=15)), "00:15")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=16)), "00:30")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=29)), "00:30")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=30)), "00:30")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=45)), "00:45")
        self.assertEqual(self.csv_parser.round_up_to_quarter_hour(start_time.replace(minute=59)), "01:00")

    def test_finalize_readings(self):
        self.csv_parser.intermediate_readings = {
            "2019-01-01": {
                "00:00": 5.03,
                "00:15": 9.24,
                "00:30": 10.2
            }
        }
        finalized_readings = self.csv_parser.finalize_readings()
        self.assertEqual(finalized_readings, {
            "2019-01-01": [5.03, 9.24, 10.2]
        })


class TestDatePicker(SetupBase):

    def test_date_to_string(self):
        self.datepicker = solren.DatePickerSection
        self.assertEqual(self.datepicker.date_to_string(self.start_date), "2019-01-01")


class TestSolrenScraper(SetupBase):
    def setUp(self):
        super().setUp()
        config = Configuration()
        config.site_id = "12345"
        self.scraper = solren.SolrenScraper(credentials=None, date_range=DateRange(
            self.start_date.date(), self.end_date.date()), configuration=config
        )

    def test_string_to_date(self):
        self.assertEqual(self.scraper.string_to_date("2019-01-01"), self.start_date.date())

    def test_adjust_start_and_end_dates(self):
        self.scraper.install_date = self.end_date.date()
        self.scraper.end_date = datetime.min.date()
        # Start date moved to install date
        self.scraper.adjust_start_and_end_dates()
        self.assertEqual(self.scraper.start_date, self.end_date.date())
        # End date earlier than start date, so moved to one day past start
        self.assertEqual(self.scraper.end_date, self.scraper.start_date + timedelta(days=1))
