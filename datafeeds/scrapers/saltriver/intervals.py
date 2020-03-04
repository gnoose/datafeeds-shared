import os
import csv
import re
import time
from typing import List, Optional
from datetime import timedelta

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parse_date

from datafeeds.common.batch import run_datafeed
import datafeeds.scrapers.saltriver.pages as saltriver_pages
import datafeeds.scrapers.saltriver.errors as saltriver_errors
from datafeeds.common.exceptions import InvalidDateRangeError
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.common.support import DateRange, Results
from datafeeds.common.util.pagestate.pagestate import PageStateMachine, page_is_ready
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.timeline import Timeline
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

# Headers with interval data look like this:
# <unit>(ch : <channel number> set: <set number>)
# Where:
#   <unit> is a unit-of-measure name (e.g. KW, KVAR, KWH)
#   <channel number> is a channel identifier (generally, a number)
#   <set number> is another identifier that is not currently used
# Example:
# KW(ch: 1 set:0)
# This regex identifies an interval data header and stores the channel number in the first group.
header_regex = re.compile(r"\w+\(ch:\s*(\d+)\s*set:\s*\d+\)")


def parse_spatia_interval_csv(path, channel_id):
    with open(path, "r") as f:
        reader = csv.reader(f)
        # pylint: disable=stop-iteration-return
        headers = next(reader, None)
        if not headers:
            raise saltriver_errors.IntervalDataParseError(
                "No header row found in interval data download."
            )

        channel_columns = {}
        for idx, header in enumerate(headers):
            match = header_regex.match(header.strip())
            if match:
                channel_columns[match.group(1)] = idx

        data_index = channel_columns.get(channel_id)
        if not data_index:
            raise saltriver_errors.IntervalDataParseError(
                "No column found for channel with id='{}'".format(channel_id)
            )

        try:
            for row in reader:
                when = parse_date(row[1])

                reading = None
                reading_str = row[data_index].strip()
                if reading_str:
                    reading = float(row[data_index])

                yield (when, reading)
        except Exception as e:
            raise saltriver_errors.IntervalDataParseError from e


class SaltRiverIntervalConfiguration(Configuration):
    def __init__(self, meter_id: str, channel_id: str, scrape_readings: bool = True):
        super().__init__(scrape_bills=False, scrape_readings=scrape_readings)
        self.meter_id = meter_id
        self.channel_id = channel_id


class SaltRiverIntervalScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SRP Interval Scraper"
        self.interval_data_timeline = None

    @property
    def meter_id(self) -> str:
        return self._configuration.meter_id

    @property
    def channel_id(self) -> str:
        return self._configuration.channel_id

    def define_state_machine(self):
        """Define the flow of this scraper as a state machine"""

        # When we enter a new state, take a screenshot
        def enter_state_callback(state_name):
            self.screenshot("enter_state_{}".format(state_name))

        state_machine = PageStateMachine(self._driver)

        state_machine.on_enter_state(enter_state_callback)

        # We start in the init state, which navigates to the login page
        state_machine.add_state(
            name="init", action=self.init_action, transitions=["login"]
        )

        state_machine.add_state(
            name="login",
            page=saltriver_pages.SaltRiverLoginPage(self._driver),
            action=self.login_action,
            transitions=["login_failed", "landing_page"],
            wait_time=45,
        )

        state_machine.add_state(
            name="login_failed",
            page=saltriver_pages.SaltRiverLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        state_machine.add_state(
            name="landing_page",
            page=saltriver_pages.SaltRiverLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=["reports_page"],
        )

        state_machine.add_state(
            name="reports_page",
            page=saltriver_pages.SaltRiverReportsPage(self._driver),
            action=self.reports_page_action,
            transitions=["done"],
        )

        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
        return self.scrape_interval_data()

    def scrape_interval_data(self):
        self.interval_data_timeline = None
        state_machine = self.define_state_machine()
        final_state = state_machine.run()
        if final_state == "done":
            if self.interval_data_timeline:
                serialized = self.interval_data_timeline.serialize()
                self.log_readings(serialized)
                return Results(readings=serialized)
            return Results(readings={})
        raise Exception(
            "The scraper did not reach a finished state, this will require developer attention."
        )

    def init_action(self, _):
        self._driver.get("https://spatia.srpnet.com/login/spatialogin.asp")

    def login_action(self, page: saltriver_pages.SaltRiverLoginPage):
        page.login(self.username, self.password)

    def login_failed_action(self, page: saltriver_pages.SaltRiverLoginFailedPage):
        # Throw an exception on failure to login
        page.raise_on_error()

    def landing_page_action(self, _):
        self._driver.get("https://spatia.srpnet.com/itron/features/index.asp")

    @staticmethod
    def find_matching_meter_and_channel(
        meters: List[saltriver_pages.MeterInfo], meter_id: str, channel_id: str
    ):
        """Try to find a meter/channel matching the given identifiers, from a list of meter metadata"""
        matching_meters = [meter for meter in meters if meter.meter_id == meter_id]
        if not matching_meters:
            raise saltriver_errors.MeterNotFoundError.for_meter(meter_id, channel_id)
        if len(matching_meters) > 1:
            raise saltriver_errors.AmbiguousMeterError.for_meter(meter_id, channel_id)

        target_meter = matching_meters[0]
        if channel_id:
            matching_channels = [
                channel for channel in target_meter.channels if channel.id == channel_id
            ]
            if not matching_channels:
                raise saltriver_errors.MeterNotFoundError.for_meter(
                    meter_id, channel_id
                )
            if len(matching_channels) > 1:
                raise saltriver_errors.AmbiguousMeterError.for_meter(
                    meter_id, channel_id
                )
            target_channel = matching_channels[0]
        else:
            target_channel = next(
                (c for c in target_meter.channels if c.units.lower() == "kwh"), None
            )

        if not target_channel:
            raise saltriver_errors.MeterNotFoundError.for_meter(meter_id, channel_id)

        return target_meter, target_channel

    def clear_csv_downloads(self):
        to_remove = []
        download_dir = self._driver.download_dir
        for filename in os.listdir(download_dir):
            if filename.endswith(".csv"):
                to_remove.append(os.path.join(download_dir, filename))

        for path in to_remove:
            os.remove(path)

    def reports_page_action(self, reports_page: saltriver_pages.SaltRiverReportsPage):
        reports_page.goto_meter_profiles()

        meter_page = saltriver_pages.MeterProfilesPage(self._driver)
        WebDriverWait(self._driver, 30).until(page_is_ready(meter_page))
        meters = meter_page.get_meters()
        meter, channel = self.find_matching_meter_and_channel(
            meters, self.meter_id, self.channel_id
        )

        meter_page.goto_reports()
        WebDriverWait(self._driver, 30).until(page_is_ready(reports_page))
        time.sleep(10)
        reports_page.goto_interval_download()

        interval_download_page = saltriver_pages.IntervalDownloadPage(self._driver)
        WebDriverWait(self._driver, 30).until(page_is_ready(interval_download_page))
        interval_download_page.basic_configuration()
        interval_download_page.select_meter_by_id(meter.meter_id)

        start = self.start_date
        end = self.end_date

        # Snap the scraper start date to the data start date for the selected meter/channel.
        if start < channel.data_start:
            start = channel.data_start

        if start > end:
            raise InvalidDateRangeError(
                "The start date must be before the end date (start='{}', end='{}')".format(
                    start, end
                )
            )

        # Pull out data 30 days at a time
        date_range = DateRange(start, end)
        interval_size = relativedelta(days=30)
        timeline = Timeline(start, end)
        for sub_range in date_range.split_iter(delta=interval_size):
            self.clear_csv_downloads()
            interval_download_page.set_date_range(
                sub_range.start_date, sub_range.end_date
            )
            interval_download_page.download_interval_data()
            try:
                wait = WebDriverWait(self._driver, 180)
                csv_file_name = wait.until(
                    file_exists_in_dir(self._driver.download_dir, r".*\.csv")
                )
                csv_file_path = os.path.join(self._driver.download_dir, csv_file_name)
                for (when, reading) in parse_spatia_interval_csv(
                    csv_file_path, channel.id
                ):
                    # The CSV file reports readings at the end of each fifteen minute interval. So the first reading
                    # of the day occurs at 00:15. and the last at midnight. We want to report the readings at the
                    # _start_ of each interval, thus we subtract 15 minutes here.
                    when = when - timedelta(minutes=15)
                    timeline.insert(when, reading)
            except TimeoutException:
                raise TimeoutException("Downloading interval data from SPATIA failed.")

        self.interval_data_timeline = timeline


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    channel_id = None
    if datasource.meta:
        channel_id = datasource.meta.get("channel_id")
    conf = SaltRiverIntervalConfiguration(
        meter_id=meter.service_id, channel_id=channel_id
    )

    return run_datafeed(
        SaltRiverIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=conf,
        task_id=task_id,
    )
