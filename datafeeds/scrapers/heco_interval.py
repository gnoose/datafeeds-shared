import csv
import time
import logging

from typing import Optional, Tuple, List, Dict, Callable, Union
from datetime import timedelta, datetime, date, time as time_t
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from retrying import retry

from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import DateRange
from datafeeds.common.support import Results
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import IFrameSwitch, clear_downloads
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

logger = None
log = logging.getLogger(__name__)


EXPECTED_CSV_LEN = 96
MAX_INTERVAL_LENGTH = 60
TIME = "time"
DEMAND = "demand"
DATE_FORMAT = "%m/%d/%Y"

IntermediateReading = Dict[str, Dict[str, List[Union[float, str]]]]


class MeterNotFoundException(Exception):
    pass


def iframe_decorator(func: Callable):
    """
    Decorator for wrapping IFrame functions so selenium can find
    the necessary elements.
    """

    # Adding these rules here because of weirdness around including
    # this decorator (that depends on the IFrameBasePageObject) inside
    # the class, while still making it available to subclasses
    def func_wrapper(self, *args, **kwargs):
        iframe_selector = self.get_iframe_selector()
        if iframe_selector:
            # Switches to iframe only if exists
            with IFrameSwitch(self._driver, self.get_iframe_selector()):
                return func(self, *args, **kwargs)
        else:
            return func(self, *args, **kwargs)

    return func_wrapper


class IFrameBasePageObject(CSSSelectorBasePageObject):
    IFrameSelector = "div.powertrax > iframe"

    def get_iframe_selector(self):
        try:
            return self.find_element(self.IFrameSelector)
        except NoSuchElementException:
            return None

    @iframe_decorator
    def wait_until_ready(
        self,
        selector: str,
        error_selector: Optional[str] = None,
        error_cls=None,
        error_msg: Optional[str] = None,
    ):
        return super().wait_until_ready(
            selector=selector,
            error_selector=error_selector,
            error_cls=error_cls,
            error_msg=error_msg,
        )

    @iframe_decorator
    def wait_until_text_visible(
        self,
        selector: str,
        text: str,
        error_selector: Optional[str] = None,
        alt_text: Optional[str] = None,
        error_cls=None,
        error_msg: Optional[str] = None,
    ):
        return super().wait_until_text_visible(
            selector=selector,
            text=text,
            error_selector=error_selector,
            alt_text=alt_text,
            error_cls=error_cls,
            error_msg=error_msg,
        )


class HECOGridConfiguration(Configuration):
    def __init__(self, meter_id: str):
        super().__init__(scrape_readings=True)

        # This is the meter_id (not the meter number) associated
        # with the meter on Powertrax.
        self.meter_id = meter_id


class LoginPage(CSSSelectorBasePageObject):
    """Represents the authenticate page in the web UI.

    A very basic login page with username and password fields.
    """

    UsernameFieldSelector = 'input[id="cred_userid_inputtext"]'
    PasswordFieldSelector = 'input[id="cred_password_inputtext"]'
    SigninButtonSelector = 'button[id="cred_sign_in_button"]'

    def get_signin_button(self):
        return self.find_element(self.SigninButtonSelector)

    def login(self, username: str, password: str):
        """Authenticate with the web page.

        Fill in the username, password, then click "Sign in"
        """
        log.info("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.get_signin_button().click()


class AccountOverviewPage(CSSSelectorBasePageObject):
    LoginErrorSelector = "div.error_msg"
    PowerTraxLinkSelector = 'a[href$="/MyBusinessPortal/PowerTrax"]'

    def get_powertrax_link(self):
        return self.find_element(self.PowerTraxLinkSelector)

    def navigate_to_powertrax(self):
        """Navigate to Powertrax, for interval data.

        Click on the powertrax link.
        """
        log.info("Clicking on the PowerTrax link")
        self.get_powertrax_link().click()


class DownloadPage(IFrameBasePageObject):
    @retry(stop_max_attempt_number=3, wait_fixed=10000)
    def get_download_page_link(self, download_selector):
        return self.find_element(download_selector)

    @iframe_decorator
    def navigate_to_download_page(self, download_selector):
        """Navigate to download page

        Click on the download link.
        """
        log.info("Clicking on the Download link")
        self.get_download_page_link(download_selector).click()


class MeterPage(IFrameBasePageObject):
    MeterSearchInput = "#meterTable_filter > label > input"

    def get_meter_searchbox(self):
        return self.find_element(self.MeterSearchInput)

    @iframe_decorator
    def search_by_meter_id(self, meter_id: str):
        """Search by meter id - there are many pages of meters, so rather
        than paginate through them, use the search bar.
        """
        log.info("Searching by meter id.")
        self.get_meter_searchbox().send_keys(meter_id)


class MeterSearchResult(IFrameBasePageObject):
    SearchResult = "table#meterTable tbody tr:first-child td:first-child.breakAll"
    NoResultsFound = (
        "table#meterTable tbody tr:first-child td:first-child.dataTables_empty"
    )

    def get_search_result(self):
        return self.find_element(self.SearchResult)

    @iframe_decorator
    def click_on_meter_result(self):
        log.info("Clicking on meter id.")
        self.get_search_result().click()


class AvailableDateComponent(IFrameBasePageObject):
    AvailableDates = "label#dataAvailable"

    def _get_available_dates_element(self):
        return self.find_element(self.AvailableDates)

    def _extract_available_dates(self) -> Tuple[date, date]:
        """
        Pulls available start/end dates off the page - a request is made to populate this date
        when we click the "Download" button. Because we have to add several delays
        while searching for the meter prior to this step, assume the dates are populated by
        this point.

        Assumes date string in format "Data available from mm/dd/yyyy to mm/dd/yyyy"
        """
        log.info("Extracting available dates.")
        available_date_arr = self._get_available_dates_element().text.split(" to ")
        start_date = datetime.strptime(
            available_date_arr[0], "Data available from {}".format(DATE_FORMAT)
        ).date()
        end_date = datetime.strptime(available_date_arr[1], DATE_FORMAT).date()
        return start_date, end_date

    @iframe_decorator
    def adjust_start_and_end_dates(self, start: date, end: date) -> Tuple[date, date]:
        min_start, max_end = self._extract_available_dates()

        if start < min_start:
            start = min_start
            log.info("Start date adjusted to min start date {}".format(min_start))

        if end > max_end:
            log.info("End date adjusted to max end date {}".format(max_end))
            end = max_end

        return start, end


class IntervalForm(IFrameBasePageObject):
    StartDate = "input#reportStart"
    EndDate = "input#reportEnd"
    DemandButton = "label:nth-child(2) > span"
    SaveButton = "input[value=Save]"

    @staticmethod
    def _format_date(date_to_format: datetime) -> str:
        return date_to_format.strftime(DATE_FORMAT)

    @staticmethod
    def _backup_start_date(start: datetime, days: Optional[int] = 1) -> datetime:
        """
        The first demand measurement at midnight is missing on the first date returned,
        so we back up to the previous day.
        """
        return start - timedelta(days=days)

    def _set_date(self, date_selector: str, date_input: datetime):
        """
        Adds the date to the date picker.  The date picker requires significant
        pauses to populate correctly.

        :param date_selector: css selector for date picker input
        :param date_input: date to write to input box
        """
        time.sleep(3)
        calendar_input = self.find_element(date_selector)
        time.sleep(3)
        # Backspace was seeming more reliable than "clear"
        calendar_input.send_keys(Keys.BACKSPACE * 10)
        time.sleep(3)
        calendar_input.send_keys(IntervalForm._format_date(date_input))

    @iframe_decorator
    def fill_out_interval_form_and_download(
        self, start: datetime, end: datetime, timeout: Optional[int] = 60
    ):
        log.info("Filling out interval form")
        # Fill out Report Start
        self._set_date(self.StartDate, IntervalForm._backup_start_date(start))
        # Fill out Report End
        self._set_date(self.EndDate, end)
        # Select Demand Option
        self.find_element(self.DemandButton).click()
        # Click Save
        self.find_element(self.SaveButton).click()


class HECOScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        """
        HECO MVWeb Selenium Scraper
        """
        super().__init__(*args, **kwargs)
        self.name = "HECO"
        self.login_url = "https://mybiz.heco.com/"
        # Download link selector can differ on MVWeb implementation
        self.download_link_selector = 'a[href$="/mvweb/download"]'

    # Overrides BaseWebScraper.start
    def start(self):
        super().start()
        self._driver.set_window_size(1280, 960)

    @property
    def meter_id(self):
        return self._configuration.meter_id

    @staticmethod
    def _get_header_position(header_row: List[str], column_title: str) -> int:
        """
        CSV data has a lot of whitespace in the header rows, and the header titles
        themselves are subject to change.  Searches every column title for a partial
        case-insensitive match and returns the matching index.
        """
        for pos, column in enumerate(header_row):
            if column_title.lower() in column.lower():
                return pos

        raise Exception("Expected column header not found for {}".format(column_title))

    @staticmethod
    def _remove_incomplete_demand_data(
        response: IntermediateReading, date_to_check: str
    ):
        """
        As we're iterating through the CSV, once all the demand data for a given
        day (date_to_check) is populated, we verify that the amount of data is
        what we expected.

        If demand data length is incorrect, or demand start/stop times don't match
        up, they are removed from the response.
        """
        to_delete = False
        if len(response[date_to_check][DEMAND]) != EXPECTED_CSV_LEN:
            log.info(
                "Skipping partial day {}, unexpected CSV row length ({} != {}).".format(
                    date_to_check, len(response[date_to_check]), EXPECTED_CSV_LEN
                )
            )
            to_delete = True

        # First demand value expected at midnight
        if (
            response[date_to_check][TIME]
            and response[date_to_check][TIME][0] != "00:00"
        ):
            to_delete = True

        # Final demand value for the day expected fifteen min before midnight
        if (
            response[date_to_check][TIME]
            and response[date_to_check][TIME][-1] != "23:45"
        ):
            to_delete = True

        if to_delete:
            # Not including partial days in the response
            del response[date_to_check]
        return

    @staticmethod
    def _format_time(time_to_format: time_t) -> str:
        return time_to_format.strftime("%H:%M")

    @staticmethod
    def _finalize_readings(
        readings: IntermediateReading,
    ) -> Dict[str, List[Union[float, str]]]:
        """
        Modifies the intermediate readings format to take the final desired format,
        dates mapped to arrays of demand values
        readings = {
            'YYYY-MM-DD': [100.3, 432.0, ...],
            'YYYY-MM-DD': ['104.3', '99.7', ...],
        }
        """
        finalized = {}
        for each_date in readings:
            finalized[each_date] = readings[each_date][DEMAND]
        return finalized

    @staticmethod
    def _convert_demand_type(demand: str) -> Union[float, str]:
        try:
            return float(demand)
        except ValueError:
            return demand

    def _process_csv(self, file_path: str, response: IntermediateReading):
        """
        Processes the demand download csv from Powertrax - lots of whitespace in
        the column headers and data.

        Modifies the intermediate response dictionary, adding additional days
        keyed to an dictionary with 'time' and 'demand' keys. There should be
        96 demand values, at 15 minute increments, starting at midnight
        example: response = {
            'YYYY-MM-DD': {
                'demand': [100.3, 432.0, ...],
                'time': ['00:00', '00:15', ...]
            }
        }

        :param file_path: path for the downloaded csv file
        :param response: Demand dictionary in intermediate format
        """
        with open(file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            header_row = next(csv_reader)

            date_time_col = HECOScraper._get_header_position(header_row, "Date / Time")
            # Assumes there's only one kW column - this is channel 1
            demand_col = HECOScraper._get_header_position(header_row, "KW")

            current_date = ""
            for row in csv_reader:
                raw_datetime = dateparser.parse(row[date_time_col].strip())
                raw_date = raw_datetime.date().strftime("%Y-%m-%d")
                raw_time = HECOScraper._format_time(raw_datetime.time())
                raw_demand = row[demand_col].strip()

                if current_date and current_date != raw_date:
                    # An entire day's worth of demand data has been populated
                    HECOScraper._remove_incomplete_demand_data(response, current_date)

                if raw_date not in response:
                    response[raw_date] = {TIME: [], DEMAND: []}

                # To cover gaps in csv data returned, some days may be pulled multiple times.
                if len(response[raw_date][DEMAND]) < EXPECTED_CSV_LEN:
                    response[raw_date][TIME].append(raw_time)
                    response[raw_date][DEMAND].append(
                        self._convert_demand_type(raw_demand)
                    )

                current_date = raw_date

        # Verifies the last day's demand data is present
        HECOScraper._remove_incomplete_demand_data(response, current_date)
        return response

    def login_to_mvweb(self):
        """
        Log in through HECO interface and then navigate to MVWeb Portal
        """
        login_page = LoginPage(self._driver)
        # Log in
        login_page.wait_until_ready(login_page.UsernameFieldSelector)
        self.screenshot("before login")
        login_page.login(self.username, self.password)

        # Navigate to PowerTrax - if successful login, should see PowerTrax login.
        # Otherwise, expecting error message e.g. bad user id or password
        overview_page = AccountOverviewPage(self._driver)
        overview_page.wait_until_ready(
            overview_page.PowerTraxLinkSelector,
            error_selector=overview_page.LoginErrorSelector,
            error_cls=LoginError,
            error_msg="User ID and/or password not found.",
        )
        self.screenshot("before navigating to powertrax")
        overview_page.navigate_to_powertrax()

    def _execute(self):
        # Direct the driver to the login page
        self._driver.get(self.login_url)
        # Create page helpers
        download_page = DownloadPage(self._driver)
        meter_page = MeterPage(self._driver)
        search_result = MeterSearchResult(self._driver)
        available_dates = AvailableDateComponent(self._driver)
        interval_form = IntervalForm(self._driver)

        self.login_to_mvweb()

        # Navigate to Download Page
        # Pause to let the IFrame to settle down
        time.sleep(5)

        download_page.wait_until_ready(selector=self.download_link_selector)
        self.screenshot("before clicking on download link")
        download_page.navigate_to_download_page(self.download_link_selector)
        time.sleep(10)

        # Enter MeterId in the search box
        meter_page.wait_until_ready(meter_page.MeterSearchInput)
        self.screenshot("before searching for meter")
        meter_page.search_by_meter_id(self.meter_id)
        time.sleep(10)

        # When search results have settled down, click on first meter result.
        # If meter isn't found, throw an error.
        search_result.wait_until_text_visible(
            search_result.SearchResult,
            self.meter_id,
            error_selector=search_result.NoResultsFound,
            alt_text="No matching records found",
            error_cls=MeterNotFoundException,
            error_msg="No matching records found for Meter ID {}".format(self.meter_id),
        )
        self.screenshot("before clicking on meter result")
        search_result.click_on_meter_result()

        # Adjust start and end dates if supplied start and end are out of range
        adjusted_start, adjusted_end = available_dates.adjust_start_and_end_dates(
            self.start_date, self.end_date
        )

        date_range = DateRange(adjusted_start, adjusted_end)
        interval_size = relativedelta(days=MAX_INTERVAL_LENGTH)

        readings = {}
        # Breaks the date range into small, manageable chunks and downloads a csv
        # of demands for each one.
        for sub_range in date_range.split_iter(delta=interval_size):
            log.info("Getting interval data for date range: {}".format(sub_range))
            start = sub_range.start_date
            end = sub_range.end_date

            # Fill out interval form and click save to download data
            interval_form.fill_out_interval_form_and_download(start, end)
            file_path = self.download_file("csv")

            # Extract intermediate info from csv
            self._process_csv(file_path, readings)

            # Clean up any downloaded files
            log.info("Cleaning up downloads.")
            clear_downloads(self._driver.download_dir)

        # Transform readings dict into final desired format
        transformed_readings = HECOScraper._finalize_readings(readings)

        return Results(readings=transformed_readings)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = HECOGridConfiguration(meter_id=meter.service_id)

    return run_datafeed(
        HECOScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
