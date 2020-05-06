import os
import logging
import csv

from dateutil.parser import parse as parse_date
from typing import NewType, Tuple, List, Optional
from datetime import datetime, date, timedelta

from datafeeds import config
from datafeeds.common import Timeline
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import (
    LoginError,
    InvalidDateRangeError,
    DataSourceConfigurationError,
)
from datafeeds.common.support import Results
from datafeeds.common.support import Configuration
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.typing import Status

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

log = logging.getLogger(__name__)


IntervalReading = NewType("IntervalReading", Tuple[datetime, Optional[float]])


def parse_usage_from_csv(csv_file_path) -> List[IntervalReading]:
    """Read and parse data from csv file.

    Returns: list of tuples of datetime and kWh usage
    """

    results = []
    with open(csv_file_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # for some reason the last line of csv contains the following
            # string instead of actual values, so skip that data row
            if "</td></tr></table>" in row["Starting Date"]:
                continue

            reading_date = parse_date(row["Starting Date"].strip()).date()
            starting_time_text = row["Starting Time"].strip()

            # this is weird, some of the "Starting Time" fields in data are empty
            # if that is the case, determine the starting time by subtracting
            # 00:14:59 from "Ending Time"
            if not starting_time_text:
                ending_time_text = row["Ending Time"].strip()

                if not ending_time_text:
                    # skip the reading if no starting time or ending time is present
                    log.debug(f"No starting time or ending time found for row: {row}")
                    continue

                ending_time = parse_date(ending_time_text).time()
                reading_datetime = datetime.combine(
                    reading_date, ending_time
                ) - timedelta(minutes=14, seconds=59)

            else:
                starting_time = parse_date(row["Starting Time"].strip()).time()
                reading_datetime = datetime.combine(reading_date, starting_time)

            reading_value = float(row["kWh Usage"].strip())
            results.append(IntervalReading((reading_datetime, reading_value)))

    return results


def _get_main_window(driver):
    # The window handle is often not immediately set, this goes
    # into a loop until the handle is set. We need it to
    # differentiate from the popup later.
    rval = None
    elapsed = 0
    start_time = datetime.utcnow()

    while not rval and elapsed < 60:
        rval = driver.current_window_handle
        elapsed = (datetime.utcnow() - start_time).seconds

    if not rval:
        raise Exception("Unable to get main window, dying...")

    return rval


class LoginPage(CSSSelectorBasePageObject):
    UsernameFieldSelector = 'input[id="P101_USERNAME"]'
    PasswordFieldSelector = 'input[id="P101_PASSWORD"]'
    SigninButtonSelector = 'img[alt="Login to Seattle MeterWatch"]'

    def login(self, username: str, password: str):
        """Login and wait for dropdown to load."""
        log.debug("Logging in to meterwatch...")
        self.wait_until_ready(self.UsernameFieldSelector)
        self.wait_until_ready(self.SigninButtonSelector)

        log.info("Filling in form")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)

        self.find_element(self.SigninButtonSelector).click()
        try:
            self.wait_until_ready('select[name="p_t04"]')

        except Exception:
            if self._driver.find(
                '//li[text()="Security Information Invalid."', xpath=True
            ):
                raise LoginError("Unable to login, invalid credentials?")
            raise LoginError("Unable to login")


class MeterDataPage(CSSSelectorBasePageObject):
    """Display Meter Data page with account dropdown."""

    DateAvailableFromSel = 'label[for="P10_FROM_DATE"] span'
    DateAvailableFromInputSel = 'input[id="P10_FROM_DATE"]'

    DateAvailableToSel = 'label[for="P10_THROUGH_DATE"] span'
    DateAvailableToInputSel = 'input[id="P10_THROUGH_DATE"]'

    DownloadBtnSel = "a[href=\"javascript:apex.submit('DOWNLOAD');\"].buttonhtml"

    def enter_dates(self, start_date: date, end_date: date):
        """Set dates in From Date, To Date (04/01/2020 format)

        Check available dates (Starts on 04/30/2018, Ends on 03/18/2020)
        Continue with the maximum available range if requested range not available.
        """
        # wait for date inputs to be ready
        self.wait_until_ready(self.DateAvailableFromSel)
        self.wait_until_ready(self.DateAvailableToSel)

        available_from_text = self.find_element(self.DateAvailableFromSel).text.split(
            " "
        )[-1]
        available_from = datetime.strptime(available_from_text, "%m/%d/%Y")

        available_to_text = self.find_element(self.DateAvailableToSel).text.split(" ")[
            -1
        ]
        available_to = datetime.strptime(available_to_text, "%m/%d/%Y")
        log.info("data available through %s" % available_to)

        if start_date < available_from.date():
            start_date = available_from.date()

        if end_date > available_to.date():
            end_date = available_to.date()

        # the webpage shows an error if start_date is greater than end_date
        # make sure its a valid range
        if end_date < start_date:
            raise InvalidDateRangeError(
                "end_date %s cannot be less than start_date %s" % (end_date, start_date)
            )

        log.info("Setting start date to: {}".format(start_date.strftime("%m/%d/%Y")))
        log.info("Setting end date to: {}".format(end_date.strftime("%m/%d/%Y")))

        # clear date fields first, in case there's residual text from previous account here
        self.find_element(self.DateAvailableFromInputSel).clear()
        self.find_element(self.DateAvailableToInputSel).clear()

        self._driver.fill(
            self.DateAvailableFromInputSel, start_date.strftime("%m/%d/%Y")
        )
        self._driver.fill(self.DateAvailableToInputSel, end_date.strftime("%m/%d/%Y"))

    def select_account(self, meter_number: str):
        """Choose account from Meter OR Meter Group dropdown (option value == meter_number)"""
        log.info(f"Selecting account {meter_number}...")
        select_xpath = '//select[@name="p_t04"]'

        # wait for account dropdown to be ready
        self.wait_until_ready('select[name="p_t04"]')

        try:
            option_elem = self._driver.wait(10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        f"{select_xpath}/option[starts-with(text(), {meter_number})]",
                    )
                )
            )
            log.info(f"Found account option: {option_elem.text}")
        except TimeoutException:
            raise DataSourceConfigurationError(
                f"{meter_number} not found in Meter number dropdown"
            )

        select = self._driver.get_select(select_xpath, xpath=True)
        meter_id = option_elem.get_attribute("value")

        select.select_by_value(meter_id)

        # the page refreshes after selecting meter number, so wait until its loaded
        self.wait_until_ready(f'option[value="{meter_id}"]')

    def download_data(self, meter_number: str) -> str:
        """Download data to the working directory.

        Click Download Data button.
        Saves to config.WORKING_DIRECTORY/15_minute_download.csv
        Rename the downloaded file to config.WORKING_DIRECTORY/{meter_number}.csv
        Return: the path of the downloaded csv file.
        """
        # wait for the download button to be ready
        self.wait_until_ready(self.DownloadBtnSel)

        log.info("Beginning download...")
        self.find_element(self.DownloadBtnSel).click()

        # download filename is always 15_minute_download.csv for 15 minute intervals
        filename = "15_minute_download.csv"
        download_dir = "%s/current" % config.WORKING_DIRECTORY

        try:
            self._driver.wait(30).until(
                file_exists_in_dir(
                    # end pattern with $ to prevent matching
                    # filename.crdownload
                    directory=download_dir,
                    pattern=f"^{filename}$",
                )
            )
        except Exception:
            raise Exception(f"Unable to download file...")

        log.info("Download Complete")

        csv_file_path = os.path.join(download_dir, meter_number + ".csv")

        # rename downloaded filename to {meter_number}.csv for
        # avoiding filename conflict in case of multiple accounts
        os.rename(os.path.join(download_dir, filename), csv_file_path)

        return csv_file_path


class SCLMeterWatchConfiguration(Configuration):
    def __init__(self, meter_numbers: List[str]):
        super().__init__(scrape_readings=True)
        self.meter_numbers = meter_numbers


class SCLMeterWatchScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SCL MeterWatch"
        self.url = "http://smw.seattle.gov"

    def _execute(self):
        # Meterwatch immediately spawns a popup when loaded which is the actual
        # window we want. So we have to go and grab the main window handle and
        # THEN go looking for the popup window and switch to it.

        timeline = Timeline(self.start_date, self.end_date)

        main_window = _get_main_window(self._driver)
        login_window = None

        log.info(f"Navigating to {self.url}")
        self._driver.get(self.url)

        while not login_window:
            for handle in self._driver.window_handles:
                if handle != main_window:
                    login_window = handle
                    break

        # We have our popup, so lets do stuff with it.
        self._driver.switch_to.window(login_window)

        log.debug("Driver title: " + self._driver.title)
        assert "Seattle MeterWatch" in self._driver.title

        login_page = LoginPage(self._driver)
        meterdata_page = MeterDataPage(self._driver)

        login_page.login(self.username, self.password)

        for meter_number in self._configuration.meter_numbers:
            meterdata_page.select_account(meter_number)
            meterdata_page.enter_dates(self.start_date, self.end_date)
            csv_file_path = meterdata_page.download_data(meter_number)

            log.info(f"parsing kWh usage from downloaded data for {meter_number}")
            # read kWh Usage with csv into timeline
            for reading in parse_usage_from_csv(csv_file_path):
                reading_datetime = reading[0]
                reading_value = reading[1]
                # multiply by 4 because SCL returns 1/4 of an hour's worth
                reading_value *= 4

                # subtract a second from reading_datetime because the "Starting
                # Time" in data always start at 1 second (e.g: 00:15:01 instead
                # of 00:15:00), and Timeline initializes the index with zero
                # seconds (e.g 00:15:00)
                reading_datetime = reading_datetime - timedelta(seconds=1)
                current_value = timeline.lookup(reading_datetime)

                # if value already exists for datetime, add to it
                if current_value:
                    reading_value = current_value + reading_value

                timeline.insert(reading_datetime, reading_value)

        return Results(readings=timeline.serialize(include_empty=False))


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    meter_numbers = meter.service_id.split(",")
    # if totalized is set in meta, get list of meter numbers
    totalized = (datasource.meta or {}).get("totalized")
    if totalized:
        meter_numbers = totalized.split(",")
    configuration = SCLMeterWatchConfiguration(meter_numbers=meter_numbers)

    return run_datafeed(
        SCLMeterWatchScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
