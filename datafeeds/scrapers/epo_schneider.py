import csv
import codecs
import logging
import time
from collections import namedtuple

import requests
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    ElementNotVisibleException,
    NoSuchElementException,
)
from selenium.common.exceptions import TimeoutException
from dateutil.relativedelta import relativedelta
from dateutil import parser as date_parser

from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import DateRange
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Results
from datafeeds.common.support import Configuration


logger = None
log = logging.getLogger(__name__)


def _log(msg):
    log.info(msg)


class InvalidMeterException(Exception):
    pass


class InvalidMeterDataException(Exception):
    pass


class MissingChannelIdException(Exception):
    pass


CsvRow = namedtuple(
    "CsvRow", ["account", "date", "channel_id", "units", "interval_data"]
)


class EnergyProfilerConfiguration(Configuration):
    def __init__(
        self, base_url, account_id, epo_meter_id, channel_id=None, log_in=True
    ):
        super().__init__(scrape_readings=True)

        # Login URL for the EPO Schneider whitelabel site.
        self.base_url = base_url

        # Identifier associated with the meter on the utility's website.
        self.epo_meter_id = epo_meter_id

        # The account id -- not currently used
        self.account_id = account_id

        # This flag is true if the tool must use the base_url to log in.
        self.log_in = log_in

        # Channel ID or label associated with this meter.
        self.channel_id = channel_id


class MeterElement:
    """Represents a meter from the Energy Profiler web UI.

    The webpage maintains a table, where each row contains information
    about a different meter. This class represents that row. Beyond
    meter attributes, the row also contains a checkbox which can be
    used to "select" a given meter, as part of the workflow in this UI
    for dumping interval data for a meter.
    """

    def __init__(self):
        """Creates an empty MeterElement
        The factory method(s) below do more interesting initialization.
        """
        self.checkbox = None
        self.meter_id = None
        self.meter_description = None
        self.account_description = None
        self.min_date = None
        self.max_date = None

    def __str__(self):
        fmt = "Meter(id='{0}', desc='{1}', acct='{2}', start='{3}', end='{4}')"
        return fmt.format(
            self.meter_id,  # {0}
            self.meter_description,  # {1}
            self.account_description,  # {2}
            self.min_date,  # {3}
            self.max_date,  # {4}
        )

    def __repr__(self):
        return str(self)

    def select(self):
        if not self.checkbox.is_selected():
            self.checkbox.click()

    @staticmethod
    def from_table_row(row):
        """Creates a MeterElement from a Selenium WebElement.

        This WebElement represents a table row (<tr>).

        Returns:
            An initialized MeterElement instance based on the contents
            of the provided row, else None if a MeterElement could not
            be materialized.
        """
        result = None

        cells = row.find_elements_by_css_selector("td")
        if len(cells) == 6:
            result = MeterElement()
            checkbox_cell = cells[0]
            result.checkbox = checkbox_cell.find_element_by_css_selector(
                'input[type="checkbox"]'
            )

            result.meter_id = cells[1].text
            result.meter_description = cells[2].text
            result.account_description = cells[3].text
            result.min_date = date_parser.parse(cells[4].text).date()
            result.max_date = date_parser.parse(cells[5].text).date()

        if len(cells) == 7:
            result = MeterElement()
            checkbox_cell = cells[0]
            result.checkbox = checkbox_cell.find_element_by_css_selector(
                'input[type="checkbox"]'
            )

            result.meter_id = cells[1].text
            result.meter_description = cells[2].text
            result.account_description = cells[4].text
            result.min_date = date_parser.parse(cells[5].text).date()
            result.max_date = date_parser.parse(cells[6].text).date()

        return result


class LoginPage:
    """Represents the authentication page in the web UI.

    A very basic login page with usename and password fields.
    """

    UsernameFieldSelector = 'input[id="userid"]'
    PasswordFieldSelector = 'input[id="password"]'
    ContinueButtonSelector = 'input[id="contin"]'
    FailedLoginSelector = '//div[@class="message error visible"]'

    def __init__(self, driver, login_url: str):
        self._driver = driver
        self.url = login_url

    def goto_page(self):
        self._driver.get(self.url)

    def wait_until_ready(self):
        _log("Waiting for 'Login' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.UsernameFieldSelector)
            )
        )

    def get_continue_button(self):
        return self._driver.find_element_by_css_selector(self.ContinueButtonSelector)

    def login(self, username, password):
        """Authenticate with the webpage.

        Fill in the username, password, then click "continue"
        """
        _log("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.get_continue_button().click()

        try:
            self._driver.wait(5).until(
                EC.presence_of_element_located((By.XPATH, self.FailedLoginSelector))
            )
            raise LoginError("Invalid username or login.")
        except TimeoutException:
            return  # Login succeeded.


class ConfigurationPage:
    """Represents a configuration page in the web UI.

    This page can be used to configure settings for report generation.
    You can select a meter (the page calls them "accounts") and a date
    range, then generate interval data for that meter over that range.
    """

    AccountTabId = "tab_account"
    MeterTabId = "tab_meter"
    CheckAllAccountsId = "checkALL"
    DateRangeSelector = 'input[id="date-range-custom"]'
    DateRangeRadioButton = 'input[value="DateRange"]'
    MeterTableCss = "div.k-grid-content table"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        _log("Waiting for Configuration page to be ready")
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.DateRangeRadioButton))
        )
        self._driver.wait().until(
            EC.presence_of_element_located((By.ID, self.CheckAllAccountsId))
        )

    def _goto_tab(self, tab_id, class_name):
        def _tab_loaded(_):
            try:
                selector = self._driver.find_element_by_id("selaccts")
                if class_name in selector.get_attribute("class"):
                    return selector
            except NoSuchElementException:
                pass
            return False

        tab = self._driver.find_element_by_id(tab_id)
        if "active" not in tab.get_attribute("class"):
            tab.click()
            self._driver.wait().until(_tab_loaded)

    def goto_account_tab(self):
        self._goto_tab(self.AccountTabId, "accounts")

    def goto_meter_tab(self):
        self._goto_tab(self.MeterTabId, "meters")

        # Wait until the meter table loads
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.MeterTableCss))
        )

    def select_date_range_option(self):
        """Indicate that we wish to enter a custom date range."""
        _log("Selecting the custom date range option.")
        try:
            self._driver.find_element_by_css_selector(self.DateRangeRadioButton).click()
        except ElementNotVisibleException:
            # This seems to happen if you click the radio button
            # after it has been selected. So we eat this exception,
            # since the desired condition is likely satisfied.
            pass

    def set_date_range(self, interval: DateRange):
        """Set a custom date range for data generation."""

        def fmt_date(date):
            return "{0}/{1}/{2}".format(date.month, date.day, date.year)

        # Ensure that we can enter a custom date range
        self.select_date_range_option()

        date_range_string = "{0} to {1}".format(
            fmt_date(interval.start_date), fmt_date(interval.end_date)
        )

        # Enter the specified date range
        selector = self._driver.find_element_by_css_selector(self.DateRangeSelector)
        selector.clear()
        selector.send_keys(date_range_string)

    def select_all_accounts(self):
        self.goto_account_tab()
        self._driver.find_element_by_id(self.CheckAllAccountsId).click()
        time.sleep(3)  # Javascript delay

    def iter_meters(self):
        """Generate the available meters on this page."""
        self.goto_meter_tab()
        meter_table = self._driver.find_element_by_css_selector(self.MeterTableCss)
        rows = meter_table.find_elements_by_css_selector("tr")
        for row in rows:
            meter = MeterElement.from_table_row(row)
            if meter is not None:
                yield meter
            else:
                _log("Failed to parse table row into meter data.")


class ExportCsvPage:
    """Represents a page for generating a CSV report of interval data."""

    ContinueButtonSelector = "aside#newemissionreport .button"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        _log("Waiting for 'Export CSV' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.ContinueButtonSelector)
            )
        )

    def generate_report(self):
        _log("Generating a CSV report.")
        self._driver.find_element_by_css_selector(self.ContinueButtonSelector).click()


class DownloadCsvPage:
    """Represents a page for downloading a generated CSV report."""

    CsvLinkSelector = "aside#newemissionreport a"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        _log("Waiting for 'Download CSV' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.CsvLinkSelector))
        )

    def get_csv_rows(self):
        """Download CSV data from the page.

        The CSV file is streamed via requests. Each row of the CSV
        file is generated as it becomes available.
        """
        _log("Downloading CSV data.")
        link = self._driver.find_element_by_css_selector(self.CsvLinkSelector)
        href = link.get_attribute("href")

        # In the latest version of requests, I think it's possible
        # to "with" a response object in order to guarantee cleanup,
        # but that wasn't working with the version we currently use.
        # Hence, the try/finally idiom.
        response = requests.get(href, stream=True)
        try:
            # Assumption: The file is utf-8 encoded
            resp_reader = codecs.iterdecode(response.iter_lines(), "utf-8")
            csv_reader = csv.reader(resp_reader)
            for row in csv_reader:
                yield row
        finally:
            if response is not None:
                response.close()


class Navigation:
    """Helper for nagivation about the webpage.

    This entire webpage is essentially endpoint against which POST
    requests are issued. It features a sidebar with links that issue
    these requests. This class helps to locate and click those links.
    """

    ExportLinkText = "Export"
    MeterSelectionLinkText = "Date Ranges/Meters"

    def __init__(self, driver):
        self._driver = driver

    def goto_export(self):
        _log("Navigating to the export CSV page.")
        self._driver.find_element_by_link_text(self.ExportLinkText).click()

    def goto_meter_selection(self):
        _log("Navigating to the meter selection page.")
        self._driver.find_element_by_link_text(self.MeterSelectionLinkText).click()


class EnergyProfilerScraper(BaseWebScraper):
    UnitsKwh = "kwh"
    UnitsKwhPerSqFt = "kwh / sq. ft."
    # Accept either 15-minute or 30-minute readings
    EXPECTED_CSV_COLUMNS = [100, 52]
    READINGS_NUM_30_MIN_INT = 48

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "Energy Profiler Online Scraper"

    @property
    def log_in(self):
        return self._configuration.log_in

    @property
    def base_url(self):
        return self._configuration.base_url

    @property
    def epo_meter_id(self):
        return self._configuration.epo_meter_id

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def channel_id(self):
        if not self._configuration.channel_id:
            return None
        return self._configuration.channel_id.lower()

    @staticmethod
    def _kwh_to_kw(ival_data, coeff=4):
        for item in ival_data:
            if item is None:
                yield None
            else:
                yield item * coeff

    @staticmethod
    def _parse_ival_data(s):
        if s.strip() == "":
            return None
        try:
            return float(s)
        except Exception:
            raise InvalidMeterDataException("Invalid meter reading: {0}".format(s))

    def _parse_csv_row(self, row):
        if len(row) not in self.EXPECTED_CSV_COLUMNS:
            raise InvalidMeterDataException(
                "Unexpected CSV row length ({0} not in {1})".format(
                    len(row), self.EXPECTED_CSV_COLUMNS
                )
            )

        try:
            date = date_parser.parse(row[1])
        except Exception:
            raise InvalidMeterDataException("Failed to parse date ({0})".format(row[1]))

        return CsvRow(
            account=row[0],
            date=date,
            channel_id=str(row[2]).strip().lower(),
            units=row[3].lower().strip(),
            interval_data=[self._parse_ival_data(s) for s in row[4:]],
        )

    def _get_csv_kw_rows(self, csv_rows_iter):
        """Make a list of parsed usage rows"""
        accepted_units = [self.UnitsKwh]
        if self.base_url and self.base_url.startswith("https://smudpm"):
            accepted_units = [self.UnitsKwh, self.UnitsKwhPerSqFt]
        csv_rows = [self._parse_csv_row(row) for row in csv_rows_iter]
        res = [row for row in csv_rows if row.units in accepted_units]
        return res

    @staticmethod
    def _is_multichannel(csv_kw_rows):
        channels_list = [row.channel_id for row in csv_kw_rows]
        return len(set(channels_list)) > 1

    @staticmethod
    def _get_interval_data_length(csv_kw_rows):
        if not csv_kw_rows:
            # If there are no rows, it doesn't matter
            return 0
        return len(csv_kw_rows[0].interval_data)

    def _execute(self):
        # Create page helpers
        if self.log_in:
            login_page = LoginPage(self._driver, self.base_url)
            # Authenticate
            login_page.goto_page()
            login_page.wait_until_ready()
            self.screenshot("before login")
            try:
                login_page.login(self.username, self.password)
            except LoginError as exc:
                self.screenshot("login failed")
                raise exc

        config_page = ConfigurationPage(self._driver)
        navigation = Navigation(self._driver)
        export_csv_page = ExportCsvPage(self._driver)
        download_csv_page = DownloadCsvPage(self._driver)

        # Configure interval data generation, in two steps...
        config_page.wait_until_ready()
        config_page.select_all_accounts()
        self.screenshot("accounts selected")

        matching_meter = None
        meter_query = self.epo_meter_id
        _log("Looking for a meter with ID == {0}".format(meter_query))
        for meter in config_page.iter_meters():
            _log("Found a meter: {0}".format(meter))
            if meter.meter_id == meter_query:
                _log("Found a matching meter.")
                matching_meter = meter
                break

        if matching_meter is None:
            _log("No meter with ID {0} was found.".format(meter_query))
            raise InvalidMeterException("Meter {0} was not found".format(meter_query))
        else:
            matching_meter.select()
            self.screenshot("meter selected")

        # Two notes on time...
        # 1) Each meter specifies the date range for which data is
        #    available. If we don't respect this, the page will throw
        #    errors. We restrict our start and end dates based on
        #    this information.
        if self.start_date < matching_meter.min_date:
            _log(
                "Adjusting start date from {0} to {1}".format(
                    self.start_date, matching_meter.min_date
                )
            )
            self.start_date = matching_meter.min_date
        if self.end_date > matching_meter.max_date:
            _log(
                "Adjusting end date from {0} to {1}".format(
                    self.end_date, matching_meter.max_date
                )
            )
            self.end_date = matching_meter.max_date

        # 2) Only a limited amount of data can be extracted at a time.
        #    The page enforces this by restricting the number of days
        #    for which you can download data. Therefore, we pull down
        #    data in 180-day chunks. The actual restriction is a little
        #    hard to pin down, since it varies based on some nontransparent
        #    factors. 180 though is a very conservative estimate.
        date_range = DateRange(self.start_date, self.end_date)
        interval_size = relativedelta(days=180)
        readings = {}  # Maps dates to interval data, populated below
        for interval in date_range.split_iter(delta=interval_size):
            _log("Gathering meter data for: {0}".format(interval))

            # First, set the date range for the selected meter
            config_page.set_date_range(interval)
            self.screenshot(
                "date range set {} to {}".format(
                    interval.start_date.isoformat(), interval.end_date.isoformat()
                )
            )

            # Navigate to the "Export" page, and request a CSV report
            navigation.goto_export()
            export_csv_page.wait_until_ready()
            export_csv_page.generate_report()

            # Wait for the report to generate, then download it
            # and extract interval data from it
            download_csv_page.wait_until_ready()
            csv_rows_iter = download_csv_page.get_csv_rows()
            header = next(csv_rows_iter)
            _log("CSV Header row: {0}".format(header))

            csv_kw_rows = self._get_csv_kw_rows(csv_rows_iter)
            if self._is_multichannel(csv_kw_rows) and not self.channel_id:
                raise MissingChannelIdException(
                    "Missing channel ID for multichannel meter"
                )

            # The intervals coefficient is a multiplier for the interval data.
            intervals_coeff = 4  # For 15-minute intervals
            if (
                self._get_interval_data_length(csv_kw_rows)
                == self.READINGS_NUM_30_MIN_INT
            ):
                intervals_coeff = 2  # For 30-minute intervals

            for data_row in csv_kw_rows:
                # Add usage rows with the requested channel.
                # If no channel_id was passed in, add all usage rows.
                if not self.channel_id or self.channel_id == data_row.channel_id:
                    readings[data_row.date.strftime("%Y-%m-%d")] = list(
                        self._kwh_to_kw(data_row.interval_data, intervals_coeff)
                    )

            # Navigate back to the meter selection page in preparation
            # for the next iteration. Note that we do not reselect the
            # meter, since our initial selections are cached.
            navigation.goto_meter_selection()
            config_page.wait_until_ready()

        return Results(readings=readings)
