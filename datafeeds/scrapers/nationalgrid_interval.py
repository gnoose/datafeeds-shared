import codecs
from collections import namedtuple
from typing import Optional
import csv
import logging

from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
import requests
from selenium.common.exceptions import ElementNotVisibleException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from datafeeds.common import Configuration, DateRange, Results
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import InvalidMeterDataException
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


log = logging.getLogger(__name__)
UNITS_KWH = "kwh"
EXPECTED_CSV_LEN = 100
CsvRow = namedtuple("CsvRow", ["account", "date", "units", "interval_data"])


class InvalidMeterException(Exception):
    pass


class NationalGridConfiguration(Configuration):
    def __init__(self, meter_id: str, account_id: str):
        super().__init__(scrape_readings=True)
        # This is the identifier associated with the meter on
        # the utility's website.
        self.ngrid_meter_id = meter_id
        # The account id -- not currently used
        self.account_id = account_id


class MeterElement(object):
    """Represents a meter from the national grid web UI.

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
        self.account_id = None
        self.description = None
        self.min_date = None
        self.max_date = None
        self.meter_id = None

    def __str__(self):
        fmt = "Meter(acct='{0}', desc='{1}', min_date='{2}', max_date='{3}')"
        return fmt.format(
            self.account_id, self.description, self.min_date, self.max_date
        )

    def __repr__(self):
        return str(self)

    def select(self):
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
        if len(cells) > 0:
            result = MeterElement()
            checkbox_cell = cells[0]
            result.checkbox = checkbox_cell.find_element_by_css_selector(
                'input[type="checkbox"]'
            )

            data_cells = cells[1:]
            result.account_id = data_cells[0].text
            result.description = data_cells[1].text
            result.meter_id = result.description.split()[-1]
            result.min_date = dateparser.parse(data_cells[2].text).date()
            result.max_date = dateparser.parse(data_cells[3].text).date()
        return result


class LoginPage(object):
    """Represents the authenticate page in the web UI.

    A very basic login page with username and password fields.
    """

    UsernameFieldSelector = 'input[id="userid"]'
    PasswordFieldSelector = 'input[id="password"]'
    ContinueButtonSelector = 'input[id="contin"]'

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        log.info("Waiting for 'Login' page to be ready...")
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
        log.info("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.get_continue_button().click()


class MeterSelectionPage(object):
    """Represents a configuration page in the web UI.

    This page can be used to configure settings for report generation.
    You can select a meter (the page calls them "accounts") and a date
    range, then generate interval data for that meter over that range.
    """

    AccountsTableSelector = 'aside[id="selaccts"] div .k-grid-content table'
    DateRangeSelector = 'input[id="date-range-custom"]'
    DateRangeRadioButton = 'input[value="DateRange"]'

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        log.info("Waiting for 'Meter Selection' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.AccountsTableSelector)
            )
        )

    def fmt_date(self, date):
        return "{0}/{1}/{2}".format(date.month, date.day, date.year)

    def select_date_range_option(self):
        """Indicate that we wish to enter a custom date range."""
        log.info("Selecting the custom date range option.")
        try:
            self._driver.find_element_by_css_selector(self.DateRangeRadioButton).click()
        except ElementNotVisibleException:
            # This seems to happen if you click the radio button
            # after it has been selected. So we eat this exception,
            # since the desired condition is likely satisfied.
            pass

    def set_date_range(self, range):
        """Set a custom date range for data generation.

        Args:
            range: A DateRange object
        """

        date_range_string = "{0} to {1}".format(
            self.fmt_date(range.start_date), self.fmt_date(range.end_date)
        )
        log.info("Specifying a date range of: {0}".format(date_range_string))

        # Enter the specified date range
        selector = self._driver.find_element_by_css_selector(self.DateRangeSelector)
        selector.clear()
        selector.send_keys(date_range_string)

    def get_accounts_table(self):
        return self._driver.find_element_by_css_selector(self.AccountsTableSelector)

    def iter_meters(self):
        """Generate the available meters on this page."""
        log.info("Scraping meter elements from page.")
        acct_table = self.get_accounts_table()
        rows = acct_table.find_elements_by_css_selector("tr")

        for row in rows:
            meter = MeterElement.from_table_row(row)
            if meter is not None:
                yield meter
            else:
                log.info("Failed to parse table row into meter data.")


class ExportCsvPage(object):
    """Represents a page for generating a CSV report of interval data."""

    ContinueButtonSelector = "aside#newemissionreport .button"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        log.info("Waiting for 'Export CSV' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.ContinueButtonSelector)
            )
        )

    def generate_report(self):
        log.info("Generating a CSV report.")
        self._driver.find_element_by_css_selector(self.ContinueButtonSelector).click()


class DownloadCsvPage(object):
    """Represents a page for downloading a generated CSV report."""

    CsvLinkSelector = "aside#newemissionreport a"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        log.info("Waiting for 'Download CSV' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.CsvLinkSelector))
        )

    def get_csv_rows(self):
        """Download CSV data from the page.

        The CSV file is streamed via requests. Each row of the CSV
        file is generated as it becomes available.
        """
        log.info("Downloading CSV data.")
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


class Navigation(object):
    """Helper for navigation about the webpage.

    This entire webpage is essentially endpoint against which POST
    requests are issued. It features a sidebar with links that issue
    these requests. This class helps to locate and click those links.
    """

    ExportLinkText = "Export"
    MeterSelectionLinkText = "Date Ranges/Accounts"

    def __init__(self, driver):
        self._driver = driver

    def goto_export(self):
        log.info("Navigating to the export CSV page.")
        self._driver.find_element_by_link_text(self.ExportLinkText).click()

    def goto_meter_selection(self):
        log.info("Navigating to the meter selection page.")
        self._driver.find_element_by_link_text(self.MeterSelectionLinkText).click()


class NationalGridIntervalScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "National Grid"
        self.login_url = (
            "https://core.epo.schneider-electric.com/ngrid/cgi/eponline.exe"
        )

    @property
    def ngrid_meter_id(self):
        return self._configuration.ngrid_meter_id

    @property
    def account_id(self):
        return self._configuration.account_id

    @staticmethod
    def _iso_str(dt):
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def kwh_to_kw(ival_data, coeff=4):
        for item in ival_data:
            if item is None:
                yield None
            else:
                yield item * coeff

    @staticmethod
    def parse_ival_data(s):
        if s.strip() == "":
            return None
        try:
            return float(s)
        except ValueError:
            raise InvalidMeterDataException("Invalid meter reading: {0}".format(s))

    @staticmethod
    def parse_csv_row(row):
        if len(row) != EXPECTED_CSV_LEN:
            raise InvalidMeterDataException(
                "Unexpected CSV row length ({0} != {1})".format(
                    len(row), EXPECTED_CSV_LEN
                )
            )

        try:
            date = dateparser.parse(row[1])
        except Exception:
            raise InvalidMeterDataException("Failed to parse date ({0})".format(row[1]))

        return CsvRow(
            account=row[0],
            date=date,
            units=row[3].lower().strip(),
            interval_data=[
                NationalGridIntervalScraper.parse_ival_data(s) for s in row[4:]
            ],
        )

    def _execute(self):
        # Direct the driver to the login page
        self._driver.get(self.login_url)

        # Create page helpers
        login_page = LoginPage(self._driver)
        navigation = Navigation(self._driver)
        meter_selection_page = MeterSelectionPage(self._driver)
        export_csv_page = ExportCsvPage(self._driver)
        download_csv_page = DownloadCsvPage(self._driver)

        # Authenticate
        login_page.wait_until_ready()
        self.screenshot("before login")
        login_page.login(self.username, self.password)

        # Configure interval data generation, in two steps...
        meter_selection_page.wait_until_ready()
        self.screenshot("before meter selection")

        # 1) Specify we are entering a custom date range
        meter_selection_page.select_date_range_option()
        self.screenshot("date range option selected")

        # 2) Locate the meter of interest and select it
        matching_meter = None
        meter_query = self.ngrid_meter_id
        log.info("Looking for a meter with ID == {0}".format(meter_query))
        for meter in meter_selection_page.iter_meters():
            log.info("Found a meter: {0}".format(meter))
            if meter.meter_id == meter_query:
                log.info("Found a matching meter.")
                matching_meter = meter
                break

        if matching_meter is None:
            log.info("No meter with ID {0} was found.".format(meter_query))
            raise InvalidMeterException("Meter {0} was not found".format(meter_query))
        else:
            matching_meter.select()
            self.screenshot("meter selected")

        # Two notes on time...
        # 1) Each meter specifies the date range for which data is
        #    available. If we don't respect this, the page will throw
        #    errors. So, we restrict our start and end dates based on
        #    this information.
        if self.start_date < matching_meter.min_date:
            log.info(
                "Adjusting start date from {0} to {1}".format(
                    self.start_date, matching_meter.min_date
                )
            )
            self.start_date = matching_meter.min_date
        if self.end_date > matching_meter.max_date:
            log.info(
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
        for subrange in date_range.split_iter(delta=interval_size):
            log.info("Gathering meter data for: {0}".format(subrange))

            # First, set the date range for the selected meter
            meter_selection_page.set_date_range(subrange)

            # Navigate to the "Export" page, and request a CSV report
            navigation.goto_export()
            export_csv_page.wait_until_ready()
            export_csv_page.generate_report()

            # Wait for the report to generate, then download it
            # and extract interval data from it
            download_csv_page.wait_until_ready()
            csv_rows_iter = download_csv_page.get_csv_rows()
            header = next(csv_rows_iter)
            log.info("CSV Header row: {0}".format(header))

            for data_row in csv_rows_iter:
                result = NationalGridIntervalScraper.parse_csv_row(data_row)
                if result.units == UNITS_KWH:
                    readings[self._iso_str(result.date)] = list(
                        NationalGridIntervalScraper.kwh_to_kw(result.interval_data)
                    )

            # Navigate back to the meter selection page in preparation
            # for the next iteration. Note that we do not reselect the
            # meter, since our initial selections are cached.
            navigation.goto_meter_selection()
            meter_selection_page.wait_until_ready()

        return Results(readings=readings)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    meta = datasource.meta or {}
    configuration = NationalGridConfiguration(
        meter_id=meta.get("ngridMeterNumber"), account_id=meter.utility_account_id
    )

    return run_datafeed(
        NationalGridIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
