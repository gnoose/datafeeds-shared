"""A Selenium-based scraper for the SDGE MyAccount webpage.

This replaces a PhantomJS/CasperJS based scraper. The rationale
for moving to Selenium + Chrome Headless is: greater stability
and maintainability.
"""

import csv
import io
from collections import defaultdict, namedtuple
import logging
import os
import re
from datetime import datetime
from typing import Optional
from zipfile import ZipFile

from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser
from dateutil.rrule import rrule, SU, YEARLY
from retrying import retry

from datafeeds.common.batch import run_datafeed

from datafeeds.common.support import DateRange
from datafeeds.common.support import Results
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import (
    file_exists_in_dir,
    IFrameSwitch,
    clear_downloads,
)
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


class InvalidAccountException(Exception):
    """Raised when an account number cannot be found on the site."""

    pass


class InvalidMeterException(Exception):
    """Raised when a meter number cannot be found on the site"""

    pass


class InvalidIntervalDataException(Exception):
    """Indicates invalid interval (e.g. insufficient data for a day)"""

    pass


class ScraperTimeout(Exception):
    """Indicates that a Selenium timeout occurred"""

    pass


logger = None
log = logging.getLogger(__name__)

# Represents a row in a CSV file downloaded from the SDGE MyAccount site.
# The field names correspond to column names defined in such file.
CsvRow = namedtuple(
    "CsvRow",
    [
        "MeterNumber",
        "Date",
        "StartTime",
        "Duration",
        "Value",
        "EditCode",
        "FlowDirection",
        "TOU",
    ],
)

# An interval reading drawn from the SDGE CSV file. Has a date/time, and a
# value (units, KW). The CSV has units of KWH, so the value here should
# differ by a factor of 4.
RawReading = namedtuple("RawReading", ["date", "time", "value"])


class AttributeMatches:
    """Wait until a tag attribute matches a regex.

    This is passed to a Selenium wait().until construct. A Selenium locator
    is used to find a given web element, then a given attribute is monitored
    on that element until it matches a given regex.

    Args:
        locator: A Selenium locator (e.g. (By.CSS_SELECTOR, selector))
        attribute (str): The attribute to watch
        pattern (str): A regular expression
    """

    def __init__(self, locator, attribute, pattern):
        self.locator = locator
        self.attribute = attribute
        self.pattern = re.compile(pattern)

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        if self.pattern.match(element.get_attribute(self.attribute)):
            return element
        return False


class SdgeMyAccountConfiguration(Configuration):
    """Configuration for the SDGE MyAccount Scraper

    Args:
        account_id (str): Identifies a specific account.
            Some weirdness: the account ids in the webapp have one fewer
            digit compared to those in admin (the webapp ids are missing
            the last digit). This parameter represents the id from admin.
            The discrepancy noted here is handled in code in the scraper.
        service_id (str): Identifies a specific meter
    """

    def __init__(self, account_id: str, service_id: str, direction: str):
        super().__init__(scrape_readings=True)
        self.account_id = account_id
        self.service_id = service_id
        self.direction = direction


class ExportCsvDialog:
    """The export CSV page/dialog on the SDGE MyAccount site.

    This dialog is used to export CSV interval date for a given
    meter and date range.
    """

    # Used to select a meter by its ID
    SelectMeterCss = 'select[id="ddlMeters"]'

    # Used to set start/end date for the export
    StartDateCss = 'input[id="txtStartDate"]'
    EndDateCss = 'input[id="txtEndDate"]'

    # The dialog reports the dates for which interval data is available.
    # These selectors can be used to find the min/max of that range.
    MinDateCss = 'label[for="lblHistory"]'
    MaxDateCss = 'label[for="lblRecentAvaliableDataDate"]'

    # The export button generates the CSV report, along with a download link...
    ExportCss = 'a[id="la-gb-export"]'
    # ...which can be found (the download link) with this selector.
    DownloadCss = 'a[id="lnkDownload"]'
    # The background of the CSV dialog
    BackgroundCss = 'div[id="la-greenbutton-container"]'

    # This dialog runs inside an iframe with the following ID
    TargetIFrame = "pt1:if1"

    def __init__(self, driver):
        self._driver = driver

    def _remove_attribute(self, id_attr, attribute):
        """Remove an attribute from an element with the given id"""
        script = 'document.getElementById("{0}").removeAttribute("{1}")'
        self._driver.execute_script(script.format(id_attr, attribute))

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""
        with IFrameSwitch(self._driver, self.TargetIFrame):
            self._driver.wait().until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.StartDateCss))
            )

    def get_available_meters(self):
        """Return an iterable of available meter ids"""
        with IFrameSwitch(self._driver, self.TargetIFrame):
            meter_selector = self._driver.find_element_by_css_selector(
                self.SelectMeterCss
            )
            for option in Select(meter_selector).options:
                yield option.get_attribute("value")

    def select_meter(self, service_id):
        """Choose a meter by id from a dropdown.

        Returns False if the selection fails, e.g. if the given id doesn't exist
        in the dropdown. Returns True otherwise.
        """
        with IFrameSwitch(self._driver, self.TargetIFrame):
            meter_selector = self._driver.find_element_by_css_selector(
                self.SelectMeterCss
            )

            select = Select(meter_selector)

            # Sometimes, the meter id in the selector has a leading 0 that
            # isn't in the id we get from admin. Therefore, we search the
            # options manually to take this into account.
            target_value = None
            for opt in select.options:
                val = opt.get_attribute("value")
                if val == service_id or val.lstrip("0") == service_id:
                    target_value = val
            if target_value is None:
                return False

            try:
                select.select_by_value(target_value)
            except NoSuchElementException:
                return False

            return True

    def begin_export_csv(self):
        """Start the CSV Export."""
        log.info("Starting CSV export")
        # TODO: This is a little arduous right now, and I'd like to simplify if
        # possible. We click the background to make sure that previous UI
        # elements lose focus, then move to the export button and click
        # it. Without doing this, I was getting weird errors due to UI
        # elements occluding things I needed to interact with.
        with IFrameSwitch(self._driver, self.TargetIFrame):
            export = self._driver.find_element_by_css_selector(self.ExportCss)
            background = self._driver.find_element_by_css_selector(self.BackgroundCss)
            action_chains = ActionChains(self._driver)
            action_chains.click(background)
            action_chains.pause(5)
            action_chains.move_to_element(export)
            action_chains.pause(10)
            action_chains.click(export)
            log.debug("\tstarting action chain")
            log.debug("click background: %s", self.BackgroundCss)
            log.debug("move to / click export: %s", self.ExportCss)
            action_chains.perform()

    def wait_until_export_done(self):
        """Wait for the CSV export to finish. This might take 10s of seconds"""
        log.info("Waiting for CSV export: %s", self.DownloadCss)
        with IFrameSwitch(self._driver, self.TargetIFrame):
            # Be a little more generous with this wait
            wait = WebDriverWait(self._driver, 180)
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.DownloadCss))
            )

            # Wait until the link has a certain href value (ends with .zip)
            self._driver.wait().until(
                AttributeMatches(
                    (By.CSS_SELECTOR, self.DownloadCss), "href", r".*\.zip"
                )
            )

    def download_csv_file(self):
        """Download the CSV file to disk"""

        @retry(stop_max_attempt_number=3, wait_fixed=10000)
        def get_download_link():
            return self._driver.find_element_by_css_selector(self.DownloadCss)

        with IFrameSwitch(self._driver, self.TargetIFrame):
            download_link = get_download_link()

            # We open the download link in a new window (accomplished with
            # a shift-click). Selenium got very confused if a clicked the
            # link directly; I could no longer interact with the page.

            # NOTE: This might not be resilient across browsers, so this
            # scraper should only use the Chrome driver.

            # NOTE: I tried to download the file with requests, but this
            # didn't work, as the link here is not a direct link.
            action_chains = ActionChains(self._driver)
            action_chains.move_to_element(download_link)
            action_chains.pause(5)
            action_chains.key_down(Keys.SHIFT)
            action_chains.click(download_link)
            action_chains.key_up(Keys.SHIFT)
            action_chains.perform()

    def set_date_range(self, start_date, end_date):
        """Set the export date range."""
        with IFrameSwitch(self._driver, self.TargetIFrame):
            start_field = self._driver.find_element_by_css_selector(self.StartDateCss)
            end_field = self._driver.find_element_by_css_selector(self.EndDateCss)

            # NOTE: we remove the readonly attribute on these elements
            # so that we can set their value directly (with send_keys)
            # We also remove the class to prevent the date-picker from
            # showing up (this might not be necessary).
            self._remove_attribute("txtStartDate", "readonly")
            self._remove_attribute("txtStartDate", "class")
            self._remove_attribute("txtEndDate", "readonly")
            self._remove_attribute("txtEndDate", "class")

            date_fmt = "%m/%d/%Y"
            # .clear doesn't always work, but this should
            for _ in range(11):
                start_field.send_keys(Keys.BACKSPACE)
                end_field.send_keys(Keys.BACKSPACE)
            self._driver.sleep(1)
            start_field.send_keys(start_date.strftime(date_fmt))
            end_field.send_keys(end_date.strftime(date_fmt))

    def get_min_start_date(self):
        """Get the min possible start date from the UI."""
        with IFrameSwitch(self._driver, self.TargetIFrame):
            min_date_label = self._driver.find_element_by_css_selector(self.MinDateCss)
            parts = min_date_label.text.split()
            date_str = parts[-1]
            return dateparser.parse(date_str).date()

    def get_max_start_date(self):
        """Get the max possible start date from the UI."""
        with IFrameSwitch(self._driver, self.TargetIFrame):
            max_date_label = self._driver.find_element_by_css_selector(self.MaxDateCss)
            parts = max_date_label.text.split()
            date_str = parts[-1]
            return dateparser.parse(date_str).date()


class MyEnergyPage:
    """Represents the SDGE MyAccount 'My Energy' tab

    We do a few things on this page: select the desired account,
    navigate to the 'Energy Use' view, and then open the CSV export
    dialog.
    """

    # When multiple accounts are available, this selector will be visible.
    AccountSelectorCss = 'select[id="pt1:acctlst"]'
    # This hidden input field holds the currently selected account.
    HiddenAccountNumberCss = 'input[id="accountNumberHiddenElem"]'
    # This span holds the rendered, active account number
    AccountNumberXpath = '//span[@id="pt1:pgl1"]/div[1]/div[2]'

    # This page has a dropdown that is used to switch to different
    # "views", e.g. the 'Energy Use' view
    ViewSelectorCss = 'select[id="pt1:soc1"]'

    # Opens the Export CSV dialog
    ExportLinkCss = 'a[id="la-csvexport-view-trigger"]'

    # Some elements live in iframe with this id
    TargetIFrame = "pt1:if1"

    def __init__(self, driver):
        self._driver = driver

    def _try_get_account_selector(self):
        """Return the account selector web element, if it's present.

        If there is only one account associated with the sign-in, this selector
        will not be present, and this function will return 'None'
        """
        account_selector = None
        try:
            account_selector = self._driver.find_element_by_css_selector(
                self.AccountSelectorCss
            )
        except NoSuchElementException:
            pass
        return account_selector

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""
        log.debug("waiting for HiddenAccountNumberCss %s", self.HiddenAccountNumberCss)
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.HiddenAccountNumberCss)
            )
        )

    def get_active_account_number(self):
        """ Return the currently active account number."""
        hidden_input = self._driver.find_element_by_css_selector(
            self.HiddenAccountNumberCss
        )
        return hidden_input.get_attribute("value")

    def get_available_accounts(self):
        """Return an iterable of available account ids"""

        # Some sign-ins only have a single account associated with them.
        # In these cases, there is no account selector present, and we
        # will just yield the currently active account
        account_selector = self._try_get_account_selector()
        if account_selector is not None:
            for option in Select(account_selector).options:
                yield option.get_attribute("value")
        else:
            yield self.get_active_account_number()

    def select_account(self, account_id):
        """Select the desired account.

        Returns False if the selection fails, e.g. because the desired account
        could not be found. Returns True otherwise."""

        # Account numbers on the webpage only have the first ten characters
        if len(account_id) > 10:
            account_id = account_id[:10]

        # If we have the ability to select accounts, try to do so, and if
        # successful, wait for the page to update with the new account info
        account_selector = self._try_get_account_selector()
        if account_selector is not None:
            try:
                Select(account_selector).select_by_value(account_id)
            except NoSuchElementException:
                return False

            self._driver.wait().until(
                EC.text_to_be_present_in_element_value(
                    (By.CSS_SELECTOR, self.HiddenAccountNumberCss), account_id
                )
            )
            return True

        # Otherwise:
        # If there is no account selector (meaning there is only one account)
        # for the current sign in), just check if the desired account number
        # matches the currently active account.
        active_account = self.get_active_account_number()
        return active_account == account_id

    def select_my_energy_use(self):
        """Move to the 'Energy Use' view"""
        view_selector = self._driver.find_element_by_css_selector(self.ViewSelectorCss)
        Select(view_selector).select_by_visible_text("My Energy Use")

    def wait_until_energy_use_ready(self):
        """Wait until the 'Energy Use' view is ready to interact with."""
        self._driver.wait().until(
            EC.presence_of_element_located((By.ID, self.TargetIFrame))
        )

        with IFrameSwitch(self._driver, self.TargetIFrame):
            self._driver.wait().until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.ExportLinkCss))
            )

    def navigate_to_csv_export(self):
        """Open the export CSV dialog."""

        @retry(stop_max_attempt_number=3, wait_fixed=10000)
        def click_csv_export():
            # This fails sometimes (for AJAX reasons), so retry.
            self._driver.find_element_by_css_selector(self.ExportLinkCss).click()

        with IFrameSwitch(self._driver, self.TargetIFrame):
            click_csv_export()


class HomePage:
    """Represents the SDGE MyAccount homepage, which appears post login."""

    AccountsCss = 'span[id="pt1:tblAccounts"]'
    NavBarCss = "div .navbar"
    AccountCellCss = 'td[abbr="Account"]'
    PaperlessPopupCss = 'div[id="paperlessModal"]'

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""

        # Sometimes, a popup appears asking if we want to "Go Paperless". We
        # will wait 10 seconds for this popup to appear, closing it if it does.
        popup_wait = WebDriverWait(self._driver, 10)
        popup = None
        try:
            popup_wait.until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, self.PaperlessPopupCss)
                )
            )
            popup = self._driver.find_element_by_css_selector(self.PaperlessPopupCss)
        except:  # noqa: E722
            # If we can't find this popup, no worries
            log.info("no popup")
            pass

        # Close the popup if we found it
        if popup is not None:
            log.info("closing popup")
            close_button = popup.find_element_by_css_selector("button")
            # Pause after closing to make sure it disappears
            actions = ActionChains(self._driver)
            actions.click(close_button)
            actions.pause(5)
            actions.perform()

        selectors = [self.NavBarCss, self.AccountsCss, self.AccountCellCss]
        for css in selectors:
            self._driver.wait(120).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css))
            )

    def navigate_to_my_energy(self):
        """Move to the 'My Energy' page"""
        my_energy = self._driver.find_element_by_link_text("My Energy")
        action_chains = ActionChains(self._driver)
        action_chains.move_to_element(my_energy)
        # Pause for a moment to let the dropdown render
        action_chains.pause(3)
        action_chains.perform()

        my_energy_overview = self._driver.find_element_by_link_text(
            "My Energy Overview"
        )
        my_energy_overview.click()


class LoginPage:
    """Represents the authentication page in the web UI."""

    # Nothing fancy here. There is a username field, password field,
    # and login button
    UsernameFieldCss = 'input[id="UserID"]'
    PasswordFieldCss = 'input[id="Password"]'
    LoginButtonCss = 'button[id="jsLoginBtn"]'
    RememberMeXpath = '//label[@for="RememberMe"] //span'
    # always on page but hidden until bad credentials validated
    FailedLoginSelector = "#UserIdPasswordInvalid"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""
        log.info("Waiting for 'Login' page to be ready...")
        selectors = [self.UsernameFieldCss, self.PasswordFieldCss, self.LoginButtonCss]
        for css in selectors:
            self._driver.wait().until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css))
            )

    def get_login_button(self):
        return self._driver.find_element_by_css_selector(self.LoginButtonCss)

    def login(self, username, password, scraper):
        log.info("Inserting credentials on login page.")
        log.debug("setting username")
        username_field = self._driver.find_element_by_css_selector(
            self.UsernameFieldCss
        )
        log.debug("clearing username")
        username_field.click()
        for _ in range(len(username)):
            username_field.send_keys(Keys.BACKSPACE)
        username_field.send_keys(username)
        self._driver.sleep(1)
        log.debug("setting password")
        password_field = self._driver.find_element_by_css_selector(
            self.PasswordFieldCss
        )
        password_field.send_keys(password)
        self._driver.sleep(1)
        scraper.screenshot("after credentials")
        # click remember me to make sure focus exits password
        log.debug("clicking remember me")
        # input is absolutely positioned off the screen; click the label span instead
        self._driver.find_element_by_xpath(self.RememberMeXpath).click()
        scraper.screenshot("after remember me")
        self._driver.sleep(1)
        self.get_login_button().click()
        try:
            self._driver.wait(5).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, self.FailedLoginSelector)
                )
            )
            raise LoginError("Invalid username or login.")
        except TimeoutException:
            return  # Login succeeded.


def wait_for_download(driver, timeout=60):
    """Wait for a download to finish.

    In particular, wait for a zip file to show up in the download directory.
    """
    wait = WebDriverWait(driver, timeout)
    download_dir = driver.download_dir

    filename = wait.until(file_exists_in_dir(download_dir, r".*\.zip$"))
    filepath = os.path.join(download_dir, filename)
    return filepath


def extract_csv_rows(download_path):
    """Pull CSV data from the downloaded zip file."""
    with ZipFile(download_path) as zip_file:
        csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]
        if len(csv_files) != 1:
            pass  # TODO handle error

        target_file = csv_files[0]
        with zip_file.open(target_file) as data_file:
            csv_reader = csv.reader(io.TextIOWrapper(data_file))
            seen_header = False
            for row in csv_reader:
                if len(row) == 2:
                    log.info("CSV Metadata: {0} = {1}".format(row[0], row[1]))
                elif len(row) == 8:
                    if not seen_header:
                        seen_header = True
                    else:
                        yield CsvRow._make(row)


def to_raw_reading(csv_row, direction: str):
    """Convert a CSV row to an interval reading."""
    reading_date = dateparser.parse(csv_row.Date).date()
    reading_time = dateparser.parse(csv_row.StartTime).time()
    kwh_value = float(csv_row.Value)
    if (
        direction == "forward"
        and kwh_value < 0
        or direction == "reverse"
        and kwh_value > 0
    ):
        log.info(
            "dropping reading for %s meter on %s %s: invalid sign %s",
            direction,
            reading_date,
            reading_time,
            kwh_value,
        )
        return RawReading(date=reading_date, time=reading_time, value=None)
    # Multiply the KWH value by 4 to get KW
    return RawReading(date=reading_date, time=reading_time, value=kwh_value * 4)


DST_STARTS = set(
    # Daylight savings time starts on the second Sunday in March
    dt.date()
    for dt in rrule(
        YEARLY, bymonth=3, byweekday=SU(2), dtstart=datetime(2000, 1, 1), count=100
    )
)


DST_ENDS = set(
    # Daylight savings time ends on the first Sunday in November
    dt.date()
    for dt in rrule(
        YEARLY, bymonth=11, byweekday=SU(1), dtstart=datetime(2000, 1, 1), count=100
    )
)


def adjust_for_dst(day, readings):
    if day in DST_STARTS:
        for i in range(8, 12):
            readings[i] = None
    elif day in DST_ENDS:
        for i in range(4, 8):
            readings[i] = readings[i] / 2

    return readings


class SdgeMyAccountScraper(BaseWebScraper):
    """The main SDGE MyAccount scraper entry point."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SDGE MyAccount"
        self.login_url = "https://myaccount.sdge.com"

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def service_id(self):
        return self._configuration.service_id

    @property
    def direction(self):
        return self._configuration.direction

    def _execute(self):
        try:
            return self._execute_internal()
        except TimeoutException:
            self.screenshot("timeout")
            raise ScraperTimeout(
                "Scraper timed out on waiting for an element to appear: %s"
                % self._driver.current_url
            )

    def _execute_internal(self):
        # Direct the driver to the login page
        self._driver.get(self.login_url)

        # Create page helpers
        login_page = LoginPage(self._driver)
        home_page = HomePage(self._driver)
        my_energy_page = MyEnergyPage(self._driver)
        export_csv_dialog = ExportCsvDialog(self._driver)

        # Authenticate
        log.info("Logging in.")
        login_page.wait_until_ready()
        self.screenshot("before login")
        # login seems to sometimes fail; try twice
        try:
            login_page.login(self.username, self.password, self)
        except LoginError:
            log.info("login failed; trying login a second time in 30s")
            self._driver.get(self.login_url)
            self._driver.sleep(30)
            self.screenshot("before second login")
            login_page.login(self.username, self.password, self)
        self.screenshot("after login")

        # On the homepage, fetch the visible account information. This info
        # tells us (among other things) which account id is associated with
        # which account name.
        log.info("Waiting for home page to be ready.")
        home_page.wait_until_ready()
        self.screenshot("home page loaded")

        # Go to the 'My Energy' Page
        log.info("Navigating to 'My Energy' page.")
        home_page.navigate_to_my_energy()
        my_energy_page.wait_until_ready()
        self.screenshot("my_energy_page_initial")

        log.info("Selecting account: {0}".format(self.account_id))
        if not my_energy_page.select_account(self.account_id):
            available_accounts = set(my_energy_page.get_available_accounts())
            error_msg = "Unable to find account with ID={0}. Available accounts are: {1}".format(
                self.account_id, available_accounts
            )
            log.info(error_msg)
            raise InvalidAccountException(error_msg)

        self.screenshot("my_energy_page_account_selected")
        log.info("Navigating to 'My Energy Use'")
        my_energy_page.select_my_energy_use()
        my_energy_page.wait_until_energy_use_ready()
        self.screenshot("my_energy_page_energy_use")

        log.info("Opening 'Export CSV' dialog.")
        my_energy_page.navigate_to_csv_export()
        self.screenshot("navigate_to_csv")
        export_csv_dialog.wait_until_ready()
        self.screenshot("csv_export_dialog_initial")

        # Select the desired meter
        log.info("Selecting meter with id: {0}".format(self.service_id))
        if not export_csv_dialog.select_meter(self.service_id):
            available_meters = set(export_csv_dialog.get_available_meters())
            error_msg = "Unable to find meter with ID={0}. Available meters are: {1}".format(
                self.service_id, available_meters
            )
            raise InvalidMeterException(error_msg)
        self.screenshot("csv_export_dialog_meter_selected")

        # Truncate the date range, if necessary
        min_date = export_csv_dialog.get_min_start_date()
        max_date = export_csv_dialog.get_max_start_date()
        if self.start_date < min_date:
            log.info(
                "Adjusting start date from {0} to {1}".format(self.start_date, min_date)
            )
            self.start_date = min_date
        if self.end_date > max_date:
            log.info(
                "Adjusting end date from {0} to {1}".format(self.end_date, max_date)
            )
            self.end_date = max_date

        # This page only allows you to download a certain amount of
        # billing data at a time. We will use a conservative chunk
        # size of 180 days.
        date_range = DateRange(self.start_date, self.end_date)
        interval_size = relativedelta(days=180)
        raw_readings = defaultdict(list)

        for subrange in date_range.split_iter(delta=interval_size):
            log.info("Getting interval data for date range: {0}".format(subrange))
            start = subrange.start_date
            end = subrange.end_date

            # Set the date range in the UI, then click "Export"
            log.info("Setting date range.")
            export_csv_dialog.set_date_range(start, end)
            self.screenshot(
                "csv_export_{0}-{1}".format(start.isoformat(), end.isoformat())
            )

            log.info("Clicking 'Export' button.")
            export_csv_dialog.begin_export_csv()
            log.info("done begin_export_csv")
            export_csv_dialog.wait_until_export_done()

            # Save the current window handle. We will be closing windows
            # later, and don't want to close this one
            current_window = self._driver.current_window_handle

            # Click the download link, and wait for it to finish...
            log.info("Clicking download link.")
            export_csv_dialog.download_csv_file()
            download_path = wait_for_download(self._driver)

            log.info("Processing downloaded file: {0}".format(download_path))
            # ...then process the downloaded file.
            for row in extract_csv_rows(download_path):
                raw_reading = to_raw_reading(row, self.direction)
                raw_readings[raw_reading.date].append(raw_reading)

            # Clean up any downloaded files
            log.info("Cleaning up downloads.")
            clear_downloads(self._driver.download_dir)

            # Close any windows that might have been opened
            log.info("Closing extra windows")
            handles = list(self._driver.window_handles)
            handles.remove(current_window)
            for handle in handles:
                self._driver.switch_to_window(handle)
                self._driver.close()
                self._driver.switch_to_window(current_window)

        log.info("Processing raw interval data.")
        # Convert the raw readings into the expected dictionary of results
        result = {}
        for key in sorted(raw_readings.keys()):
            iso_str = key.strftime("%Y-%m-%d")
            sorted_readings = sorted(raw_readings[key], key=lambda r: r.time)
            if len(sorted_readings) == 96:
                result[iso_str] = adjust_for_dst(
                    key, [x.value for x in sorted_readings]
                )
            else:
                error_msg = "Incomplete interval data ({0}: {1} readings)"
                error_msg = error_msg.format(iso_str, len(sorted_readings))
                raise InvalidIntervalDataException(error_msg)
        return Results(readings=result)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """Run scraper for SDGE MyAccount if enabled.

    Retrying a bad login will lock the account. If a login fails, mark all data sources
    for this account as disabled.
    """

    configuration = SdgeMyAccountConfiguration(
        meter.utility_account_id, meter.service_id, meter.direction
    )
    return run_datafeed(
        SdgeMyAccountScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
