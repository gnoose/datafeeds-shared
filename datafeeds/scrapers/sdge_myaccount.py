"""A Selenium-based scraper for the SDGE MyAccount webpage.

This replaces a PhantomJS/CasperJS based scraper. The rationale
for moving to Selenium + Chrome Headless is: greater stability
and maintainability.
"""

import csv
from collections import namedtuple
from glob import glob
import logging
import os
import re
from datetime import date, datetime, time, timedelta
from typing import Optional

import pandas as pd
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser
from dateutil.rrule import rrule, SU, YEARLY

from datafeeds.common.batch import run_datafeed

from datafeeds.common.support import DateRange
from datafeeds.common.support import Results
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Configuration
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import (
    file_exists_in_dir,
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
        "Consumption",
        "Generation",
        "Net",
    ],
)

# An interval reading drawn from the SDGE CSV file. Has a date/time, and a
# value (units, KW). For 15 minute electric meters, the CSV has units of KWH, so the value here should
# differ by a factor of 4. For daily gas mters, use the value as-is.
RawReading = namedtuple("RawReading", ["date", "time", "value"])


def close_modal(driver):
    # Sometimes, a popup appears asking if we want to "Go Paperless". We
    # will wait 10 seconds for this popup to appear, closing it if it does.
    popup_wait = WebDriverWait(driver, 10)
    popup = None
    try:
        popup_wait.until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, 'div[id="paperlessModal"]')
            )
        )
        popup = driver.find_element_by_css_selector('div[id="paperlessModal"]')
    except:  # noqa: E722
        # If we can't find this popup, no worries
        log.info("no popup")
        pass

    # Close the popup if we found it
    if popup is not None:
        log.info("closing popup")
        close_button = popup.find_element_by_css_selector("button")
        # Pause after closing to make sure it disappears
        actions = ActionChains(driver)
        actions.click(close_button)
        actions.pause(5)
        actions.perform()

    try:
        driver.wait(5).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".modal-header button.close")
            )
        )
        driver.find_element_by_css_selector(".modal-header button.close").click()
    except:  # noqa: E722
        # If we can't find this popup, no worries
        pass


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

    def __init__(
        self,
        account_id: str,
        service_id: str,
        direction: str,
        interval: int,
        commodity: str,
    ):
        super().__init__(scrape_readings=True)
        self.account_id = account_id
        self.service_id = service_id
        self.direction = direction
        self.commodity = commodity
        self.interval = interval
        if interval == 15 and commodity == "kw":
            self.adjustment_factor = 4  # adjust 15-minute readings to kWh
        else:
            self.adjustment_factor = 1


class UsagePage:
    def __init__(self, driver):
        self._driver = driver

    def is_enterprise(self):
        """Enterprise version has a different URL and layout."""
        return "-ent.sdge.com" in self._driver.current_url

    def navigate_to_usage_page(self):
        self._driver.wait(10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".AccountslctClass"))
        )
        if self.is_enterprise():
            self._driver.get("https://myaccount-ent.sdge.com/Portal/Usage/Index")
        else:
            self._driver.get("https://myaccount.sdge.com/portal/Usage/Index")

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""
        log.debug("waiting for account list select")
        if self.is_enterprise():
            selector = "#multiple-selectAccount"
        else:
            selector = 'button[data-id="accountList"]'
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        )

    def _try_get_account_selector(self):
        """Return the account selector web element, if it's present.

        If there is only one account associated with the sign-in, this selector
        will not be present, and this function will return 'None'
        """
        account_selector = None
        try:
            account_selector = self._driver.find_element_by_css_selector("#accountList")
        except NoSuchElementException:
            pass
        return account_selector

    def get_available_accounts(self):
        """Return an iterable of available account ids"""

        # Some sign-ins only have a single account associated with them.
        # In these cases, there is no account selector present, and we
        # will just yield the currently active account
        account_selector = self._try_get_account_selector()
        if account_selector is not None:
            self._driver.find_element_by_css_selector(
                'button[data-id="accountList"]'
            ).click()
            self._driver.wait(10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.show"))
            )
            for account_span in self.driver.find_elements_by_css_selector(
                '.AccountslctClass a[role="option"] .smallcontent'
            ):
                yield account_span.text
        else:
            # TODO: need example with single account
            yield None

    def wait_for_loading_spinner(self):
        # wait for loading spinner mdl-spinner
        self._driver.wait(2).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, ".mdl-spinner"))
        )

    def switch_portfolio(self, index: int) -> int:
        log.debug(f"switch to portfolio {index}")
        # open dropdown
        self._driver.find_element_by_css_selector(
            'button[data-id="single-Select1"]'
        ).click()
        portfolio_text = [
            el.text
            for el in self._driver.find_elements_by_css_selector(".AccountslctClass li")
            if el.text
        ]
        log.info(f"portfolio ids={portfolio_text}")
        for idx, li in enumerate(
            self._driver.find_elements_by_css_selector(".AccountslctClass li")
        ):
            if idx != index:
                continue
            log.info(f"clicking portfolio {idx} {li.text}")
            li.click()
            self.wait_for_loading_spinner()
            self._driver.sleep(3)
            return len(portfolio_text)
        return len(portfolio_text)

    def select_enterprise_account(self, search_account: str) -> bool:
        found = False
        self.wait_for_loading_spinner()
        log.debug(f"looking for account {search_account}")
        # open accounts dropdown
        self._driver.wait(20).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "#multiple-selectAccount button")
            )
        )
        self._driver.find_element_by_css_selector(
            "#multiple-selectAccount button"
        ).click()
        self._driver.sleep(3)
        for option in self._driver.find_elements_by_css_selector(
            "#multiple-selectAccount .dropdown-menu li"
        ):
            log.info(f"found account {option.text}")
            if search_account in option.text or search_account[:-1] in option.text:
                log.info(f"clicking option text={option.text}")
                option.click()
                self.wait_for_loading_spinner()
                self._driver.sleep(3)
                found = True
                break
        return found

    def select_account_from_portfolio(self, account_id: str):
        # Account numbers on the webpage don't have leading leading zeros, or the last digit
        search_account = re.sub(r"^0+", "", account_id)
        portfolios = self.switch_portfolio(0)
        found = self.select_enterprise_account(search_account)
        if not found:
            for idx in range(portfolios - 1):
                self.switch_portfolio(idx + 1)
                found = self.select_enterprise_account(search_account)
                if found:
                    break
        log.debug(f"found={found}")
        self._driver.screenshot(
            BaseWebScraper.screenshot_path("portfolio click account")
        )
        return found

    def enterprise_select_usage(self, interval: int):
        log.debug("select usage and resolution")
        self._driver.find_element_by_css_selector("#usageView").click()
        usage_type = "Min" if interval == 15 else "Day"
        selector = f'li[type="{usage_type}"] a'
        log.debug(f"waiting for {selector}")
        self.wait_for_loading_spinner()
        self._driver.wait(10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
        )
        self._driver.find_element_by_css_selector(selector).click()
        self._driver.screenshot(
            BaseWebScraper.screenshot_path("portfolio click account")
        )

    def enterprise_download(self, from_dt: date, to_dt: date, interval: int):
        """Download data via script; 15 minute meters can only download one day at time."""
        usage_type = "GetUsageByMin" if interval == 15 else "GetUsageByDay"
        script = f"""
        g_payload = usage.FormatRequestPayload();
        g_payload["FromDate"] = new Date('{from_dt.isoformat()}');
        g_payload["ToDate"] = new Date('{to_dt.isoformat()}');
        usage.UsageOperation("ExportExcel", g_payload, "{usage_type}");
        """
        log.debug(f"download {from_dt} - {to_dt}: executing script {script}")
        self._driver.execute_script(script)
        self._driver.sleep(3)

    def select_account(self, account_id: str):
        """Select the desired account.

        Returns False if the selection fails, e.g. because the desired account
        could not be found. Returns True otherwise."""

        # Account numbers on the webpage don't have leading leading zeros, or the last digit
        search_account = re.sub(r"^0+", "", account_id)
        log.info(f"select_account {account_id} enterprise={self.is_enterprise()}")
        # If we have the ability to select accounts, try to do so, and if
        # successful, wait for the page to update with the new account info
        account_selector = self._try_get_account_selector()
        # click to open the dropdown
        if account_selector is not None:
            self._driver.find_element_by_css_selector(
                'button[data-id="accountList"]'
            ).click()
            self._driver.wait(10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.show"))
            )
            self._driver.sleep(3)
            found = False
            for option in self._driver.find_elements_by_css_selector(
                ".AccountslctClass .dropdown li"
            ):
                if search_account in option.text or search_account[:-1] in option.text:
                    option.click()
                    found = True
                    break
            self._driver.screenshot(BaseWebScraper.screenshot_path("click account"))
            if not found:
                raise InvalidAccountException(
                    f"account {account_id} not found in dropdown"
                )
        else:
            log.info("no account selector; single account")
        try:
            self._driver.wait(10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".acctMtrNotEligible"))
            )
            raise InvalidAccountException("account is not eligible")
        except TimeoutException:
            return True

    def get_available_meters(self):
        """Return an iterable of available meter ids"""
        self._driver.find_element_by_css_selector("#meterDiv .md-select-icon").click()
        self._driver.wait(10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".md-clickable"))
        )
        for option in self._driver.find_elements_by_css_selector(
            ".md-clickable md-select-menu md-option"
        ):
            yield option.text

    def select_meter(self, service_id: str) -> bool:
        # click meter dropdown
        self._driver.find_element_by_css_selector("#meterDiv .md-select-value").click()
        self._driver.wait(10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".md-clickable"))
        )
        self._driver.screenshot(BaseWebScraper.screenshot_path("click meter dropdown"))
        # if there's more than one md-option, click one
        options = [
            el
            for el in self._driver.find_elements_by_css_selector(
                ".md-clickable md-select-menu md-option"
            )
        ]
        if len(options) == 1:
            log.debug("only one meter option; clicking")
            self._driver.find_element_by_css_selector(".md-clickable md-option").click()
            return True
        # remove leading 0s
        short_service_id = re.sub("^0+", "", service_id)
        log.info(f"service_id={service_id}; looking for {short_service_id}")
        for option in self._driver.find_elements_by_css_selector(
            ".md-clickable md-select-menu md-option"
        ):
            log.info(f"option text={option.text}")
            if service_id in option.text or short_service_id in option.text:
                log.debug(f"click option {option} ({option.tag_name}")
                option.click()
                return True
        return False

    def open_green_button(self):
        # click GreenButton Download
        self._driver.find_element_by_css_selector('a[data-target="#grButton"]').click()
        self._driver.screenshot(BaseWebScraper.screenshot_path("green button modal"))

    def download(self, start: date, end: date):
        # set date range in JavaScript instead of trying to navigate date picker
        self._driver.execute_script(
            "fromDate = new Date(Date.parse('%s'));" % start.strftime("%Y-%m-%d")
        )
        self._driver.execute_script(
            "toDate = new Date(Date.parse('%s'));" % end.strftime("%Y-%m-%d")
        )
        # click Download
        self._driver.find_element_by_css_selector("#btngbDataDownload").click()


class HomePage:
    """Represents the SDGE MyAccount homepage, which appears post login."""

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""

        close_modal(self._driver)
        self._driver.wait(10).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, ".mdl-spinner"))
        )
        self._driver.wait(120).until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, 'input[id="usernamex"]')
            )
        )


class LoginPage:
    """Represents the authentication page in the web UI."""

    # Nothing fancy here. There is a username field, password field,
    # and login button
    UsernameFieldCss = 'input[id="usernamex"]'
    PasswordFieldCss = 'input[id="passwordx"]'
    LoginButtonCss = 'button[id="btnlogin"]'
    SaveUsernameCss = 'input[id="rmbrmer"]'
    FailedLoginSelector = ".toast-error"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        """Wait until the page is ready to interact with."""
        log.info("Waiting for Login page to be ready...")
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
        self._driver.sleep(1)
        self.get_login_button().click()
        self._driver.sleep(5)
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

    In particular, wait for a csv file to show up in the download directory.
    """
    wait = WebDriverWait(driver, timeout)
    download_dir = driver.download_dir

    filename = wait.until(file_exists_in_dir(download_dir, r".*\.csv$"))
    filepath = os.path.join(download_dir, filename)
    return filepath


def extract_csv_rows(download_path):
    """Pull CSV data from the downloaded csv file."""
    with open(download_path) as f:
        csv_reader = csv.reader(f)
        seen_header = False
        for row in csv_reader:
            if len(row) == 2:
                log.info("CSV Metadata: {0} = {1}".format(row[0], row[1]))
            elif len(row) == 7:
                if not seen_header:
                    seen_header = True
                else:
                    yield CsvRow._make(row)


def to_raw_reading(csv_row, direction: str, adjustment_factor: int = 1):
    """Convert a CSV row to an interval reading. Multiply be an adjustment factor if needed.

    Set the adjustment_factor to 4 to convert 15 minute values to kWh
    """
    reading_date = dateparser.parse(csv_row.Date).date()
    if csv_row.StartTime:
        reading_time = dateparser.parse(csv_row.StartTime).time()
    else:
        # for daily meters, the StartTime field is blank
        reading_time = time(0, 0)
    if direction == "forward":
        value = float(csv_row.Consumption)
    else:
        value = float(csv_row.Generation)
    # For 15 minute electric meters, multiply the KWH value by 4 to get KW
    return RawReading(
        date=reading_date, time=reading_time, value=value * adjustment_factor
    )


def parse_xlsx(timeline: Timeline, filename: str, adjustment_factor: int = 1):
    log.info(f"parsing {filename}")
    df = pd.read_excel(filename, engine="openpyxl")
    # Meter Number	Date	Start Time	Duration	Consumed
    seen_header = False
    for index, row in df.iterrows():
        if row[0] == "Meter Number":
            seen_header = True
            log.info(f"header={' '.join(row)}")
            continue
        if not seen_header:
            continue
        if adjustment_factor == 1:  # daily
            dt = dateparser.parse(row[1])
        else:
            # remove extra : from times like 12:00:am
            time_str = row[2].replace(":am", " am").replace(":pm", " pm")
            dt = dateparser.parse(f"{row[1]} {time_str}")
        kw = float(row[4]) * adjustment_factor
        val = timeline.lookup(dt)
        if val:
            timeline.insert(dt, (kw + val) / 2)
        else:
            timeline.insert(dt, kw)


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
    if len(readings) == 1:
        return readings
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

    @property
    def adjustment_factor(self):
        return self._configuration.adjustment_factor

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
        usage_page = UsagePage(self._driver)

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

        # Go to the 'Usage' Page
        log.info("Navigating to 'Usage' page.")
        usage_page.navigate_to_usage_page()
        usage_page.wait_until_ready()
        self.screenshot("usage_page_initial")

        if usage_page.is_enterprise():
            log.info("Enterprise: selecting account: {0}".format(self.account_id))
            if not usage_page.select_account_from_portfolio(self.account_id):
                error_msg = f"Unable to find account with ID={self.account_id}"
                log.info(error_msg)
                raise InvalidAccountException(error_msg)
            self.screenshot("usage_account_selected")

        else:
            log.info("Selecting account: {0}".format(self.account_id))
            if not usage_page.select_account(self.account_id):
                available_accounts = set(usage_page.get_available_accounts())
                error_msg = "Unable to find account with ID={0}. Available accounts are: {1}".format(
                    self.account_id, available_accounts
                )
                log.info(error_msg)
                raise InvalidAccountException(error_msg)

            self.screenshot("usage_account_selected")

        # Select the desired meter
        log.info("Selecting meter with id: {0}".format(self.service_id))
        if not usage_page.select_meter(self.service_id):
            available_meters = set(usage_page.get_available_meters())
            error_msg = (
                "Unable to find meter with ID={0}. Available meters are: {1}".format(
                    self.service_id, available_meters
                )
            )
            raise InvalidMeterException(error_msg)
        self.screenshot("selected meter")

        date_range = DateRange(self.start_date, self.end_date)
        timeline = Timeline(
            self.start_date, self.end_date, interval=self._configuration.interval
        )

        if usage_page.is_enterprise():
            usage_page.enterprise_select_usage(self._configuration.interval)
            if self._configuration.interval == 14440:
                for subrange in date_range.split_iter(delta=relativedelta(days=7)):
                    usage_page.enterprise_download(
                        subrange.start_date,
                        subrange.end_date,
                        self._configuration.interval,
                    )
            else:
                dt = self.start_date
                while dt < self.end_date:
                    usage_page.enterprise_download(dt, dt, self._configuration.interval)
                    dt += timedelta(days=1)
            for filename in glob(f"{self._driver.download_dir}/*.xlsx"):
                parse_xlsx(timeline, filename, self.adjustment_factor)
        else:
            usage_page.open_green_button()
            self.screenshot("opened green button")

            # This page only allows you to download a certain amount of
            # billing data at a time. We will use a conservative chunk
            # size of 180 days.
            interval_size = relativedelta(days=180)
            for subrange in date_range.split_iter(delta=interval_size):
                log.info("Getting interval data for date range: {0}".format(subrange))
                start = subrange.start_date
                end = subrange.end_date

                # Set the date range in the UI, then click "Export"
                log.info("Setting date range.")
                usage_page.download(start, end)
                download_path = wait_for_download(self._driver)

                log.info("Processing downloaded file: {0}".format(download_path))
                # ...then process the downloaded file.
                for row in extract_csv_rows(download_path):
                    raw_reading = to_raw_reading(
                        row, self.direction, self.adjustment_factor
                    )
                    dt = datetime.combine(raw_reading.date, raw_reading.time)
                    val = timeline.lookup(dt)
                    if val:
                        timeline.insert(dt, (raw_reading.value + val) / 2)
                    else:
                        timeline.insert(dt, raw_reading.value)

                # rename to keep files in archive, but prevent matching on filename
                os.rename(download_path, f"{download_path}.processed")

        return Results(readings=timeline.serialize())


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
        meter.utility_account_id,
        meter.service_id,
        meter.direction,
        meter.interval,
        meter.commodity,
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
