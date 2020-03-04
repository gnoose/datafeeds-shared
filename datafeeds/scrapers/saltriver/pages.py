"""Page object definitions for Salt River Project scrapers"""

from datetime import date
from typing import List, NamedTuple

from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support.select import Select
from dateutil.parser import parse as parse_date

from datafeeds.common.util.selenium import (
    ec_and,
    ec_in_frame,
    window_count_equals,
    IFrameSwitch,
    WindowSwitch,
)
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.common.exceptions import LoginError
import datafeeds.scrapers.saltriver.errors as saltriver_errors

ChannelInfo = NamedTuple(
    # Represents basic information about an individual channel for a meter
    "ChannelInfo",
    [("id", str), ("units", str), ("data_start", date), ("data_end", date)],
)

MeterInfo = NamedTuple(
    # Represents basic meter metadata, based on what's available on the "meter profile" page within SPATIA
    "MeterInfo",
    [
        ("name", str),
        ("account", str),
        ("meter_id", str),
        ("address", str),
        ("meter_number", str),
        ("iph", str),
        ("channels", List[ChannelInfo]),
    ],
)

BillSummaryRow = NamedTuple(
    # Represents a table row drawn from a SPATIA billing summary
    "BillSummaryRow",
    [
        ("detail_link", WebElement),
        (
            "stop_date",
            date,
        ),  # The billing summary table only has a stop date for the period
        ("rate", str),
        ("max_kw", float),
        ("total_kwh", float),
        ("cost", float),
    ],
)

BillDetail = NamedTuple(
    "BillDetail",
    # Represents more detailed billing data, obtained by clicking on the "detail_link" of a bill summary row.
    [
        ("account", str),
        ("rate", str),
        ("bill_start", date),
        ("bill_stop", date),
        ("total_kwh", float),
        ("on_peak_kw", float),
        ("cost", float),
    ],
)


class SaltRiverLoginPage(PageState):
    UsernameInputLocator = (By.XPATH, "//input[@id='Text1']")
    PasswordInputLocator = (By.XPATH, "//input[@id='Password1']")
    SubmitButtonLocator = (By.XPATH, "//input[@id='Submit1']")

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.UsernameInputLocator),
            EC.presence_of_element_located(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
        )

    def login(self, username: str, password: str):
        username_field = self.driver.find_element(*self.UsernameInputLocator)
        username_field.send_keys(username)

        password_field = self.driver.find_element(*self.PasswordInputLocator)
        password_field.send_keys(password)

        submit_button = self.driver.find_element(*self.SubmitButtonLocator)
        submit_button.click()


class SaltRiverLoginFailedPage(PageState):
    UsernameInputLocator = (By.XPATH, "//input[@id='Text1']")
    PasswordInputLocator = (By.XPATH, "//input[@id='Password1']")
    SubmitButtonLocator = (By.XPATH, "//input[@id='Submit1']")
    ErrorMessageLocator = (By.XPATH, "//strong//font[@color='red']")

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.UsernameInputLocator),
            EC.presence_of_element_located(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
            EC.presence_of_element_located(self.ErrorMessageLocator),
        )

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        error = self.driver.find_element(*self.ErrorMessageLocator)
        message = "Login failed. The website error is: '{}'".format(error.text)
        raise LoginError(message)


class SaltRiverLandingPage(PageState):
    ContentLocator = (By.XPATH, "//div[@id='mainbody']//div[@id='maincolumn']")

    def get_ready_condition(self):
        return ec_and(
            EC.title_contains("SPATIA"),
            EC.presence_of_element_located(self.ContentLocator),
        )


class MeterProfilesPage(PageState):
    """This page lists information about the meters available for the current SPATIA login"""

    ProfileTableLocator = (By.XPATH, "//p//table")
    HeaderLocator = (By.XPATH, "//h1[contains(text(), 'All Meters Report')]")
    ReportPageLink = (By.PARTIAL_LINK_TEXT, "Billing Reports")

    def get_ready_condition(self):
        return ec_in_frame(
            "mainFrame",
            ec_and(
                EC.presence_of_element_located(self.ProfileTableLocator),
                EC.presence_of_element_located(self.HeaderLocator),
                EC.element_to_be_clickable(self.ReportPageLink),
            ),
        )

    def get_meters(self) -> List[MeterInfo]:
        """Scrape the meter data from this page into a list of MeterInfo objects"""
        results = []
        with IFrameSwitch(self.driver, "mainFrame"):
            table = self.driver.find_element(*self.ProfileTableLocator)
            current_meter = None
            current_stop_date = None

            for row in table.find_elements_by_tag_name("tr"):
                # This table has two kinds of rows: "primary" rows, which describe a meter and its first channel;
                # and supplementary "channel" rows, which describe additional channels for a given meter. The
                # primary row will appear first, followed by a channel row for each channel beyond the first.
                # Therefore, we keep track of the "current" meter, so that when we come across a channel row we
                # known which meter to associate it with.

                cells = row.find_elements_by_tag_name("td")
                if len(cells) >= 9:
                    # Each meter channel reports the dates for which data is available (start and end date). However,
                    # in all observed cases the end date is specified once, on the primary row for the meter (opposed
                    # to the start date, which appears for each channel). However, in case its possible for a channel
                    # to report a different stop date, we still check for one, even if we are sitting on a channel row.
                    if len(cells) == 10:
                        current_stop_date = parse_date(cells[9].text.strip()).date()

                    meter_name = cells[0].text.strip()
                    if meter_name:
                        # This indicates we have hit a new "primary" row, and thus need to emit a record for the
                        # previous active meter, if there is one.
                        if current_meter:
                            results.append(current_meter)

                        current_meter = MeterInfo(
                            name=cells[0].text.strip(),
                            account=cells[1].text.strip(),
                            meter_id=cells[2].text.strip(),
                            address=cells[3].text.strip(),
                            meter_number=cells[4].text.strip(),
                            iph=cells[5].text.strip(),
                            channels=[],
                        )

                    if current_meter:
                        current_meter.channels.append(
                            ChannelInfo(
                                id=cells[6].text.strip(),
                                units=cells[7].text.strip(),
                                data_start=parse_date(cells[8].text.strip()).date(),
                                data_end=current_stop_date,
                            )
                        )

            if current_meter:
                results.append(current_meter)

        return results

    def goto_reports(self):
        with IFrameSwitch(self.driver, "banner"):
            self.driver.find_element(*self.ReportPageLink).click()


class BillHistoryConfigPage(PageState):
    """This page is used to configure and generate a billing history report."""

    HeaderLocator = (By.XPATH, "//h1//b[contains(text(), 'Bills By Account Report')]")
    NextButtonLocator = (By.XPATH, "//input[@type='submit']")
    MeterSelectLocator = (By.XPATH, "//select[@name='ID']")
    TimeButtonSelector = (By.XPATH, "//input[@name='Interval']")
    ReportPageLink = (By.PARTIAL_LINK_TEXT, "Billing Reports")

    def get_ready_condition(self):
        return ec_in_frame(
            "mainFrame",
            ec_and(
                EC.presence_of_element_located(self.HeaderLocator),
                EC.presence_of_element_located(self.NextButtonLocator),
                EC.presence_of_element_located(self.MeterSelectLocator),
                EC.presence_of_element_located(self.TimeButtonSelector),
            ),
        )

    def select_longest_report(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            selectors = self.driver.find_elements(*self.TimeButtonSelector)
            longest = max(selectors, key=lambda s: int(s.get_attribute("value")))
            longest.click()

    def select_account(self, account_id):
        with IFrameSwitch(self.driver, "mainFrame"):
            selector = Select(self.driver.find_element(*self.MeterSelectLocator))

            try:
                selector.select_by_value(account_id)
            except NoSuchElementException:
                raise saltriver_errors.MeterNotFoundError.for_account(account_id)

    def generate_report(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.NextButtonLocator).click()


class BillHistoryResultsPage(PageState):
    """This page appears after generating a billing history report (from BillHistoryConfigPage)

    The page contains a table summarizing bills over some time period. Each table row contains a link pointing to
    more detailed information for the associated period.
    """

    HeaderLocator = (By.XPATH, "//h2[contains(text(), 'Monthly Billing Report')]")
    BillTableLocator = (By.XPATH, "//p//table[2]")

    def get_ready_condition(self):
        return ec_in_frame(
            "mainFrame",
            ec_and(
                EC.presence_of_element_located(self.HeaderLocator),
                EC.presence_of_element_located(self.BillTableLocator),
            ),
        )

    @staticmethod
    def parse_float(str):
        return float(str.replace(",", ""))

    @staticmethod
    def parse_cost(str):
        replacements = ",$()"
        for r in replacements:
            str = str.replace(r, "")
        return float(str)

    def get_bill_summaries(self):
        results = []
        with IFrameSwitch(self.driver, "mainFrame"):
            table = self.driver.find_element(*self.BillTableLocator)
            for row in table.find_elements_by_tag_name("tr"):
                cells = row.find_elements_by_tag_name("td")
                if len(cells) == 10 and cells[0].text.strip() != "Total":
                    results.append(
                        BillSummaryRow(
                            detail_link=cells[0].find_element_by_tag_name("a"),
                            stop_date=parse_date(cells[1].text.strip()).date(),
                            rate=cells[2].text.strip(),
                            max_kw=self.parse_float(cells[3].text.strip()),
                            total_kwh=self.parse_float(cells[4].text.strip()),
                            cost=self.parse_cost(cells[9].text.strip()),
                        )
                    )
        return results

    def get_bill_details(self, bill_row: BillSummaryRow):
        """Click on the 'details' link for a given bill summary, and scrape the resulting page"""
        with IFrameSwitch(self.driver, "mainFrame"):
            # We open the page in a new window by shift-clicking.
            actions = ActionChains(self.driver)
            actions.move_to_element(bill_row.detail_link)
            actions.key_down(Keys.SHIFT)
            actions.click(bill_row.detail_link)
            actions.key_up(Keys.SHIFT)
            actions.perform()

        self.driver.wait().until(window_count_equals(2))
        other_window = self.driver.window_handles[1]
        detail_raw = {}
        with WindowSwitch(self.driver, other_window, close=True):
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//table"))
            )
            table = self.driver.find_element_by_tag_name("table")
            for row in table.find_elements_by_tag_name("tr"):
                cells = row.find_elements_by_tag_name("td")
                if len(cells) >= 2:
                    row_label = cells[0].text.strip()
                    detail_raw[row_label] = cells[1].text.strip()

        return BillDetail(
            account=str(detail_raw["Account"]),
            rate=str(detail_raw["Rate"]),
            bill_start=parse_date(detail_raw["Bill Start"]).date(),
            bill_stop=parse_date(detail_raw["Bill Stop"]).date(),
            total_kwh=self.parse_float(detail_raw["Total kWh"]),
            on_peak_kw=self.parse_float(detail_raw["On Peak KW"]),
            cost=self.parse_cost(detail_raw["Total Bill"]),
        )


class IntervalDownloadPage(PageState):
    """This page is used to download interval data for a given time range, into a csv file"""

    HeaderLocator = (By.XPATH, "//h1[contains(text(), 'Interval Download')]")
    StartDateLocator = (By.XPATH, "//input[@name='StartDate']")
    StopDateLocator = (By.XPATH, "//input[@name='StopDate']")
    FifteenMinuteOptionLocator = (By.XPATH, "//input[@name='Interval' and @value='15']")
    DemandDatatypeLocator = (By.XPATH, "//input[@name='datatype' and @value='0']")
    MeterSelectLocator = (By.XPATH, "//select[@name='ID']")
    SubmitSelector = (By.XPATH, "//input[@type='submit']")

    def get_ready_condition(self):
        return ec_in_frame(
            "mainFrame",
            ec_and(
                EC.presence_of_element_located(self.HeaderLocator),
                EC.presence_of_element_located(self.StartDateLocator),
                EC.presence_of_element_located(self.StopDateLocator),
                EC.element_to_be_clickable(self.FifteenMinuteOptionLocator),
                EC.element_to_be_clickable(self.DemandDatatypeLocator),
                EC.presence_of_element_located(self.MeterSelectLocator),
            ),
        )

    def basic_configuration(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.FifteenMinuteOptionLocator).click()
            self.driver.find_element(*self.DemandDatatypeLocator).click()

    def select_meter_by_id(self, meter_id: str):
        with IFrameSwitch(self.driver, "mainFrame"):
            selector = Select(self.driver.find_element(*self.MeterSelectLocator))
            selector.select_by_value(meter_id)

    def set_date_range(self, start: date, end: date):
        date_format = "%m/%d/%Y"
        with IFrameSwitch(self.driver, "mainFrame"):
            start_elem = self.driver.find_element(*self.StartDateLocator)
            start_elem.clear()
            start_elem.send_keys(start.strftime(date_format))

            end_elem = self.driver.find_element(*self.StopDateLocator)
            end_elem.clear()
            end_elem.send_keys(end.strftime(date_format))

    def download_interval_data(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.SubmitSelector).click()


class SaltRiverReportsPage(PageState):
    """This page contains links for navigating to various report types."""

    BillsByAccountLink = (By.LINK_TEXT, "Bills by account:")
    MeterProfilesLink = (By.LINK_TEXT, "All meters profiles:")
    IntervalDownloadLink = (By.LINK_TEXT, "Interval download:")

    def get_ready_condition(self):
        return ec_in_frame(
            "mainFrame",
            ec_and(
                EC.presence_of_element_located(self.BillsByAccountLink),
                EC.presence_of_element_located(self.MeterProfilesLink),
                EC.element_to_be_clickable(self.IntervalDownloadLink),
            ),
        )

    def goto_bill_history(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.BillsByAccountLink).click()

    def goto_meter_profiles(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.MeterProfilesLink).click()

    def goto_interval_download(self):
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.IntervalDownloadLink).click()
