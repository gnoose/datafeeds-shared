"""Page object definitions for Salt River Project scrapers"""

import logging
from datetime import date, timedelta
from io import BytesIO
from typing import List, NamedTuple

from dateutil import parser as date_parser
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support.select import Select
from dateutil.parser import parse as parse_date

from datafeeds import config
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.typing import BillingDatum
from datafeeds.common.upload import hash_bill_datum, upload_bill_to_s3
from datafeeds.common.util.selenium import (
    ec_and,
    ec_in_frame,
    scroll_to,
    window_count_equals,
    IFrameSwitch,
    WindowSwitch,
    file_exists_in_dir,
)
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.common.exceptions import LoginError
import datafeeds.scrapers.saltriver.errors as saltriver_errors

log = logging.getLogger(__name__)

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
    UsernameInputLocator = (By.XPATH, "//input[@name='username']")
    PasswordInputLocator = (By.XPATH, "//input[@name='password']")
    SubmitButtonLocator = (By.XPATH, "//button[@type='submit']")

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.UsernameInputLocator),
            EC.presence_of_element_located(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
        )

    def login(self, username: str, password: str):
        actions = ActionChains(self.driver)
        actions.send_keys(username)
        actions.send_keys(Keys.TAB)
        actions.send_keys(password)
        actions.send_keys(Keys.ENTER)
        actions.perform()
        self.driver.screenshot(BaseWebScraper.screenshot_path("login"))


class SpatiaLoginPage(PageState):
    UsernameInputLocator = (By.XPATH, "//input[@name='user']")
    PasswordInputLocator = (By.XPATH, "//input[@name='password']")
    SubmitButtonLocator = (By.XPATH, "//input[@type='submit']")

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.UsernameInputLocator),
            EC.presence_of_element_located(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
        )

    def login(self, username: str, password: str):
        actions = ActionChains(self.driver)
        actions.send_keys(username)
        actions.send_keys(Keys.TAB)
        actions.send_keys(password)
        actions.send_keys(Keys.ENTER)
        actions.perform()
        self.driver.screenshot(BaseWebScraper.screenshot_path("login"))


class SaltRiverLoginFailedPage(PageState):
    ErrorMessageLocator = (By.CSS_SELECTOR, "div.srp-alert-error.mb-2")

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.ErrorMessageLocator)

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        error = self.driver.find_element(*self.ErrorMessageLocator)
        message = "Login failed. The website error is: '{}'".format(error.text)
        raise LoginError(message)


class SpatiaLandingPage(PageState):
    LinkLocator = (By.XPATH, '//a[text()="Billing & Special Reports"]')

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.LinkLocator)

    def select_billing(self):
        self.driver.find_element(self.LinkLocator).click()


class SaltRiverLandingPage(PageState):
    AccountDropdownLocator = (By.XPATH, "//div[@id='rw_1_input']")
    AccountDropdownButtonLocator = (By.XPATH, "//button[@title='open dropdown']")
    AccountOptionLocator = (By.XPATH, "//li[@class='rw-list-option']")
    AccountActiveOptionLocator = (By.XPATH, "//li[@id='rw_1_listbox_active_option']")
    MyBillButtonLocator = (By.XPATH, "//a[contains(text(), 'My bill')]")
    NoBillsDisplayedLocator = (By.XPATH, "//div[contains(text(), '12')]")
    NoBillsDisplayedOptionLocator = (
        By.XPATH,
        "//div[@id='menu-']" "/div" "/ul" "/li[contains(text(), '36')]",
    )
    UsageRadioButtonLocator = (By.XPATH, "//input[@type='radio' and @value='usage']")
    UsageTableBodyLocator = (By.XPATH, "//*[@id='usagetable']/tbody")
    UsageTableRowsLocator = (By.XPATH, "//*[@id='usagetable']/tbody/tr")

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.AccountDropdownLocator)

    def select_account(self, account_id: str):
        """Select account from dropdown.

        self.account_id does not have dashes (805555003) but option does (805-555-003)
        In case account_id is in wrong format, strip for digits, and re-format
        """
        account_id = "".join(c for c in account_id if c.isdigit())
        account_id = "{}-{}-{}".format(account_id[:3], account_id[3:6], account_id[6:9])
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.AccountDropdownButtonLocator)
        )

        account_dropdown_button = self.driver.find_element(
            *self.AccountDropdownButtonLocator
        )
        account_dropdown_button.click()
        WebDriverWait(self.driver, 10).until(
            ec_and(
                EC.presence_of_element_located(self.AccountOptionLocator),
                EC.presence_of_element_located(self.AccountActiveOptionLocator),
            )
        )

        account_options = self.driver.find_elements(*self.AccountOptionLocator)
        account_options.append(
            self.driver.find_element(*self.AccountActiveOptionLocator)
        )
        found = False
        try:
            for option in account_options:
                if account_id in option.text:
                    option.click()
                    found = True
        except StaleElementReferenceException:
            pass

        if not found:
            raise saltriver_errors.MeterNotFoundError.for_account(account_id)

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.MyBillButtonLocator)
        )
        my_bill_button = self.driver.find_element(*self.MyBillButtonLocator)
        self.driver.execute_script("arguments[0].click()", my_bill_button)

    def set_displayed_bills(self):
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.NoBillsDisplayedLocator)
        )
        no_bills_displayed_select = self.driver.find_element(
            *self.NoBillsDisplayedLocator
        )
        scroll_to(self.driver, no_bills_displayed_select)
        no_bills_displayed_select.click()

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.NoBillsDisplayedOptionLocator)
        )
        no_bills_displayed_option = self.driver.find_element(
            *self.NoBillsDisplayedOptionLocator
        )
        no_bills_displayed_option.click()

    def set_history_type(self):
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.UsageRadioButtonLocator)
        )
        usage_radio_button = self.driver.find_element(*self.UsageRadioButtonLocator)
        scroll_to(self.driver, usage_radio_button)
        usage_radio_button.click()

    def _overlap(self, start: date, end: date, bill_start: date, bill_end: date):
        c_start = max(start, bill_start)
        c_end = min(end, bill_end)
        return max(c_end - c_start, timedelta())

    def get_bills(self, account_id: str, start: date, end: date) -> List[BillingDatum]:
        """Get bills from the table.

        for each row:
          get end from Read date column (date)
          get start date from end date - (Days column (date) - 1)
          get statement date from Bill date column (date)
          if not start - end overlaps passed in start / end, continue
          get peak from On-peak Billed kW (float)
          get used from (Off-peak kWh + Shoulder kWh + On-peak kWh) (float)
          get cost from New charges (float)
          click eye icon to download PDF; wait for download to complete to self.driver.download_dir
        """
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.UsageTableBodyLocator)
        )
        usage_table_rows = self.driver.find_elements(*self.UsageTableRowsLocator)

        bill_data: List[BillingDatum] = []
        self.driver.screenshot(BaseWebScraper.screenshot_path("bill table"))
        for row in usage_table_rows:
            cols = row.find_elements_by_tag_name("td")
            cols = [c for c in cols if "display: none" not in c.get_attribute("style")]

            col = lambda x: cols[x].text
            to_num = lambda x: "".join(d for d in col(x) if d.isdigit() or d == ".")
            to_float = lambda x: float(to_num(x)) if len(to_num(x)) > 0 else 0

            log.debug(f"statement={col(1)} end={col(2)} days={col(7)}")
            # statement date
            statement_date = date_parser.parse(col(1)).date()

            # bill end
            period_year = statement_date.year
            if statement_date.month == 1 and col(2).startswith("12"):
                period_year = statement_date.year - 1
            end_str = f"{col(2)}/{period_year}"
            bill_end = date_parser.parse(end_str).date()

            # bill start
            bill_start = bill_end - timedelta(days=int(to_float(7)) - 1)
            log.debug(f"start={bill_start} end={bill_end}")

            if not self._overlap(start, end, bill_start, bill_end):
                log.info(
                    f"skipping bill {bill_start} - {bill_end}: does not overlap requested range {start} - {end}"
                )
                continue

            # cost
            new_charges = to_float(8)
            # used
            used = to_float(4) + to_float(5) + to_float(6)
            # peak
            peak = to_float(3)

            bill_datum = BillingDatum(
                start=bill_start,
                end=bill_end,
                statement=statement_date,
                cost=new_charges,
                used=used,
                peak=peak,
                items=None,
                attachments=None,
                utility_code=None,
            )

            try:
                bill_pdf_name = "SRPbill{}{}".format(
                    statement_date.strftime("%B"), statement_date.year
                )
                pdf_download_link = cols[0].find_element_by_tag_name("a")
                scroll_to(self.driver, pdf_download_link)
                pdf_download_link.click()
                log.info(
                    "looking for %s in %s", bill_pdf_name, self.driver.download_dir
                )
                self.driver.wait(60).until(
                    file_exists_in_dir(self.driver.download_dir, bill_pdf_name)
                )
            except Exception as e:
                raise Exception(
                    f"Failed to download bill {bill_pdf_name} for statement date {statement_date}:\n {e}"
                )
            log.info(
                f"Bill {bill_pdf_name} for statement date {statement_date} downloaded successfully"
            )

            attachment_entry = None
            # open downloaded PDF and upload
            if config.enabled("S3_BILL_UPLOAD"):
                key = hash_bill_datum(account_id, bill_datum)
                with open(
                    f"{self.driver.download_dir}/{bill_pdf_name}", "rb"
                ) as pdf_data:
                    attachment_entry = upload_bill_to_s3(
                        BytesIO(pdf_data.read()),
                        key,
                        source="myaccount.srpnet.com",
                        statement=bill_datum.statement,
                        utility="utility:salt-river-project",
                        utility_account_id=account_id,
                    )
            if attachment_entry:
                bill_data.append(bill_datum._replace(attachments=[attachment_entry]))
            else:
                bill_data.append(bill_datum)
        return bill_data


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
        log.info("interval download page ready condition")
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
        log.info("basic_configuration")
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.FifteenMinuteOptionLocator).click()
            self.driver.find_element(*self.DemandDatatypeLocator).click()

    def select_meter_by_id(self, meter_id: str):
        log.info("select meter by id")
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
            log.info("clicking submit")
            self.driver.find_element(*self.SubmitSelector).click()


class SaltRiverReportsPage(PageState):
    """This page contains links for navigating to various report types."""

    BillsByAccountLink = (By.LINK_TEXT, "Bills by account")
    MeterProfilesLink = (By.LINK_TEXT, "All meters profiles:")
    IntervalDownloadLink = (By.LINK_TEXT, "Interval download:")

    def get_ready_condition(self):
        return ec_in_frame(
            "mainFrame",
            ec_and(
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
        log.info("click interval download")
        with IFrameSwitch(self.driver, "mainFrame"):
            self.driver.find_element(*self.IntervalDownloadLink).click()
