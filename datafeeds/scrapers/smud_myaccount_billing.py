import time
import logging
import traceback

import requests

from io import BytesIO
from typing import Optional, NamedTuple, List
from collections import namedtuple
from datetime import date, datetime, timedelta
from dateutil.parser import parse as parse_date

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select

from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, BillingDatum


from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.exceptions import LoginError
from datafeeds.common.util.pagestate.pagestate import PageStateMachine, PageState
from datafeeds.common.util.selenium import ec_and
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
import datafeeds.common.upload as bill_upload

log = logging.getLogger(__name__)


# Represents metadata about a bill pdf download from a website table
BillPdfInfo = namedtuple("BillPdfInfo", ["issue_date", "due_date", "link"])

BillPeriodSelector = NamedTuple(
    "BillPeriodSelector", [("value", str), ("start", date), ("end", date)]
)

BillPeriodDetails = NamedTuple(
    "BillPeriodDetails",
    [
        ("start", date),
        ("end", date),
        ("total_electric_charges", float),
        ("total_charges", float),
        ("total_kwh", float),
        ("max_kw", float),
        ("download_link", str),
    ],
)


class InvalidTimeRangeException(Exception):
    pass


class UnexpectedBillDataException(Exception):
    pass


class SMUDMyAccountBillingConfiguration(Configuration):
    def __init__(
        self, account_id: str,
    ):
        super().__init__(scrape_bills=True)
        self.account_id = account_id


class SmudMyAccountLoginPage(PageState):
    """Represents the lgin page in the web UI."""

    UsernameFieldId = "UserId"
    PasswordFieldId = "Password"
    SignInForm = "sign-in-form"
    SignInButton = '//button[@type="submit"]'

    def get_ready_condition(self):
        return EC.presence_of_element_located((By.ID, self.UsernameFieldId))

    def login(self, username, password):
        """Authenticate with the webpage.

        Fill in the username, password, then click "continue"
        """
        log.info("Inserting credentials on login page.")
        self.driver.find_element_by_id(self.UsernameFieldId).send_keys(username)
        self.driver.find_element_by_id(self.PasswordFieldId).send_keys(password)
        self.driver.find_element_by_id(self.SignInForm).find_element_by_xpath(
            self.SignInButton
        ).click()


class SmudMyAccountFailedLoginPage(PageState):
    """Represents the login page after a failed login."""

    LoginErrorsSelector = "div.validation-summary-errors"

    def get_ready_condition(self):
        return EC.presence_of_element_located(
            (By.CSS_SELECTOR, self.LoginErrorsSelector)
        )

    def get_login_errors(self):
        validation_errors = self.driver.find_element_by_css_selector(
            self.LoginErrorsSelector
        )
        return [item.text for item in validation_errors.find_elements_by_tag_name("li")]

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        login_errors = self.get_login_errors()
        error_text = ""
        if login_errors:
            error_text = ", ".join(login_errors)
        message = "Login failed. The website errors are: '{}'".format(error_text)
        raise LoginError(message)


class SmudChooseAccountPage(PageState):
    """Represents the lgin page in the web UI."""

    PageContentLocator = (By.XPATH, "div#")
    AccountDivSelector = "div#accounts-available div"
    AccountRowsSelector = "div#accounts-available div.card-row"
    ShowMoreLinkSelector = "div.show-more-link a"

    def get_ready_condition(self):
        return EC.presence_of_element_located(
            (By.CSS_SELECTOR, self.AccountDivSelector)
        )

    def expand_accounts(self):
        done = False
        while not done:
            result = self.driver.find_elements_by_css_selector(
                self.ShowMoreLinkSelector
            )
            if result:
                link = result[0]
                if link.is_displayed():
                    link.click()
                    time.sleep(2)
                else:
                    done = True
            else:
                done = True

    def select_account(self, target):
        for available_account in self.driver.find_elements_by_css_selector(
            self.AccountRowsSelector
        ):
            account_number = available_account.get_attribute("data-account-number")
            if account_number == target:
                available_account.click()
                return


class SmudMyAccountOverviewPage(PageState):
    """Represents the SMUD overview page, which appears after login"""

    PageContentLocator = (By.XPATH, "//div[@id='page-content']")

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.PageContentLocator)


class SmudBillComparePage(PageState):
    """Represents the SMUD overview page, which appears after login"""

    Bill1Locator = (By.XPATH, "//select[@id='BillingPeriod1_Value']")
    Bill2Locator = (By.XPATH, "//select[@id='BillingPeriod1_Value']")
    UsageDetailExpanderLocator = (By.XPATH, "//span[@data-key='TotalKWHUsed']")

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.Bill1Locator),
            EC.presence_of_element_located(self.Bill2Locator),
            EC.presence_of_element_located(self.UsageDetailExpanderLocator),
        )

    def get_all_billing_periods(self) -> List[BillPeriodSelector]:
        """Return all available bill periods"""
        selector = Select(self.driver.find_element(*self.Bill1Locator))
        results = []
        for option in selector.options:
            text = option.get_attribute("textContent").strip()
            parts = text.split("-")
            start = parse_date(parts[0]).date()
            end = parse_date(parts[1]).date()

            results.append(
                BillPeriodSelector(
                    value=option.get_attribute("value"), start=start, end=end
                )
            )
        return results

    @staticmethod
    def _parse_cost_string(s: str) -> float:
        return float(s.replace("$", "").replace(",", ""))

    @staticmethod
    def _parse_usage_string(s: str) -> Optional[float]:
        if not s:
            return None

        return float(s.replace(",", ""))

    def _parse_raw_bill_data(self, raw_data: dict) -> BillPeriodDetails:
        date_strings = raw_data.get("bill categories", "").split("-")
        return BillPeriodDetails(
            start=parse_date(date_strings[0]).date(),
            end=parse_date(date_strings[1]).date(),
            total_electric_charges=self._parse_cost_string(
                raw_data.get("total electrical charges")
            ),
            total_charges=self._parse_cost_string(
                raw_data.get("total electric service charges/credits")
            ),
            total_kwh=self._parse_usage_string(raw_data.get("total kwh used")),
            max_kw=self._parse_usage_string(raw_data.get("maximum kw")),
            download_link=raw_data.get("link"),
        )

    def get_visible_bill_details(self) -> List[BillPeriodDetails]:
        """Retrieve billing details for the currently visible bills"""
        billing_row_divs = self.driver.find_elements(
            By.XPATH, "//div[@id='compare-container']//div[contains(@class, 'row')]"
        )

        # This view shows bills in pairs, so we expect to parse out two bill detail objects
        bill_1_raw_values = {}
        bill_2_raw_values = {}
        for billing_row in billing_row_divs:
            billing_columns = billing_row.find_elements(By.XPATH, "./div")
            billing_links = billing_row.find_elements(By.PARTIAL_LINK_TEXT, "View Bill")
            if billing_links:
                try:
                    bill_1_raw_values["Link"] = (
                        billing_columns[1]
                        .find_element_by_tag_name("a")
                        .get_attribute("href")
                    )
                except Exception:
                    pass
                try:
                    bill_2_raw_values["Link"] = (
                        billing_columns[2]
                        .find_element_by_tag_name("a")
                        .get_attribute("href")
                    )
                except Exception:
                    pass
            else:
                column_texts = [col.text.strip() for col in billing_columns]
                if len(column_texts) == 4:
                    label = column_texts[0].lower()
                    if label:
                        bill_1_raw_values[label] = column_texts[1]
                        bill_2_raw_values[label] = column_texts[2]

        results = []
        for raw_bill in [bill_1_raw_values, bill_2_raw_values]:
            try:
                results.append(self._parse_raw_bill_data(raw_bill))
            except Exception as e:
                msg = "Failed to parse billing data from site. Raw values: {}".format(
                    raw_bill
                )
                traceback.print_exc()
                raise UnexpectedBillDataException(msg) from e

        return results

    def select_bill(self, bill_period: BillPeriodSelector):
        locator = "//option[@value='{}']".format(bill_period.value)
        self.driver.find_element_by_xpath(locator).click()

    def toggle_usage_details(self):
        self.driver.find_element(*self.UsageDetailExpanderLocator).click()
        time.sleep(5)


class SMUDMyAccountBillingScraper(BaseWebScraper):
    """Downloads bill data from the SMUD MyAccount webpage

    Fetching the raw billing data is fairly straightforward; there is an Excel file downloadable from the
    landing page for a given account, which contains all of the information we need.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SMUD MyAccount Billing Scraper"
        self.bill_history = []

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def download_pdfs(self):
        """Returns True if bill PDFS should be downloaded, False otherwise"""
        return self._configuration.download_pdfs

    @property
    def billing_start(self) -> date:
        return self._configuration.billing_start

    @property
    def billing_end(self) -> date:
        return self._configuration.billing_end

    def _execute(self):
        if self.end_date - self.start_date < timedelta(days=90):
            log.info("Widening bill window to increase odds of finding bill data.")
            self.start_date = self.end_date - timedelta(days=90)

        today = datetime.now().date()
        if self.end_date > today:
            log.info("Truncating end date to %s." % today)
            self.end_date = today

        if self.end_date < self.start_date:
            InvalidTimeRangeException("Final bill date must be after the start date.")

        # We define the scraper flow below using a simple state machine.
        state_machine = PageStateMachine(self._driver)

        # The scraper begins in an initial state, which transitions to the login page
        state_machine.add_state(
            name="init", action=self.init_action, transitions=["login"]
        )

        # Next we login. This either transitions to
        #   1) A login failure page
        #   2) An account home page, if there is only one account present
        #   3) An account selection page, if there are multiple accounts to select
        state_machine.add_state(
            name="login",
            page=SmudMyAccountLoginPage(self._driver),
            action=self.login_action,
            transitions=["choose_account", "account_home", "login_failed"],
        )

        # This state is entered when login fails. It is a terminal state. Currently it serves to
        # capture any error messages and expose them through an exception.
        state_machine.add_state(
            name="login_failed",
            page=SmudMyAccountFailedLoginPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        # The choose_account state is entered when one logs in with credentials that represent multiple accounts.
        # In this state, we choose an account, then transition to the account_home state
        state_machine.add_state(
            name="choose_account",
            page=SmudChooseAccountPage(self._driver),
            action=self.choose_account_action,
            transitions=["account_home"],
        )

        # The account_home state is entered either after logging in with an account that contains only one utility
        # account, or after choosing an account in the choose_account step. This is where we download billing history
        # for the active account. Afterwards we transition to the download_pdfs state
        state_machine.add_state(
            name="account_home",
            page=SmudMyAccountOverviewPage(self._driver),
            action=self.account_home_action,
            transitions=["bill_compare"],
        )

        state_machine.add_state(
            name="bill_compare",
            page=SmudBillComparePage(self._driver),
            action=self.bill_compare_action,
            transitions=["done"],
        )
        state_machine.add_state("done")

        # Begin in the init state, and run the state machine
        state_machine.set_initial_state("init")
        final_state = state_machine.run()
        if final_state == "done":
            return Results(bills=self.bill_history)
        raise Exception(
            "The scraper did not reach a finished state, this will require developer attention."
        )

    def init_action(self, _):
        self._driver.get("https://myaccount.smud.org/")

    def login_action(self, page: SmudMyAccountLoginPage):
        """Action for the 'login' state. Simply logs in with provided credentials."""
        log.info("Attempting to authenticate with the SMUD MyAccount page")
        page.login(self.username, self.password)

    def login_failed_action(self, page: SmudMyAccountFailedLoginPage):
        """Action for the 'login_failed' state. Gathers errors and raises an exception."""
        page.raise_on_error()

    def choose_account_action(self, page: SmudChooseAccountPage):
        """Action for the 'choose_account' state. Selects an account based on the account id passed to the scraper."""
        log.info("Selecting an account with number: {}".format(self.account_id))
        page.expand_accounts()
        time.sleep(5)
        page.select_account(self.account_id)

    def account_home_action(self, page: SmudMyAccountOverviewPage):
        """Action for the 'account_home' state. Navigates to the bill comparison page"""
        self._driver.get("https://myaccount.smud.org/manage/billcompare")

    def bill_compare_action(self, page: SmudBillComparePage):
        """Extracting billing info from the 'bill comparison' page"""
        page.toggle_usage_details()
        periods = page.get_all_billing_periods()
        seen_bills: set = set()

        # The comparison UI requires selecting two bill periods, after which it displays details for both.
        # (along with percentage differences and other statistics). Therefore we process bills in pairs
        idx = 0
        while idx < len(periods):
            first = periods[idx]

            if idx + 1 == len(periods):
                # If there is just one bill left, select the first bill as the comparison point
                second = periods[0]
            else:
                second = periods[idx + 1]

            page.select_bill(first)
            page.select_bill(second)

            # This can probably be more intelligent, but keeping it for now to rate limit our requests
            time.sleep(10)

            for bill_detail in page.get_visible_bill_details():
                bill_dates = (bill_detail.start, bill_detail.end)

                if bill_dates not in seen_bills and self.bill_in_range(bill_detail):
                    seen_bills.add(bill_dates)
                    bill_datum = self.make_billing_datum(bill_detail)
                    self.bill_history.append(bill_datum)

            idx += 2

    def make_billing_datum(self, bill_detail: BillPeriodDetails) -> BillingDatum:
        """Convert a billing detail summary from the website to a Gridium BillingDatum object"""
        bill_datum = BillingDatum(
            start=bill_detail.start,
            end=bill_detail.end,
            cost=bill_detail.total_charges,
            used=bill_detail.total_kwh,
            peak=bill_detail.max_kw,
            items=None,
            attachments=None,
        )

        pdf_bytes = self.download_pdf(bill_detail)
        if pdf_bytes:
            key = bill_upload.hash_bill_datum(self.account_id, bill_datum)
            attachment_entry = bill_upload.upload_bill_to_s3(BytesIO(pdf_bytes), key)
            if attachment_entry:
                bill_datum = bill_datum._replace(attachments=[attachment_entry])

        return bill_datum

    def bill_in_range(self, bill_detail: BillPeriodDetails) -> bool:
        """Determine whether a bill is in the scraper date range"""
        if self.start_date:
            if bill_detail.start < self.start_date:
                return False
        if self.end_date:
            if bill_detail.start > self.end_date:
                return False
        return True

    def download_pdf(self, bill_detail: BillPeriodDetails) -> Optional[bytes]:
        """Download the bill associated with a bill detail summary from the website"""
        if not bill_detail.download_link:
            return None

        response = requests.get(bill_detail.download_link)
        try:
            response.raise_for_status()
            return response.content
        except Exception:
            return None


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SMUDMyAccountBillingConfiguration(
        account_id=meter.utility_service.utility_account_id
    )

    return run_datafeed(
        SMUDMyAccountBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
