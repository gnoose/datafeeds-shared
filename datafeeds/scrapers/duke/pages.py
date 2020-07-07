""" Duke Page module """
import logging
import os

from datetime import date
from typing import List, Tuple

from dateutil.parser import parse as parse_date
from selenium.common.exceptions import NoSuchElementException

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datafeeds import config
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.typing import BillingDatum, BillingRange
from datafeeds.common.util.selenium import ec_and
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.scrapers.duke import errors
from datafeeds.scrapers.duke.errors import BillingScraperAccountUnavailable
from datafeeds.scrapers.duke.pdf_parser import parse_pdf

logger = None
log = logging.getLogger(__name__)


def scroll_to(driver, elem):
    driver.execute_script("arguments[0].scrollIntoView(false);", elem)


class DukeLoginPage(PageState):
    UsernameInputLocator = (By.CSS_SELECTOR, "input#username")
    PasswordInputLocator = (By.CSS_SELECTOR, "input#password")
    SubmitButtonLocator = (By.CSS_SELECTOR, "#mobile-login > div.text-center > button")

    def __init__(self, driver):
        super().__init__(driver)

    def get_ready_condition(self):
        return ec_and(
            EC.element_to_be_clickable(self.UsernameInputLocator),
            EC.element_to_be_clickable(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
        )

    def login(self, username: str, password: str):
        """Log into the Duke account """
        username_field = self.driver.find_element(*self.UsernameInputLocator)
        username_field.send_keys(username)

        password_field = self.driver.find_element(*self.PasswordInputLocator)
        password_field.send_keys(password)

        submit_button = self.driver.find_element(*self.SubmitButtonLocator)
        submit_button.click()


class DukeLoginFailedPage(PageState):
    """Page object representing a failed Duke login

    A failed login should produce an error message.
    This class facilitates detecting and retrieving that error message.
    """

    LoginErrorLocator = (By.XPATH, "//p[contains(text(), 'match our records')]")

    def __init__(self, driver):
        super().__init__(driver)

    def get_ready_condition(self):
        return ec_and(
            EC.title_contains("Sign In to Your Account"),
            EC.presence_of_element_located(self.LoginErrorLocator),
        )

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        error = self.driver.find_element(*self.LoginErrorLocator)
        message = "Login failed. The website error is: '{}'".format(error.text)
        raise errors.BillingScraperLoginException(message)


class DukeLandingPage(PageState):
    """Page object for the Duke Energy landing page """

    def __init__(self, driver):
        super().__init__(driver)
        self.link_to_accs_locator = None

    def get_ready_condition(self):
        self.link_to_accs_locator = (By.CSS_SELECTOR, "button#btnViewnPayBill")
        return EC.element_to_be_clickable(self.link_to_accs_locator)

    def open_accounts_page(self):
        """Opens page with all the accounts """
        log.info("In landing page")
        bills_page_for_meters_link = self.driver.find_element(
            *self.link_to_accs_locator
        )
        bills_page_for_meters_link.click()
        self.driver.sleep(1)
        self.driver.switch_to.window(self.driver.window_handles[-1])

    def open_profiler_page(self):
        log.info("opening profilers page")
        profiler_locator = (By.CSS_SELECTOR, "a#lnkInternalMeter")
        profiler_page_link = self.driver.find_element(*profiler_locator)
        profiler_page_link.click()


class AccountListPage(PageState):
    """Page object for account list page."""

    def __init__(self, driver, account_id: str):
        super().__init__(driver)
        self.account_id = account_id

    def get_ready_condition(self):
        return EC.visibility_of_element_located((By.CSS_SELECTOR, "div.NavGrid"))

    def click_account(self):
        """Find and click account id."""
        try:
            account_link = self.driver.find_element(
                By.XPATH, f"//a[contains(., '{self.account_id}')]"
            )
        except NoSuchElementException:
            raise BillingScraperAccountUnavailable(
                f"Account {self.account_id} not available from account page."
            )
        scroll_to(self.driver, account_link)
        self.driver.sleep(0.5)
        account_link.click()


class BillHistoryPage(PageState):
    """Page object for bill history page."""

    def __init__(self, driver, account_id: str, start_date: date, end_date: date):
        super().__init__(driver)
        self.start_date = start_date
        self.end_date = end_date
        self.account_id = account_id
        self.pdfs: List[Tuple[BillingRange, str, date]] = []
        self.download_dir = f"{config.WORKING_DIRECTORY}/current"

    def get_ready_condition(self):
        return EC.visibility_of_element_located((By.CSS_SELECTOR, "tr.GridHeader"))

    def download_pdfs(
        self, utility: str, utility_account_id: str
    ) -> List[BillingDatum]:
        """Download PDFs and collect billing date ranges.

        For dates in start_date - end_date range
          - build a list of start/end dates from the list of bills (end date in table, start date
            is previous bill end date + 1 day)
          - download PDFs for dates in range
          - return billing date ranges and pdf filenames
        """
        billing_data = []
        available_dates = self.driver.find_elements(
            By.CSS_SELECTOR, "tr > td:nth-child(2) > a"
        )
        available_dates = [parse_date(i.text).date() for i in available_dates]
        log.info(
            "available dates: %s", [dt.strftime("%Y-%m-%d") for dt in available_dates]
        )

        # loop through dates in table in ascending order
        previous_date = None
        for pdf_date in reversed(available_dates):
            if previous_date is None:
                previous_date = pdf_date
            # skip if the date isn't in the specified range
            if not (self.start_date <= pdf_date <= self.end_date):
                previous_date = pdf_date
                log.debug("skipping date outside range: %s", pdf_date)
                continue

            view_pdf_link = self.driver.find_element_by_xpath(
                f'//a[.="{pdf_date.strftime("%-m/%-d/%Y")}"]'
            )

            scroll_to(self.driver, view_pdf_link)
            self.driver.sleep(0.5)
            view_pdf_link.click()

            try:
                self.driver.wait(30).until(
                    file_exists_in_dir(
                        directory=self.download_dir, pattern=r"^View.pdf$",
                    )
                )
            except Exception:
                raise Exception(f"Unable to download file for %s" % pdf_date)

            curr_path = os.path.join(self.download_dir, f"View.pdf")
            new_path = os.path.join(
                self.download_dir, f"bill_{pdf_date.strftime('%Y-%m-%d')}.pdf"
            )
            os.rename(curr_path, new_path)

            datum = parse_pdf(new_path, utility, utility_account_id)
            billing_data.append(datum)
            log.info("parsed pdf for %s - %s", datum.start, datum.end)
            previous_date = pdf_date

        return billing_data

    def get_pdf(self, dt: date):
        """Find and return a pdf in self.pdfs with the same month and year"""
        pdfs = [
            pdf
            for pdf in self.pdfs
            if (pdf[0].end.month == dt.month and pdf[0].end.year == dt.year)
        ]
        log.debug("found pdf for %s/%s: %s", dt.month, dt.year, pdfs)
        if len(pdfs) != 1:
            return None
        return pdfs[0]
