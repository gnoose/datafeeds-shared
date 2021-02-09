""" Duke Page module """
import logging
import re
import os

from io import BytesIO

from datetime import date, timedelta
from typing import List, Tuple

from dateutil.parser import parse as parse_date
from selenium.common.exceptions import NoSuchElementException

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from datafeeds import config
from datafeeds.common.upload import hash_bill, upload_bill_to_s3
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.typing import BillingDatum, BillingRange
from datafeeds.common.util.selenium import ec_and
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.scrapers.duke import errors
from datafeeds.scrapers.duke.errors import BillingScraperAccountUnavailable

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
        self.link_to_accs_locator = (By.CSS_SELECTOR, "button#btnBillView")
        return EC.element_to_be_clickable(self.link_to_accs_locator)

    def open_accounts_page(self):
        """Opens page with all the accounts """
        log.info("In landing page")
        bills_page_for_meters_link = self.driver.find_element(
            *self.link_to_accs_locator
        )
        bills_page_for_meters_link.click()
        self.driver.sleep(5)
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
        return EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "table#billViewAccounts")
        )

    def click_account(self):
        """Find and click account id."""
        try:
            account_link = self.driver.find_element(
                By.XPATH,
                f"//td[contains(., '{self.account_id}')]/following-sibling::td/a",
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
        return EC.visibility_of_element_located((By.CSS_SELECTOR, "div#account"))

    def get_bills(self, utility: str, utility_account_id: str) -> List[BillingDatum]:
        billing_data = []

        available_dates = self.driver.find_elements(
            By.CSS_SELECTOR, "table.table-alt a.bill-view-link"
        )
        available_dates = [parse_date(i.text).date() for i in available_dates]
        log.info(
            "available dates: %s", [dt.strftime("%Y-%m-%d") for dt in available_dates]
        )

        xpath_locators = {
            # Finds the last KWH reading under Total Usage column
            "cost": "//table[contains(., 'NEW CHARGES')]/tbody/tr/td[3]",
            "used": "(//table[contains(.,'USAGE')]//tr/td[contains(., 'KWH')])",
            "usage_kw": "//table[contains(.,'USAGE')]//tr/td[contains(.,'KW') and not(contains(.,'KWH'))]",
        }

        # loop through dates in table in ascending order
        for pdf_date in reversed(available_dates):
            # skip if the date isn't in the specified range
            if not (self.start_date <= pdf_date <= self.end_date):
                log.debug("skipping date outside range: %s", pdf_date)
                continue

            view_bill_link = self.driver.find_element_by_xpath(
                '//a[.="%s"]' % pdf_date.strftime("%m/%d/%Y")
            )
            scroll_to(self.driver, view_bill_link)

            self.driver.sleep(0.5)
            view_bill_link.click()

            self.driver.wait(30).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.billImage"))
            )

            start_date = None
            end_date = None
            cost = None
            used = None
            peak = None

            dates_line_text: str = self.driver.find_element_by_xpath(
                "//td[contains(., 'Service From:')]"
            ).text
            dates_match = re.search(
                r"Service From: (?P<from>\w+ \d\d) to (?P<to>\w+ \d\d) \(\d\d Days\)",
                dates_line_text,
            )

            if dates_match:
                start_date = parse_date(
                    dates_match.group("from") + pdf_date.strftime(" %Y")
                )
                end_date = parse_date(
                    dates_match.group("to") + pdf_date.strftime(" %Y")
                )

            cost_match = self.driver.find(xpath_locators["cost"], xpath=True)
            if cost_match:
                cost = cost_match.text
                cost = float(cost.replace("$", "").replace(",", ""))

            kwh_usages = []
            for match in self.driver.find_all(xpath_locators["used"], xpath=True):
                # include only if it has a reading values as siblings; exclude credit line items
                parent = match.find_element_by_xpath("..")
                # meter number, previous reading, current reading
                readings_text = ""
                for idx, child in enumerate(parent.find_elements_by_xpath(".//td")):
                    log.debug("\t%s\t%s", idx, child.text.strip())
                    readings_text += child.text.strip()
                    if idx == 2:
                        break
                if not readings_text:
                    log.info("skipping non-reading line item: %s", parent.text)
                    continue
                kwh_value = float(
                    match.text.replace("KWH", "").replace(",", "").strip()
                )
                kwh_usages.append(kwh_value)

            if kwh_usages:
                used = sum(kwh_usages)

            kw_usages = []
            for usage_kw_match in self.driver.find_all(
                xpath_locators["usage_kw"], xpath=True
            ):
                kw_usages.append(
                    float(
                        usage_kw_match.text.replace("KW", "").replace(",", "").strip()
                    )
                )

            if kw_usages:
                peak = max(kw_usages)

            data = BillingDatum(
                start=start_date,
                end=end_date - timedelta(days=1),
                statement=end_date - timedelta(days=1),
                cost=cost,
                peak=peak,
                used=used,
                items=None,
                attachments=None,
                utility_code=None,
            )

            self.driver.find("a#billImageToPrint").click()
            self.driver.sleep(1)
            self.driver.switch_to.window(self.driver.window_handles[-1])

            # the filename of the printed pdf is f"{current page title}.pdf"
            self.driver.execute_script("window.print();")

            try:
                file_exists_in_dir(
                    directory=self.download_dir, pattern=r"^Bill View Bill Image.pdf$"
                )
            except Exception:
                raise Exception("Unable to download file for %s" % pdf_date)

            curr_path = os.path.join(self.download_dir, "Bill View Bill Image.pdf")
            new_path = os.path.join(
                self.download_dir, f"bill_{pdf_date.strftime('%Y-%m-%d')}.pdf"
            )
            os.rename(curr_path, new_path)

            log.info("parsed bill for %s - %s", data.start, data.end)

            self.driver.find("a#close").click()
            self.driver.sleep(1)
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.sleep(1)

            # upload PDF:
            key = hash_bill(
                utility_account_id,
                data.start,
                data.end,
                data.cost,
                data.peak,
                data.used,
            )

            with open(new_path, "rb") as pdf_data:
                attachment_entry = upload_bill_to_s3(
                    BytesIO(pdf_data.read()),
                    key,
                    source="www.duke-energy.com",
                    statement=data.end,
                    utility=utility,
                    utility_account_id=utility_account_id,
                )

            if attachment_entry:
                data = data._replace(attachments=[attachment_entry])

            billing_data.append(data)

            # Click Bill Information in breadcrumbs to go back to bills list page
            self.driver.find("a#billInformation").click()

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
