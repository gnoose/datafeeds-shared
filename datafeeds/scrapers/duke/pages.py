""" Duke Page module """
import time
import logging
import os
import csv

from itertools import islice
from datetime import date, timedelta
from typing import List, Tuple

from dateutil.parser import parse as parse_date

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datafeeds import config
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.typing import BillingDatum, BillingRange
from datafeeds.common.util.selenium import ec_and
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.scrapers.duke import errors
from datafeeds.common.upload import hash_bill_datum, upload_bill_to_s3


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
        time.sleep(15)
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
        time.sleep(1)
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
        account_link = self.driver.find_element(
            By.XPATH, f"//a[contains(., '{self.account_id}')]"
        )

        scroll_to(self.driver, account_link)
        time.sleep(0.5)
        account_link.click()


class BillHistoryPage(PageState):
    """Page object for bill history page."""

    def __init__(self, driver, account_id: str, start_date: date, end_date: date):
        super().__init__(driver)
        self.start_date = start_date
        self.end_date = end_date
        self.pdfs: List[Tuple[BillingRange, str]] = []

    def get_ready_condition(self):
        return EC.visibility_of_element_located((By.CSS_SELECTOR, "tr.GridHeader"))

    def download_pdfs(self):

        """
        for dates in start_date - end_date range
          - build a list of start/end dates from the list of bills (end date in table, start date
            is previous bill end date + 1 day)
          - download PDFs for dates in range
          - return billing date ranges and pdf filenames
        """

        available_dates = self.driver.find_elements(
            By.CSS_SELECTOR, "tr > td:nth-child(2) > a"
        )
        available_dates = [parse_date(i.text).date() for i in available_dates]

        # loop through dates in table in ascending order
        for _date in reversed(available_dates):
            # skip if the date isn't in the specified range
            if not (self.start_date <= _date <= self.end_date):
                previous_date = _date
                continue

            view_pdf_link = self.driver.find_element_by_xpath(
                f'//a[.="{_date.strftime("%-m/%-d/%Y")}"]'
            )

            scroll_to(self.driver, view_pdf_link)
            time.sleep(0.5)
            view_pdf_link.click()

            download_dir = f"{config.WORKING_DIRECTORY}/current"
            try:
                self.driver.wait(30).until(
                    file_exists_in_dir(directory=download_dir, pattern=r"^View.pdf$",)
                )
            except Exception:
                raise Exception(f"Unable to download file...")

            curr_path = os.path.join(download_dir, f"View.pdf")
            new_path = os.path.join(
                download_dir, f"bill_{_date.strftime('%-m_%-d_%Y')}.pdf"
            )
            os.rename(curr_path, new_path)

            self.pdfs.append(
                (
                    BillingRange(start=previous_date + timedelta(days=1), end=_date),
                    new_path,
                )
            )

            previous_date = _date

        return self.pdfs

    def get_details(self, utility, account_id):
        billing_data: List[BillingDatum] = []

        # click 13 Month kWh history
        self.driver.find_element(
            By.XPATH, "//a[contains(., '13 Month kWh History')]"
        ).click()

        time.sleep(1)
        self.driver.switch_to.window(self.driver.window_handles[-1])
        # click View Usage History
        self.driver.find_element(By.XPATH, "//a[.='View Usage History']").click()
        # click Export
        self.driver.find_element(By.CSS_SELECTOR, "a#agreementHistoryToExport").click()

        download_dir = f"{config.WORKING_DIRECTORY}/current"
        try:
            self.driver.wait(30).until(
                file_exists_in_dir(
                    directory=download_dir, pattern=r"^UsageHistory\.csv$",
                )
            )

        except Exception:
            raise Exception(f"Unable to download file...")

        file_path = download_dir + "/UsageHistory.csv"

        # open UsageHistory.csv
        # parse CSV

        n = 0
        f = open(file_path, "r")
        #  we need to skip few of the starting lines
        for line in f.readlines():
            if line.startswith(
                '"Bill Month","Bill Year"'
            ):  # line with headers was found
                break
            n += 1
        f.close()
        f = islice(open(file_path, "r"), n, None)
        reader = csv.DictReader(f)
        for row in reader:
            month = int(row["Bill Month"])
            year = int(row["Bill Year"])

            # find pdf in self.pdfs with the same month and year
            a = [
                i
                for i in self.pdfs
                if (i[0].end.month == month and i[0].end.year == year)
            ]

            # skip entry if no pdf found for the date
            if len(a) != 1:
                continue

            a = a[0]

            billing_range = a[0]
            pdf_path = a[1]

            start = billing_range.start
            end = billing_range.end
            cost = float(row["Total Charges"].replace("$", "").replace(",", ""))
            usage = float(row["Electricity Usage"].replace(",", ""))
            peak = float(row["Billing Demand"].replace(",", ""))

            bill_data = BillingDatum(
                start=start,
                end=end,
                cost=cost,
                peak=peak,
                statement=billing_range.end,
                used=usage,
                items=None,
                attachments=None,
            )

            with open(pdf_path, "rb") as bill_file:
                key = hash_bill_datum(account_id, bill_data) + ".pdf"
                bill_data = bill_data._replace(
                    attachments=[
                        upload_bill_to_s3(
                            bill_file,
                            key,
                            source="duke-energy.com",
                            statement=billing_range.end,
                            utility=utility,
                            utility_account_id=account_id,
                        )
                    ]
                )
            billing_data.append(bill_data)
        f.close()

        return billing_data
