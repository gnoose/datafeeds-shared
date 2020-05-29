""" Duke Page module """
import logging
import os
import csv
import re

from datetime import date, timedelta
from typing import List, Tuple, Dict

from dateutil.parser import parse as parse_date

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datafeeds import config
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.typing import BillingDatum, BillingRange
from datafeeds.common.util.selenium import ec_and
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.parsers import pdfparser
from datafeeds.scrapers.duke import errors
from datafeeds.common.upload import hash_bill, upload_bill_to_s3


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
        account_link = self.driver.find_element(
            By.XPATH, f"//a[contains(., '{self.account_id}')]"
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

    def download_pdfs(self):
        """Download PDFs and collect billing date ranges.

        For dates in start_date - end_date range
          - build a list of start/end dates from the list of bills (end date in table, start date
            is previous bill end date + 1 day)
          - download PDFs for dates in range
          - return billing date ranges and pdf filenames
        """

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
                self.download_dir, f"bill_{pdf_date.strftime('%-m_%-d_%Y')}.pdf"
            )
            os.rename(curr_path, new_path)

            """
            Get the date range from the pdf: Service From:\n\nFEB 20 to MAR 20 ( 29 Days).
            Billing ranges overlap: FEB 20 to MAR 20, MAR 20 - APR 21; exclude last date
            """
            text = pdfparser.pdf_to_str(new_path)
            dates = re.search(r"([A-Z]+ \d+) to ([A-Z]+ \d+)", text, re.MULTILINE)
            if dates:
                start_text = dates.group(1)
                end_text = dates.group(2)
                end = parse_date("%s %s" % (end_text, pdf_date.year)).date()
                year = pdf_date.year - 1 if "JAN" in end_text else pdf_date.year
                start = parse_date("%s %s" % (start_text, year)).date()
                log.info(
                    "%s bill has date range %s - %s from pdf (%s to %s)",
                    pdf_date.strftime("%Y-%m-%d"),
                    start.strftime("%Y-%m-%d"),
                    end.strftime("%Y-%m-%d"),
                    start_text,
                    end_text,
                )
            else:
                start = previous_date - timedelta(days=1)
                end = pdf_date
                log.info(
                    "%s bill has date range %s - %s from table",
                    pdf_date.strftime("%Y-%m-%d"),
                    start.strftime("%Y-%m-%d"),
                    end.strftime("%Y-%m-%d"),
                )
            self.pdfs.append(
                (
                    BillingRange(start=start, end=end - timedelta(days=1)),
                    new_path,
                    pdf_date,
                )
            )
            log.debug("found pdf for %s - %s", start, end)
            previous_date = pdf_date

        return self.pdfs

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

    def parse_csv(self, filename: str) -> Dict[date, BillingDatum]:
        """Parse a UsageHistory.csv into a collection of BillingDatum objects."""

        """
        Account Number,"#1769654818   ",,,,,,,,,,,,,,,,,,,,,,,
        Current Balance," $0.00",,,,,,,,,,,,,,,,,,,,,,,
        ,,,,,,,,,,,,,,,,,,,,,,,,,
        "LGS - Large General Service",
        "Meter Number","#077618850"
        "Contract Demand",
        "Contract: 100"
        ,

        and then an assortment of layouts:

        "Bill Month","Bill Year","Extra Facilities","Sales Tax","# of Days","Adj","Total Charges",

        "Bill Month","Bill Year","Customer Charge","On-Peak Actual Demand(Winter)",
        "On-Peak Billing Demand","On-Peak Billing Demand Amount","On-Peak Actual Demand(Summer)",
        "On-Peak Billing Demand","On-Peak Billing Demand Amount","Off-Peak Actual Demand",
        "On-Peak Energy(Winter)","On-Peak Energy(Winter) Amount","On-Peak Energy(Summer)",
        "On-Peak Energy(Summer) Amount","Off-Peak Energy","Off-Peak Energy Amount","Economy Demand",
        "Economy Demand Amount","Renewable Energy Rider","Sales Tax","# of Days","Adj","Total Charges",

        "Bill Month","Bill Year","Standby Generation Credit","Sales Tax","# of Days","Adj","Total Charges",

        "Bill Month","Bill Year","Electricity Usage","Electricity Usage Amount","Actual Demand",
        "Billing Demand","Renewable Energy Rider","Sales Tax","# of Days","Adj","Total Charges",

        "Bill Month","Bill Year","Electricity Usage","Electricity Usage Amount","Sales Tax",
        "# of Days","Adj","Total Charges",

        "Bill Month","Bill Year","Standby Generation Credit","Sales Tax","# of Days","Adj",
        "Total Charges",
        """
        data: Dict[date, BillingDatum] = {}
        # find the headers; they vary by meter typee, and aren't at the top of the file
        headers = []
        with open(filename, "r") as f:
            reader = csv.reader(f)
            for header_row in reader:
                if header_row and "Bill Month" in header_row:
                    headers = header_row
                    break
        log.info("found headers %s", headers)
        seen_headers = False
        with open(filename, "r") as f:
            dict_reader = csv.DictReader(f, headers)
            for row in dict_reader:
                log.debug("row=%s", row)
                # skip rows up to header
                if "Bill Month" in row and row["Bill Month"] == "Bill Month":
                    seen_headers = True
                    continue
                if not seen_headers:
                    continue
                month = int(row["Bill Month"])
                year = int(row["Bill Year"])
                pdf = self.get_pdf(date(year, month, 1))
                # skip entry if no pdf found for the date
                if not pdf:
                    log.info("no pdf found with end date %s/%s", month, year)
                    continue
                (billing_range, pdf_path, statement_date) = pdf
                start = billing_range.start
                end = billing_range.end
                cost = float(row["Total Charges"].replace("$", "").replace(",", ""))
                # get the max peak and used from possibly multiple columns
                peak = None
                used = 0.0
                for col in row.keys():
                    if not row[col] or "$" in row[col]:
                        continue
                    if not ("Demand" in col or "Energy" in col):
                        continue
                    log.debug("col %s = %s", col, row[col])
                    val = float(row[col].replace(",", ""))
                    # On-Peak Actual Demand(Winter) or Billing Demand
                    if "Demand" in col:
                        peak = val if peak is None else max(val, peak)
                    # On-Peak Energy(Winter) and Off-Peak Energy
                    # but not On-Peak Energy(Winter) Amount or Off-Peak Energy Amount
                    if "Energy" in col and "Amount" not in col:
                        used += val
                if "Electricity Usage" in row:
                    used = float(row["Electricity Usage"].replace(",", ""))  # type: ignore
                data[end] = BillingDatum(
                    start=start,
                    end=end,
                    cost=cost,
                    peak=peak,
                    statement=statement_date,
                    used=used,
                    items=None,
                    attachments=None,
                )
        return data

    def get_details(self, utility: str, account_id: str) -> List[BillingDatum]:
        billing_data: List[BillingDatum] = []

        # click 13 Month kWh history
        self.driver.find_element(
            By.XPATH, "//a[contains(., '13 Month kWh History')]"
        ).click()

        self.driver.sleep(1)
        self.driver.switch_to.window(self.driver.window_handles[-1])
        # there can be multiple View Usage History links for a totalized meter
        links = self.driver.find_elements(By.XPATH, "//a[.='View Usage History']")
        log.info("%s View Usage History links", len(links))
        data: Dict[date, List[BillingDatum]] = {}
        for idx in range(len(links)):
            # re-get links each time to avoid stale element errors
            links = self.driver.find_elements(By.XPATH, "//a[.='View Usage History']")
            log.info("getting usage %s/%s", idx + 1, len(links))
            # click View Usage History
            links[idx].click()
            # click Export
            self.driver.find_element(
                By.CSS_SELECTOR, "a#agreementHistoryToExport"
            ).click()
            try:
                self.driver.wait(30).until(
                    file_exists_in_dir(
                        directory=self.download_dir, pattern=r"^UsageHistory\.csv$",
                    )
                )
            except Exception:
                raise Exception(f"Unable to download 13 Month kWh history")
            usage_filename = "%s/%s-UsageHistory.csv" % (self.download_dir, idx)
            os.rename("%s/UsageHistory.csv" % self.download_dir, usage_filename)
            history = self.parse_csv(usage_filename)
            for dt in history:
                data.setdefault(dt, [])
                data[dt].append(history[dt])
            self.driver.back()

        # combine the BillingDatum records for each date
        for dt in data:
            log.debug("bills for %s = %s", dt, data[dt])
            (billing_range, pdf_path, statement_date) = self.get_pdf(dt)
            cost = 0.0
            peak = None
            used = 0.0
            for bill_datum in data[dt]:
                cost += bill_datum.cost
                used += bill_datum.used
                if bill_datum.peak:
                    peak = (
                        bill_datum.peak if peak is None else max(bill_datum.peak, peak)
                    )
            with open(pdf_path, "rb") as bill_file:
                attachment = upload_bill_to_s3(
                    bill_file,
                    "%s.pdf"
                    % hash_bill(
                        account_id,
                        billing_range.start,
                        billing_range.end,
                        cost,
                        peak,
                        used,
                    ),
                    source="duke-energy.com",
                    statement=statement_date,
                    utility=utility,
                    utility_account_id=account_id,
                )
            bill_data = BillingDatum(
                start=billing_range.start,
                end=billing_range.end,
                cost=cost,
                peak=peak,
                statement=statement_date,
                used=used,
                items=None,
                attachments=[attachment],
            )
            billing_data.append(bill_data)
        return billing_data
