import logging
import os
import re
import time
from typing import Optional, List

from datetime import timedelta, date
from dateutil.parser import parse as parse_time
from pdfminer.pdfparser import PDFSyntaxError
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    TimeoutException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from datafeeds import config
from datafeeds.common import BillingDatum, Configuration, Results
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed

from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import file_exists_in_dir, clear_downloads
from datafeeds.models import SnapmeterAccount, SnapmeterMeterDataSource, Meter
from datafeeds.parsers.pdfparser import pdf_to_str
from datafeeds.common.upload import hash_bill, upload_bill_to_s3

log = logging.getLogger(__name__)


class LoginException(Exception):
    pass


class NoRelevantBillsException(Exception):
    pass


class PortlandBizPortalException(Exception):
    pass


def _format_number(n):
    return n.replace("$", "").replace(",", "")


def extract_bill_data(
    pdf_filename, service_id, utility, utility_account_id
) -> Optional[BillingDatum]:
    # this function should upload the file to s3 to set attachments?
    try:
        text = pdf_to_str(pdf_filename)
    except PDFSyntaxError:
        log.exception("Downloaded bill file failed to parse as a PDF.")
        return None

    current_charges_pattern = "Current Charges.*\n.*\n.*\n.*\n\n.*\n(.*)\n"
    current_charges = re.search(current_charges_pattern, text).groups()[0]

    period_start, period_end = extract_bill_period(pdf_filename)

    usage_pattern = r"Energy Charges \((\d*) kWh\)"
    usage = re.search(usage_pattern, text).groups()[0]

    on_peak_demand_pattern = r"On-Peak Demand \((\d+\.\d+)\ KW"
    on_peak_demand = re.search(on_peak_demand_pattern, text).groups()[0]

    offpeak_demand_pattern = r"Off-Peak Demand \((\d+\.\d+)\ KW"
    offpeak_demand = re.search(offpeak_demand_pattern, text).groups()[0]

    bill_attachment = []
    if config.enabled("S3_BILL_UPLOAD"):
        log.info("S3_BILL_UPLOAD is enabled")
        with open(pdf_filename, "rb") as f:
            key = hash_bill(
                service_id,
                period_start,
                period_end,
                _format_number(current_charges),
                0,
                _format_number(usage),
            )
            # no statement date; use end date
            bill_attachment.append(
                upload_bill_to_s3(
                    f,
                    key,
                    source="portlandgeneral.com",
                    statement=period_end,
                    utility=utility,
                    utility_account_id=utility_account_id,
                )
            )
            log.info("Uploaded bill %s to s3", bill_attachment)

    bill = BillingDatum(
        start=period_start,
        end=period_end,
        statement=period_end,
        cost=_format_number(current_charges),
        used=_format_number(usage),
        peak=max(float(on_peak_demand), float(offpeak_demand),),
        items=[],
        attachments=bill_attachment,
        utility_code=None,
    )

    return bill


def extract_bill_period(pdf_filename):
    """Convert the PDF to a string so we can determine the dates this bill covers."""
    try:
        text = pdf_to_str(pdf_filename)
    except PDFSyntaxError:
        log.exception("Downloaded bill file failed to parse as a PDF.")
        return None, None

    pattern = r"Service Period\n(\d+/\d+/\d+)\n(\d+/\d+/\d+)"
    match = re.search(pattern, text)

    if match:
        period_a = parse_time(match.group(1)).date()
        period_b = parse_time(match.group(2)).date()
        return min(period_a, period_b), max(period_a, period_b)

    return None, None


def _overlap(a, b):
    c_start = max(a.start, b.start)
    c_end = min(a.end, b.end)
    return max(c_end - c_start, timedelta())


def _adjust_bill_dates(bills: List[BillingDatum]) -> List[BillingDatum]:
    """Ensure that the input list of bills is sorted by date and no two bills have overlapping dates."""
    bills.sort(key=lambda x: x.start)

    final_bills: List[BillingDatum] = []
    for b in bills:
        for other in final_bills:
            if _overlap(b, other) > timedelta() or b.start == other.end:
                b = b._replace(start=max(b.start, other.end + timedelta(days=1)))
        final_bills.append(b)

    return final_bills


class PortlandBizportalConfiguration(Configuration):
    def __init__(
        self,
        utility: str,
        utility_account_id: str,
        account_group,
        bizportal_account_number,
        service_id,
    ):
        super().__init__(scrape_bills=True)
        self.account_group = account_group

        # We need to guarantee that this ID is a string; it might have a prefix of zeros.
        self.bizportal_account_number = str(bizportal_account_number)
        self.service_id = service_id
        self.utility = utility
        self.utility_account_id = utility_account_id


class BillingPage:
    def __init__(self, driver):
        self.driver = driver

    def next_page(self) -> None:
        next_page_button_xpath = "//button[@role='next-page-button']"
        next_page_button = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, next_page_button_xpath))
        )
        self.driver.execute_script(
            "window.scrollTo(0," + str(next_page_button.location["y"]) + ")"
        )
        time.sleep(2)
        log.info("Going to next page")
        next_page_button.click()
        # Wait for the table to reappear
        WebDriverWait(self.driver, 25).until(
            ec.presence_of_element_located(
                (By.XPATH, "//p[contains(text(), 'Amount due')]")
            )
        )

    def page_numbers(self) -> int:
        page_string_xpath = "//button[@role='prev-page-button']//following::div[1]"
        page_string = (
            WebDriverWait(self.driver, 15)
            .until(ec.presence_of_element_located((By.XPATH, page_string_xpath)))
            .text
        )
        # For example: "1 of 4"
        return int(page_string.split()[2])

    def choose_account(self, account_group, bizportal_account_number) -> bool:
        account_group_button_xpath = (
            "//p[contains(text(), 'Account group:')]//following::button[1]"
        )
        account_group_button = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, account_group_button_xpath))
        )
        account_group_button.click()
        account_group_xpath = "//span[contains(text(), '%s')]" % account_group
        ag_target = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, account_group_xpath))
        )
        ag_target.click()

        # Choosing the account group causes account info to load.
        # The capitalization on "Amount due" doesn't match what is shown in the UI
        WebDriverWait(self.driver, 45).until(
            ec.presence_of_element_located(
                (By.XPATH, "//p[contains(text(), 'Amount due')]")
            )
        )

        bizportal_account_number_xpath = (
            "//p[contains(text(), 'Selected account:')]//following::button[1]"
        )
        ban_button = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, bizportal_account_number_xpath))
        )
        ban_button.click()
        ban_target_xpath = "//span[contains(text(), '%s')]" % bizportal_account_number
        ban_target = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, ban_target_xpath))
        )
        # For some reason, it is possible that selenium can select the wrong account number here.
        # Adding an absolute time sleep step seems to help.
        time.sleep(2)
        ban_target.click()

        try:
            WebDriverWait(self.driver, 25).until(
                ec.presence_of_element_located(
                    (By.XPATH, "//p[contains(text(), 'Amount due')]")
                )
            )
            return True
        except TimeoutException:
            return False

    def handle_pdfs(
        self,
        service_id,
        start: date,
        end: date,
        utility,
        utility_account_id,
        first_page=False,
    ) -> List[BillingDatum]:
        pdf_links_xpath = "//a[contains(text(), 'View Bill')]"

        download_dir = self.driver.download_dir
        bill_data: Optional[List[BillingDatum]]
        bill_data = []
        # The most recent bill link is a special case.
        # It does not download directly but opens a new page with a download link.
        first_link_found = False
        if not first_page:
            first_link_found = True

        if not first_link_found:
            pdf_link_1 = WebDriverWait(self.driver, 15).until(
                ec.presence_of_element_located((By.XPATH, pdf_links_xpath))
            )

            log.info("Downloading most recent bill")
            pdf_link_1.click()

            if (
                self.driver.current_url
                == "https://new.portlandgeneral.com/secure/view-bill"
            ):
                download_bill_button_xpath = (
                    "//span[contains(text(), 'Download bill (PDF)')]"
                )
                download_bill_button = WebDriverWait(self.driver, 25).until(
                    ec.element_to_be_clickable((By.XPATH, download_bill_button_xpath))
                )

                self.driver.execute_script(
                    "window.scrollTo(0, window.scrollY+(document.body.scrollHeight/2))"
                )
                time.sleep(2)

                try:
                    download_bill_button.click()
                except ElementClickInterceptedException:
                    log.error("Could not click")
                    raise

                filename = self.driver.wait(60).until(
                    file_exists_in_dir(download_dir, r".*\.pdf$")
                )

                file_path = os.path.join(download_dir, filename)
                log.info("Processing most recent bill")
                single_bill = extract_bill_data(
                    file_path, service_id, utility, utility_account_id
                )
                clear_downloads(self.driver.download_dir)

                bill_data.append(single_bill)
                log.info("appended a bill")

                bill_history_button_xpath = (
                    "//span[contains(text(), 'Billing and payment history')]"
                )
                bill_history_button = WebDriverWait(self.driver, 25).until(
                    ec.element_to_be_clickable((By.XPATH, bill_history_button_xpath))
                )
                log.info("Returning to bill history page")
                bill_history_button.click()

        pdf_links = WebDriverWait(self.driver, 25).until(
            ec.presence_of_all_elements_located((By.XPATH, pdf_links_xpath))
        )
        log.info("Found %s pdf's on page", len(pdf_links))

        for link in pdf_links:
            if not first_link_found:
                first_link_found = True
                continue
            self.driver.execute_script(
                "window.scrollTo(0," + str(link.location["y"]) + ")"
            )
            time.sleep(2)
            link.click()

            filename = self.driver.wait(90).until(
                file_exists_in_dir(download_dir, r".*\.pdf$")
            )
            file_path = os.path.join(download_dir, filename)

            period_start, period_end = extract_bill_period(file_path)

            # If the bill starts after our end date, skip it
            if period_start > end:
                clear_downloads(self.driver.download_dir)
                continue

            # If the bill ends before our start date, break and return (finding where to end)
            if period_end < start:
                break

            if not period_start or not period_end:
                log.info(
                    "Could not determine bill period for pdf %s. Skipping" % file_path
                )
                continue

            single_bill = extract_bill_data(
                file_path, service_id, utility, utility_account_id
            )

            clear_downloads(self.driver.download_dir)

            bill_data.append(single_bill)
            log.info("appended a bill")

        non_overlapping_bills = _adjust_bill_dates(bill_data)
        return non_overlapping_bills


class AccountPage:
    def __init__(self, driver):
        self.driver = driver

    def goto_billing(self) -> BillingPage:

        billing_link_xpath = "//span[contains(text(), 'Billing & Payment History')]"
        billing_link = WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located((By.XPATH, billing_link_xpath))
        )

        billing_link.click()

        return BillingPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def _account_page_load_successful(self) -> bool:
        try:
            WebDriverWait(self.driver, 15).until(
                ec.presence_of_element_located(
                    (By.XPATH, "//span[contains(text(), 'My Accounts')]")
                )
            )
            return True
        except TimeoutException:
            return False

    def login(self, username: str, password: str) -> AccountPage:
        self.driver.get("https://new.portlandgeneral.com/auth/sign-in")

        # Press escape to close modal
        ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()

        try:
            username_box = WebDriverWait(self.driver, 15).until(
                ec.presence_of_element_located((By.NAME, "email"))
            )
        except TimeoutException:
            raise LoginException("Login page failed to load")

        password_box = WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located((By.NAME, "password"))
        )

        login_button = WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located((By.ID, "sign-in-submit-btn"))
        )

        username_box.send_keys(username)
        password_box.send_keys(password)
        login_button.click()

        if not self._account_page_load_successful():
            raise LoginException("Login failed")

        return AccountPage(self.driver)


# class Scraper(BaseApiScraper):
class Scraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "Portland Bizportal Scraper"

    @property
    def account_group(self):
        return self._configuration.account_group

    @property
    def bizportal_account_number(self):
        return str(self._configuration.bizportal_account_number)

    @property
    def utility(self):
        return str(self._configuration.utility)

    @property
    def utility_account_id(self):
        return str(self._configuration.utility_account_id)

    @property
    def service_id(self):
        return self._configuration.service_id

    def _execute(self):
        if self.end_date - self.start_date < timedelta(days=90):
            log.info("Widening bill window to increase odds of finding bill data.")
            self.start_date = self.end_date - timedelta(days=90)

        log.info("Final date range: %s - %s", self.start_date, self.end_date)

        login_page = LoginPage(self._driver)
        log.info("=" * 80)
        log.info("Logging in")
        log.info("=" * 80)
        account_page = login_page.login(self.username, self.password)

        billing_page = account_page.goto_billing()

        found_account = billing_page.choose_account(
            self.account_group, self.bizportal_account_number
        )
        if found_account:
            log.info(
                "Loading bills for account group %s and account number %s",
                self.account_group,
                self.bizportal_account_number,
            )
        else:
            log.error(
                "Could not load bills for account group %s and account number %s",
                self.account_group,
                self.bizportal_account_number,
            )
            raise PortlandBizPortalException

        total_pages = billing_page.page_numbers()
        log.info("Detected %s pages of bills", total_pages)

        bills: List[BillingDatum] = []
        first_page = True
        for page in range(total_pages):
            more_bills = billing_page.handle_pdfs(
                self.service_id,
                self.start_date,
                self.end_date,
                self.utility,
                self.utility_account_id,
                first_page=first_page,
            )
            log.info("more_bills is %s", more_bills)
            if more_bills:
                bills.append(more_bills)
                billing_page.next_page()
                first_page = False
            # if handle_pdfs returns None the date range has been covered
            else:
                break

        log.info("bills are %s", bills)
        return Results(bills=bills[0])


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = PortlandBizportalConfiguration(
        utility=meter.utility_service.utility,
        utility_account_id=meter.utility_service.utility_account_id,
        account_group=datasource.meta.get("accountGroup"),
        bizportal_account_number=datasource.meta.get("bizportalAccountNumber"),
        service_id=meter.service_id,
    )

    return run_datafeed(
        Scraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
