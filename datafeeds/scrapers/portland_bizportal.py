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
from datafeeds.common.typing import BillingDatum
from datafeeds.common.support import Configuration
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Results

from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import file_exists_in_dir
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

    current_charges_pattern = "Current Charges(.*?)Cycle"
    for line in (
        re.search(current_charges_pattern, text, re.DOTALL).group(1).split("\n")
    ):
        # get the last number
        if re.match(r"[\d,\.]", line.strip()):
            current_charges = line.strip().replace(",", "")

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
        peak=max(
            float(on_peak_demand),
            float(offpeak_demand),
        ),
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


def close_survey(driver) -> bool:
    try:
        log.debug("looking for survey modal")
        WebDriverWait(driver, 5).until(
            ec.visibility_of_element_located(
                (By.CSS_SELECTOR, "#kampyleInviteContainer")
            )
        )
        log.debug("switching to survey iframe")
        WebDriverWait(driver, 5).until(
            ec.frame_to_be_available_and_switch_to_it(
                (By.CSS_SELECTOR, "#kampyleInvite")
            )
        )
        log.debug("closing survey")
        WebDriverWait(driver, 5).until(
            ec.element_to_be_clickable((By.CSS_SELECTOR, "#kplDeclineButton"))
        )
        driver.find_element_by_css_selector("#kplDeclineButton").click()
        driver.switch_to.default_content()
        time.sleep(2)
        return True
    except TimeoutException:
        log.debug("survey not found")
        pass
    return False


def close_modal(driver):
    try:
        log.debug("trying to close modal")
        close_button = WebDriverWait(driver, 5).until(
            ec.presence_of_element_located(
                (By.CSS_SELECTOR, '.MuiDialog-container button[aria-label="close"]')
            )
        )
        close_button.click()
    except TimeoutException:
        pass


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
        self.seen_survey = False

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

    def click_close_modal(self, element):
        try:
            element.click()
        except ElementClickInterceptedException:
            close_modal(self.driver)
            element.click()

    def choose_account(self, account_group, bizportal_account_number) -> bool:
        account_group_button_xpath = (
            "//p[contains(text(), 'Account group:')]//following::button[1]"
        )
        account_group_button = WebDriverWait(self.driver, 25).until(
            ec.presence_of_element_located((By.XPATH, account_group_button_xpath))
        )
        time.sleep(10)
        # This click tends to throw a 'stale element reference' error
        account_group_button.click()
        account_group_xpath = "//span[contains(text(), '%s')]" % account_group
        ag_target = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, account_group_xpath))
        )
        self.click_close_modal(ag_target)

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
        log.debug("looking for %s", bizportal_account_number_xpath)
        ban_button = WebDriverWait(self.driver, 15).until(
            ec.presence_of_element_located((By.XPATH, bizportal_account_number_xpath))
        )
        self.click_close_modal(ban_button)
        # type the account number into the search box, then enter
        log.debug("selecting account %s", bizportal_account_number)
        account_number_box = WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'input[type="search"]'))
        )
        account_number_box.click()
        account_number_box.send_keys(bizportal_account_number)
        time.sleep(1)
        account_number_box.send_keys(Keys.ENTER)
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
        log.info("first_page is %s", first_page)
        if not first_page:
            first_link_found = True

        if not first_link_found:
            log.debug("looking for pdf_link_1 %s", pdf_links_xpath)
            pdf_link_1 = WebDriverWait(self.driver, 15).until(
                ec.presence_of_element_located((By.XPATH, pdf_links_xpath))
            )
            log.info(
                "Downloading most recent bill; scroll to %s", pdf_link_1.location["y"]
            )
            self.driver.execute_script(
                "window.scrollTo(0," + str(pdf_link_1.location["y"]) + ")"
            )
            WebDriverWait(self.driver, 15).until(
                ec.element_to_be_clickable((By.XPATH, pdf_links_xpath))
            )
            self.driver.screenshot(BaseWebScraper.screenshot_path("most recent bill"))
            pdf_link_1.click()

            if (
                self.driver.current_url
                == "https://portlandgeneral.com/secure/view-bill"
            ):
                download_bill_button_xpath = (
                    "//span[contains(text(), 'Download bill (PDF)')]"
                )
                log.debug("scroll to scrollHeight/2")
                self.driver.execute_script(
                    "window.scrollTo(0, window.scrollY+(document.body.scrollHeight/2))"
                )
                time.sleep(2)
                log.debug("looking for download button %s", download_bill_button_xpath)
                download_bill_button = WebDriverWait(self.driver, 25).until(
                    ec.presence_of_element_located(
                        (By.XPATH, download_bill_button_xpath)
                    )
                )

                try:
                    log.debug("clicking download")
                    download_bill_button.click()
                    # div[role="alert"] with text  No bill found.
                except ElementClickInterceptedException as exc:
                    log.debug("click intercepted: %s", exc)
                    close_modal(self.driver)
                    download_bill_button.click()
                time.sleep(1)

                filename = self.driver.wait(60).until(
                    file_exists_in_dir(download_dir, r".*\.pdf$")
                )

                file_path = os.path.join(download_dir, filename)
                log.info("Processing most recent bill: %s", filename)
                single_bill = extract_bill_data(
                    file_path, service_id, utility, utility_account_id
                )

                bill_data.append(single_bill)
                log.info(
                    "first bill: %s - %s cost=%s",
                    single_bill.start,
                    single_bill.end,
                    single_bill.cost,
                )

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
        log.info("Found %s pdfs on page", len(pdf_links))
        self.driver.screenshot(BaseWebScraper.screenshot_path("found pdfs"))
        for link in pdf_links:
            if not first_link_found:
                first_link_found = True
                continue
            self.driver.execute_script(
                "window.scrollTo(0," + str(link.location["y"]) + ")"
            )
            time.sleep(2)
            if not self.seen_survey and close_survey(self.driver):
                self.seen_survey = True

            close_survey(self.driver)
            # get sibling node for date range text: 12/10/2020 - 01/12/2021
            match = re.match(
                r"(\d+/\d+/\d+) - (\d+/\d+/\d+)",
                link.find_element_by_xpath("../p").text,
            )
            from_dt = parse_time(match.group(1))
            to_dt = parse_time((match.group(2)))
            if to_dt < start:
                log.info("stoppinng: %s bill is before start", to_dt)
                break
            # filename is View_Bill-Dec. 10, 2020_Jan. 12, 2021.pdf
            filename = "View_Bill-%s_%s.pdf" % (
                from_dt.strftime("%b. %d, %Y"),
                to_dt.strftime("%b. %d, %Y"),
            )
            link.click()

            self.driver.wait(90).until(file_exists_in_dir(download_dir, filename))
            file_path = os.path.join(download_dir, filename)

            period_start, period_end = extract_bill_period(file_path)

            # If the bill starts after our end date, skip it
            if period_start > end:
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

            bill_data.append(single_bill)
            log.info(
                "added bill: %s - %s cost=%s",
                single_bill.start,
                single_bill.end,
                single_bill.cost,
            )

        non_overlapping_bills = _adjust_bill_dates(bill_data)
        return non_overlapping_bills


class AccountPage:
    def __init__(self, driver):
        self.driver = driver

    def goto_billing(self) -> BillingPage:
        self.driver.get(
            "https://portlandgeneral.com/secure/payment/billing-payment-history"
        )

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
        close_modal(self.driver)

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


class PortlandBizportalScraper(BaseWebScraper):
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
        self._driver.screenshot(BaseWebScraper.screenshot_path("after login"))

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

        self._driver.screenshot(BaseWebScraper.screenshot_path("found account"))
        log.info("Detecting page numbers")
        total_pages = billing_page.page_numbers()
        time.sleep(10)
        log.info("Detected %s pages of bills", total_pages)

        bills: List[BillingDatum] = []
        first_page = True
        for page in range(total_pages):
            log.info("Handling page %s; first_page is %s", page, first_page)
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
        PortlandBizportalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
