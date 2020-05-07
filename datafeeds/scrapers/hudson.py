import os
import time
import logging

from io import BytesIO
from glob import glob

from datetime import date, datetime, timedelta

from typing import Optional, Tuple, List

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, adjust_bill_dates, BillingDatum
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
import datafeeds.common.upload as bill_upload

log = logging.getLogger(__name__)


class HudsonScraperException(Exception):
    pass


class HudsonConfiguration(Configuration):
    def __init__(self, utility: str, account_number: str, meter_number: str):
        super().__init__(scrape_bills=True)
        self.account_number = account_number
        self.meter_number = meter_number
        self.utility = utility


class BillHistoryPage:
    def __init__(self, driver):
        self.driver = driver

    def _capture_current_rows(
        self, search_start: date, search_end: date
    ) -> List[Tuple[BillingDatum, Optional[bytes]]]:
        """Capture all of the billing data presented in the on-screen table."""
        results = []
        rows = self.driver.find_elements_by_xpath("//tbody/tr")

        for row in rows:
            log.info("Acquiring bill data.")
            period = row.find_element_by_xpath("./td[3]").text
            parts = period.split(" - ")

            if len(parts) != 2:
                continue

            start = datetime.strptime(parts[0], "%m/%d/%Y").date()
            end = datetime.strptime(parts[1], "%m/%d/%Y").date()

            if start < search_start or search_end < end:
                continue  # This bill is irrelevant to the current scraping run.

            charge = row.find_element_by_xpath("./td[5]").text
            cost = float(charge.replace("$", "").replace(",", ""))
            used = float(row.find_element_by_xpath("./td[9]").text)

            log.info("Downloading PDF.")
            pdf_link = row.find_element_by_xpath(".//a")
            statement = datetime.strptime(pdf_link.text, "%m/%d/%Y").date()
            pdf_link.click()

            # PDFs look like <UUID>.pdf
            time.sleep(5)  # Wait for PDF to fully download.

            pdf_data = None
            pdfs = glob(os.path.join(self.driver.download_dir, "*.pdf"))
            if len(pdfs) == 1:
                with open(pdfs[0], "rb") as f:
                    pdf_data = f.read()
                    log.info("Acquired PDF from %s." % pdfs[0])
                os.remove(pdfs[0])

            bill = BillingDatum(
                start=start,
                end=end,
                statement=statement,
                cost=cost,
                used=used,
                peak=None,
                items=None,
                attachments=None,
            )
            results.append((bill, pdf_data))

        if not results:
            log.info("No billing history was present.")

        return results

    def gather_data(
        self, start: date, end: date
    ) -> List[Tuple[BillingDatum, Optional[bytes]]]:
        """Return a list of billing data together with the associated PDF data."""

        results: list = []
        while True:
            results += self._capture_current_rows(start, end)

            try:
                self.driver.find_element_by_xpath("//button[text()='Next']").click()
            except WebDriverException:
                break

        return results


class HomePage:
    def __init__(self, driver):
        self.driver = driver

    def select_account(self, account_number: str) -> BillHistoryPage:
        search_button_xpath = "//a[@class='search-acc-box-link']"
        WebDriverWait(self.driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, search_button_xpath))
        ).click()

        # Unfortunately, the Angular frontend for this utility does not provide a very clear way to know
        # the page is ready for interaction. In particular, the click-ability of DOM elements only partly
        # captures whether the page is ready.
        #
        # For this reason, we need to add manual wait times when loading the bill history.
        # Fortunately, we only need to do this once at the beginning, not on a per-bill basis.
        time.sleep(10)

        search_textbox_xpath = "//div[@class='search-acc']//input"
        textbox = WebDriverWait(self.driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, search_textbox_xpath))
        )
        textbox.send_keys(account_number)

        time.sleep(10)

        select_button_xpath = "//button[text()='Select']"
        account_number_xpath = "//span[text()='%s']" % account_number
        try:
            button = WebDriverWait(self.driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, select_button_xpath))
            )
            button.click()

            time.sleep(10)

            # Confirm that we selected the correct account before proceeding.
            WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.XPATH, account_number_xpath))
            )
        except TimeoutException:
            raise HudsonScraperException(
                "Account number %s not found for this login." % account_number
            )

        self.driver.get("https://account.hudsonenergy.net/Billing/BillingHistory")

        time.sleep(10)

        return BillHistoryPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> HomePage:
        self.driver.get("https://account.hudsonenergy.net/Home")

        WebDriverWait(self.driver, 5).until(
            EC.element_to_be_clickable((By.ID, "btnLogin"))
        )

        self.driver.find_element_by_id("username").send_keys(username)
        self.driver.find_element_by_id("password").send_keys(password)
        self.driver.find_element_by_id("btnLogin").click()

        try:
            error_xpath = "//p[text() = 'Username or Password is incorrect.']"
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.XPATH, error_xpath))
            )
            raise HudsonScraperException("Invalid credentials.")
        except TimeoutException:
            pass

        return HomePage(self.driver)


class HudsonScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "Hudson"
        self.login_url = ""

    @property
    def account_number(self):
        return self._configuration.account_number

    @property
    def meter_number(self):
        return self._configuration.meter_number

    @property
    def utility(self):
        return self._configuration.utility

    def _execute(self):
        if self.end_date - self.start_date < timedelta(days=90):
            self.start_date = self.end_date - timedelta(days=90)
            log.info(
                "Initial time window was too narrow for this utility. Expanding time window to: %s - %s"
                % (self.start_date, self.end_date)
            )

        login_page = LoginPage(self._driver)
        home_page = login_page.login(self.username, self.password)

        log.info("Login successful. Loading bill history.")
        self.screenshot("post_login")
        bill_history_page = home_page.select_account(self.account_number)

        log.info("Loaded bill history page.")
        self.screenshot("bill_history")
        results = bill_history_page.gather_data(self.start_date, self.end_date)

        log.info(
            "Obtained %s bill records and %s PDFs."
            % (len(results), sum(1 for _, f in results if f is not None))
        )

        bills = []
        for bd, pdf_bytes in results:
            if pdf_bytes is None:
                bills.append(bd)
                continue

            key = bill_upload.hash_bill_datum(self.account_number, bd)
            attachment_entry = bill_upload.upload_bill_to_s3(
                BytesIO(pdf_bytes),
                key,
                statement=bd.statement,
                source="hudsonenergy.net",
                utility=self.utility,
                utility_account_id=self.account_number,
            )
            if attachment_entry:
                bills.append(bd._replace(attachments=[attachment_entry]))
            else:
                bills.append(bd)

        final_bills = adjust_bill_dates(bills)
        return Results(bills=final_bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = HudsonConfiguration(
        meter.utility_service.utility,
        meter.utility_service.utility_account_id,
        meter.service_id,
    )

    return run_datafeed(
        HudsonScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
