import os
import logging

from typing import Optional, Tuple, List
from datetime import date

from dateutil.parser import parse as parse_date

from datafeeds import config
from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, BillingDatum
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers.svp.pdf_parser import process_pdf

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

log = logging.getLogger(__name__)


class SVPBillingConfiguration(Configuration):
    def __init__(self, service_id: str):
        super().__init__(scrape_bills=True)
        self.service_id = service_id


class ViewBillPage:
    def __init__(self, driver):
        self.driver = driver

    def get_available_bill_dates(self) -> List[date]:
        """Get all bill dates available.in the "Bill Date" dropdown"""
        available_bill_dates = []

        bill_date_dropdown = self.driver.get_select("select#billDate")

        # skip first option: "Choose A Date"
        for option in bill_date_dropdown.options[1:]:
            available_bill_dates.append(parse_date(option.text).date())

        return available_bill_dates

    def wait_for_bill_download(self, bill_date):
        """
        wait for document.pdf to download
        Returns absolute path of the downloaded file
        """

        download_dir = config.WORKING_DIRECTORY + "/current"
        try:
            filename = self.driver.wait(30).until(
                file_exists_in_dir(directory=download_dir, pattern=r"^document.pdf$",)
            )
        except Exception:
            raise Exception(f"Unable to download file...")

        curr_filepath = os.path.join(download_dir, filename)

        # rename the file to avoid matching the wrong file in future
        filepath = os.path.join(download_dir, f"{bill_date}_bill.pdf")
        os.rename(curr_filepath, filepath)

        return filepath

    def download_bills(
        self, start_date: date, end_date: date
    ) -> List[Tuple[date, str]]:
        """
        Download all bills pdf in the range given by start_date and end_date (inclusive)
        Returns: list of (transDate,filepath) for all downloaded bills
        """

        bills: List[Tuple[date, str]] = []

        # wait for "View Bill" section to load
        self.driver.wait().until(
            EC.presence_of_element_located((By.XPATH, '//div[text()="View Bill"]'))
        )

        # loop through dates available in the Bill Date dropdown
        for _date in self.get_available_bill_dates():
            # skip if the date isn't in the specified range
            if not (start_date <= _date <= end_date):
                continue

            transDate = _date.strftime("%Y-%m-%d")
            log.info(f"downloading bill for {transDate}")

            download_url = f"https://mua.santaclaraca.gov/CC/connect/users/GetInfoSendBill?billDate={transDate}"
            self.driver.get(download_url)

            filepath = self.wait_for_bill_download(transDate)
            log.info(f"Download Complete")

            bills.append((transDate, filepath))

        return bills


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> ViewBillPage:
        self.driver.get("https://mua.santaclaraca.gov/CC/Login.xml")

        form = self.driver.find_element_by_xpath("//form[@name='userAuthentication']")

        form.find_element_by_xpath("//input[@name='username']").send_keys(username)
        form.find_element_by_xpath("//input[@name='password']").send_keys(password)
        form.find_element_by_id("submit").click()

        self.driver.wait().until(
            EC.presence_of_element_located((By.XPATH, '//a[text()="Sign out"]'))
        )

        return ViewBillPage(self.driver)


class SVPBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SVPBilling"

    @property
    def service_id(self):
        return self._configuration.service_id

    def _execute(self):
        login_page = LoginPage(self._driver)
        bill_page = login_page.login(self.username, self.password)

        log.info("Login successful. Loading bill history.")
        self.screenshot("post_login")
        results = bill_page.download_bills(self.start_date, self.end_date)
        log.info("Obtained %s bill PDF files." % (len(results)))

        bills: List[BillingDatum] = [
            process_pdf(self.service_id, statement_dt, filename)
            for (statement_dt, filename) in results
        ]

        return Results(bills=bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SVPBillingConfiguration(meter.service_id)

    return run_datafeed(
        SVPBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
