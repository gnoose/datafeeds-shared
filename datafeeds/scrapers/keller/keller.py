import os
import time
import logging

from .parsers import parse_bill_pdf

from glob import glob
from io import BytesIO
from collections import namedtuple
from datetime import date, datetime

from typing import Optional, List

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, adjust_bill_dates, show_bill_summary
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
import datafeeds.common.upload as bill_upload

log = logging.getLogger(__name__)


class KellerScraperException(Exception):
    pass


# This is just a more structured way of breaking the account number into it's three constituent pieces,
# which we need to handle separately during login.
KellerIdentifier = namedtuple(
    "KellerIdentifier", ["route_number", "service_address_id", "resident_number"]
)


class KellerConfiguration(Configuration):
    def __init__(self, account_number: str):
        super().__init__(scrape_bills=True)
        self.account_number = account_number


class BillHistoryPage:
    def __init__(self, driver):
        self.driver = driver

    @staticmethod
    def parse_date(text: str) -> Optional[date]:
        try:
            return datetime.strptime(text, "%m/%d/%Y").date()
        except ValueError:
            return None

    def process_downloads(self, prefix: str) -> List[bytes]:
        results = []
        paths = glob(self.driver.download_dir + ("/%s*.pdf" % prefix))
        for path in paths:
            with open(path, "rb") as f:
                data = f.read()
                results.append(data)
            os.remove(path)
        return results

    def gather_data(
        self, user_id: KellerIdentifier, start: date, end: date
    ) -> List[Optional[bytes]]:
        pdf_link_xpath = "//img[@title='Click to view bill.  You must have Adobe reader installed to view your bill.']"
        WebDriverWait(self.driver, 60).until(
            EC.presence_of_element_located((By.XPATH, pdf_link_xpath))
        )

        prefix = "_".join(user_id)

        results: list = []
        for pdf_link in self.driver.find_elements_by_xpath(pdf_link_xpath):
            bill_date_text = pdf_link.find_element_by_xpath("../../td").text
            bill_date = self.parse_date(bill_date_text)

            if bill_date and start <= bill_date <= end:
                pdf_link.click()
                time.sleep(10)  # Wait for download to complete.

                results += self.process_downloads(prefix)

        return results


class HomePage:
    def __init__(self, driver):
        self.driver = driver

    def to_bill_history(self):
        elt = WebDriverWait(self.driver, 60).until(
            EC.presence_of_element_located((By.ID, "ACTION_TO_PERFORM"))
        )
        select = Select(elt)
        select.select_by_value("PAYMENT_HISTORY")
        return BillHistoryPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, user_id: KellerIdentifier, password: str) -> HomePage:
        self.driver.get("https://payonline.cityofkeller.com/stw_php/stwub/ubtipact.php")

        # Confirm UI has loaded by looking for a username-like text field.
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.ID, "ROUTE_NUMBER"))
        ).send_keys(user_id.route_number)

        self.driver.find_element_by_id("SERVICE_ADDRESS_ID").send_keys(
            user_id.service_address_id
        )
        self.driver.find_element_by_id("RESIDENT_NUMBER").send_keys(
            user_id.resident_number
        )
        self.driver.find_element_by_xpath("//tr/td//input[@id='PASSWORD']").send_keys(
            password
        )
        self.driver.find_element_by_xpath("//input[@value='Continue']").click()

        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//center/div[@class='ErrorMsg']")
                )
            )
            raise KellerScraperException("Invalid credentials.")
        except TimeoutException:
            pass

        return HomePage(self.driver)


class KellerScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "City of Keller"
        self.login_url = "https://payonline.cityofkeller.com/stw_php/stwub/ubtipact.php"

    @property
    def account_number(self):
        return self._configuration.account_number

    @property
    def keller_id(self) -> KellerIdentifier:
        account = self.account_number
        elements = account.split("-")
        if (
            len(elements) != 3
            or len(elements[0]) != 3
            or len(elements[1]) != 7
            or len(elements[2]) != 3
        ):
            raise KellerScraperException(
                'Keller service account numbers must be of the form "XXX-XXXXXXX-XXX".'
            )

        return KellerIdentifier(elements[0], elements[1], elements[2])

    def _execute(self):
        login_page = LoginPage(self._driver)
        home_page = login_page.login(self.keller_id, self.password)
        self.screenshot("home_page")

        bill_history_page = home_page.to_bill_history()
        self.screenshot("bill_history_page")

        bills = bill_history_page.gather_data(
            self.keller_id, self.start_date, self.end_date
        )

        log.info(
            "Acquired %d bills (%s bytes total)."
            % (len(bills), sum(len(b) for b in bills))
        )

        bill_data = []
        for b in bills:
            bill_datum = parse_bill_pdf(BytesIO(b))

            if bill_datum is None:
                continue

            key = bill_upload.hash_bill_datum(self.account_number, bill_datum)
            attachment_entry = bill_upload.upload_bill(BytesIO(b), key)
            if attachment_entry:
                bill_data.append(bill_datum._replace(attachments=[attachment_entry]))
            else:
                bill_data.append(bill_datum)

        final_bills = adjust_bill_dates(bill_data)
        show_bill_summary(final_bills, "Final Bill Summary")
        return Results(bills=final_bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = KellerConfiguration(meter_id=meter.service_id)

    return run_datafeed(
        KellerScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
