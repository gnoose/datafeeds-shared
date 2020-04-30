from datetime import date
import logging
from typing import Optional, Tuple, List


from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, BillingDatum
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers.svp.pdf_parser import process_pdf


log = logging.getLogger(__name__)


class SVPBillingConfiguration(Configuration):
    def __init__(self, utility_account_id: str, service_id: str):
        super().__init__(scrape_bills=True)
        self.utility_account_id = utility_account_id
        self.service_id = service_id


class ViewBillPage:
    def __init__(self, driver):
        self.driver = driver

    def download_bills(self, service_id: str) -> List[Tuple[date, str]]:
        bills: List[Tuple[date, str]] = []
        """
        - wait for //div[text()="View Bill"]
        - look for date included in self.start_date - self.end_date in select[id="billDate"] option
        - extract transDate from option value =
            {"transDate": "2020-04-15", "jourNo": "27823", "dueDate": "2020-05-06",
             "formattedDueDate": "05-06-2020", "formattedTransDate": "04-15-2020",
             "jourCode": "BJ"}
        - for each matching bill
          - go to https://mua.santaclaraca.gov/CC/connect/users/GetInfoSendBill?billDate=transDate
          - download PDF to config.WORKING_DIRECTORY/current
          - return transDate, filename
        """
        return bills


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> ViewBillPage:
        self.driver.get("https://mua.santaclaraca.gov/CC/Login.xml")

        form_selector = "//form[@name='userAuthentication'] "
        self.driver.find_element_by_xpath(
            form_selector + "//input[@name='username']"
        ).send_keys(username)
        self.driver.find_element_by_xpath(
            form_selector + "//input[@name='password']"
        ).send_keys(password)
        self.driver.find_element_by_id("submit").click()

        # TODO: wait for //a[text()="Sign out"]

        return ViewBillPage(self.driver)


class SVPBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SVPBilling"

    @property
    def utility_account_id(self):
        return self._configuration.utility_account_id

    @property
    def service_id(self):
        return self._configuration.service_id

    def _execute(self):
        login_page = LoginPage(self._driver)
        bill_page = login_page.login(self.username, self.password)

        log.info("Login successful. Loading bill history.")
        self.screenshot("post_login")
        results = bill_page.download_bills(self.service_id)
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
    configuration = SVPBillingConfiguration(meter.utility_account_id, meter.service_id)

    return run_datafeed(
        SVPBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
