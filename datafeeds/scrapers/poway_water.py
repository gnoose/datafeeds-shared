import os
import re
import logging

from glob import glob

from selenium.webdriver.remote.webelement import WebElement

from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.exceptions import (
    LoginError,
    DataSourceConfigurationError,
    InvalidMeterDataException,
)
from datetime import timedelta, date
from io import BytesIO
from typing import Optional, List

from dateutil.parser import parse as parse_date

from datafeeds import config
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.batch import run_datafeed
from datafeeds.common.upload import hash_bill, upload_bill_to_s3
from datafeeds.parsers import pdfparser

from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, BillingDatum
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class PowayWaterConfiguration(Configuration):
    def __init__(self, account_id: str):
        super().__init__(scrape_bills=True)
        self.account_id = account_id


class LoginPage(CSSSelectorBasePageObject):
    UsernameFieldSelector = "input#dnn_dnnLogin_username"
    PasswordFieldSelector = "input#passworddnnLogin"
    SigninButtonSelector = 'button[type="submit"]'
    ErrorMessage = 'label[class="error"]'

    def login(self, username: str, password: str):
        """Authenticate with the web page."""

        log.info("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.find_element(self.SigninButtonSelector).click()
        self._driver.sleep(5)

        if self.element_exists(self.ErrorMessage):
            error_message = self.find_element(self.ErrorMessage).text
            raise LoginError(error_message)


class AccountOverviewPage(CSSSelectorBasePageObject):
    BillingHistoryLinkSelector = 'a[href="/my-account/billing"]'

    def goto_billing_history(self):
        self.wait_until_ready(self.BillingHistoryLinkSelector)
        self.find_element(self.BillingHistoryLinkSelector).click()


class BillingHistoryPage(CSSSelectorBasePageObject):
    DataTableSelector = "table.table"
    AccountsDropdownSelector = "div.aus-account-selector.dropdown a.dropdown-toggle"

    def select_account(self, account_id: str):
        self.wait_until_ready(self.AccountsDropdownSelector)
        self.wait_until_ready(
            f"{self.DataTableSelector} tr"
        )  # wait for table to populate

        self.find_element(self.AccountsDropdownSelector).click()  # open dropdown
        self._driver.sleep(1)

        account_to_click = self.find_account_in_dropdown(account_id)
        account_to_click.click()
        self._driver.sleep(1)

        self.wait_until_ready(
            f"{self.DataTableSelector} tr"
        )  # wait for table to populate

        accounts_per_page_dropdown = self._driver.get_select(
            "div.dataTables_length select"
        )
        accounts_per_page_dropdown.select_by_value("50")
        self._driver.sleep(5)

    def find_account_in_dropdown(self, account_id: str) -> WebElement:
        accounts_list = self._driver.find_all("#ul-list-acccount li[account]")
        account_to_click = None
        for account_element in accounts_list:
            if account_id not in account_element.get_attribute("account"):
                continue
            account_to_click = account_element
            break
        if not account_to_click:
            raise DataSourceConfigurationError(
                f"specified account ({account_id}) not found in accounts list"
            )
        return account_to_click

    def patch_view_ebill_javascript_callback(self):
        """Override the View eBill link callback to allow downloading pdfs.

        The following script overrides the viewElectronicBill global function to make it possible to download pdfs.
        The default function opened the pdf inside an iframe in a new blank page tab (about:blank), and selenium isn't able to
        interact with the about:blank page.
        This looks like a bug in chromium: https://monorail-prod.appspot.com/p/chromedriver/issues/detail?id=1895).

        The following script just replaces the part where it opened the pdf in a new tab with "window.location = data;"
        """
        self._driver.execute_script(
            """
            viewElectronicBill = (e) => {
                var billImage = e.value;
                getData("billinghistory/GetBillImage?billImageId=" + e.value, function (data) {
                    if (data == null || data.trim().length === 0) {
                        HandleBillError(resourceSettings.ViewBillError.value);
                        return;
                    }

                    // open the pdf in the current window ( downloads the pdf file )
                    window.location = data;
                    setTimeout(() => {
                        var x = getData(`billinghistory/DeleteBill?billImageId=${billImage}`)
                    }, 1000);
                });
            };

            // update onClick callback for all "View" links
            $("td > a[id^=view]")
                .off('click')
                .click((e)=>viewElectronicBill(e.target))
                .removeAttr("target")
                .attr("href", "#view0")
        """
        )

    def download_pdfs(self, start_date: date, end_date: date):
        download_dir = self._driver.download_dir
        downloaded_pdfs_dir = os.path.join(download_dir, "downloaded")
        os.makedirs(downloaded_pdfs_dir, exist_ok=True)

        data_table = self.find_element(self.DataTableSelector)
        data_rows = data_table.find_elements_by_css_selector("tbody > tr")

        for row in data_rows:
            bill_date = row.find_element_by_css_selector(
                'td[data-title="Bill Date"]'
            ).text

            bill_date = parse_date(bill_date).date()

            if start_date < bill_date < end_date:
                view_bill_link = row.find_element_by_css_selector(
                    'td[data-title="View eBill"] > a'
                )
                view_bill_link.click()
                filename = self._driver.wait().until(
                    file_exists_in_dir(download_dir, r".*\.pdf$")
                )
                file_path = os.path.join(download_dir, filename)
                new_file_path = os.path.join(downloaded_pdfs_dir, filename)
                os.rename(file_path, new_file_path)
                log.info(f"file downloaded: {new_file_path}")


def parse_poway_pdf(pdf_filename: str, account_id: str) -> BillingDatum:
    text = pdfparser.pdf_to_str(pdf_filename)

    used_pattern = r"Consumption (?P<units_used>[\d\.,]+) @"
    cost_pattern = r"(?P<water_charges>[\d\.,]+)\s+WATERBasic Service @"

    # date format: m/d/yyyy
    date_pattern = r"\d{1,2}\/\d{1,2}\/\d{4}"
    dates_pattern = (
        r"Total Current Charges.+?"
        fr"(?P<read_date_start>{date_pattern}) - (?P<read_date_end>{date_pattern})"
        fr"(?P<due_date>{date_pattern})"
        fr"(?P<statement_date>{date_pattern})"
    )

    dates_match = re.search(dates_pattern, text)
    if not dates_match:
        raise InvalidMeterDataException(f"Couldn't parse dates from pdf: {text}")

    _dates = dates_match.group("read_date_start", "read_date_end", "statement_date")
    start_date, end_date, statement_date = [
        parse_date(_date).date() for _date in _dates
    ]

    used_match = re.search(used_pattern, text)
    if not used_match:
        raise InvalidMeterDataException("fCouldn't parse usage from pdf: {text}")

    used_text = used_match.group("units_used")
    used = float(used_text.replace(",", "").replace("$", ""))

    cost_match = re.search(cost_pattern, text)
    if not cost_match:
        raise InvalidMeterDataException(f"Couldn't parse cost from pdf: {text}")

    cost_text = cost_match.group("water_charges")
    cost = float(cost_text.replace(",", "").replace("$", ""))

    if config.enabled("S3_BILL_UPLOAD"):
        key = hash_bill(account_id, start_date, end_date, cost, 0, used)
        with open(pdf_filename, "rb") as pdf_data:
            attachments = [
                upload_bill_to_s3(
                    BytesIO(pdf_data.read()),
                    key,
                    source="customerconnect.poway.org",
                    statement=statement_date,
                    utility="utility:city-of-poway",
                    utility_account_id=account_id,
                )
            ]
    else:
        attachments = []
    return BillingDatum(
        start=start_date,
        end=end_date - timedelta(days=1),
        statement=statement_date,
        cost=cost,
        peak=None,
        used=used,
        items=None,
        attachments=attachments,
        utility_code=None,
    )


class PowayWaterScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "City of Poway Water"

    def parse_pdfs(self) -> List[BillingDatum]:
        return [
            parse_poway_pdf(filename, self._configuration.account_id)
            for filename in glob(f"{config.WORKING_DIRECTORY}/current/downloaded/*.pdf")
        ]

    def _execute(self):
        self._driver.get("https://customerconnect.poway.org/my-account/billing")
        login_page = LoginPage(self._driver)
        self.screenshot("before login")
        login_page.login(self.username, self.password)
        self.screenshot("after login")
        account_overview_page = AccountOverviewPage(self._driver)
        account_overview_page.goto_billing_history()
        billing_page = BillingHistoryPage(self._driver)
        billing_page.select_account(self._configuration.account_id)
        self.screenshot("billing page")
        billing_page.patch_view_ebill_javascript_callback()
        billing_page.download_pdfs(self.start_date, self.end_date)
        return Results(bills=self.parse_pdfs())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = PowayWaterConfiguration(account_id=meter.utility_account_id)

    return run_datafeed(
        PowayWaterScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
