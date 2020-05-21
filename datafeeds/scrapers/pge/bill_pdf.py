import re
import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

from dateutil import parser as date_parser

from datafeeds import config
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Results, Configuration
from datafeeds.common.typing import Status, BillPdf
from datafeeds.common.util.selenium import file_exists_in_dir

from datafeeds.common.upload import hash_bill, upload_bill_to_s3

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

from datafeeds.scrapers.pge.support import (
    wait_for_block_overlay,
    wait_for_account,
    wait_for_accounts_list,
    click,
    close_modal,
)

from selenium.common.exceptions import (
    ElementNotInteractableException,
    TimeoutException,
    ElementClickInterceptedException,
)

log = logging.getLogger(__name__)


class DashboardPage(CSSSelectorBasePageObject):
    """Dashboard page with account dropdown.

    https://m.pge.com/index.html#myaccount/dashboard/summary/account-id
    """

    BillingHistoryTableSel = "div#billingHistoryContainer"
    ViewMoreHistorySel = (
        ".pge_coc-dashboard-billed_history_24m_activity_cntrl_block"
        " a#href-view-24month-history"
    )
    PanelxSel = (
        "div.billed_history_panel"
        ".pge_coc-dashboard-viewPay_billed_summary_panel"
        ":not(.hide)"
    )
    HeaderCostPattern = r"\s(-?\$[\d,\.]+)\s"

    def visit_dashboard(self):
        self._driver.get("https://m.pge.com/#dashboard")
        wait_for_accounts_list(self._driver)

    def select_account(self, account_id: str):
        """Select account from dropdown.

        Account number is 10 digits. The dropdown includes an additional check digit: 1234567890-5
        """

        self.account_id = account_id
        target_account_xpath = f'//ul[@id="accountListItems"]/li/a[starts-with(text(), "{self.account_id}")]'

        # verify if an account actually exists in accounts dropdown list
        if not self._driver.find(selector=target_account_xpath, xpath=True):
            log.error(
                f"account with id {self.account_id} not found in accountListItems dropdown"
            )
            # raise a login error to disable login if account is not available
            raise LoginError(
                f"account with id {self.account_id} not found in accountListItems dropdown"
            )

        # get the account number with check digit from the account dropdown list
        account_id_with_check_digit = self._driver.find_element_by_xpath(
            target_account_xpath
        ).get_attribute("innerText")

        # visit dashboard for an account directly
        self._driver.get(
            f"https://m.pge.com/#myaccount/dashboard/summary/{account_id_with_check_digit}"
        )
        self.wait_until_ready(".NDB-footer-links")
        wait_for_block_overlay(self._driver)

    def download_bills(
        self,
        latest: date,
        utility_account: str,
        utility: str,
        gen_utility: Optional[str] = None,
        gen_utility_account_id: Optional[str] = None,
    ) -> List[BillPdf]:
        """Download bill PDFs for the specified date range."""
        pdfs: List[BillPdf] = []
        log.info("Opening billing history")

        click(self._driver, css_selector="#arrowBillPaymentHistory")

        self.wait_until_ready(self.BillingHistoryTableSel)
        wait_for_block_overlay(self._driver)

        log.info("Clicking 'view up to..' link")

        click(self._driver, css_selector=self.ViewMoreHistorySel)
        self.wait_until_ready(self.BillingHistoryTableSel)

        panels_count = len(self._driver.find_elements_by_css_selector(self.PanelxSel))
        log.info(f"found {panels_count} panels in billing widget")

        # Rather than get all matching elements and iterate through, use index
        # and manually get element each time to help avoid stale element errors
        for i in range(0, panels_count):
            panel = self._driver.find_elements_by_css_selector(self.PanelxSel)[i]

            # check if is a payment panel
            panel_header = panel.find_element_by_css_selector(".panel-title")
            header_text = panel_header.text
            if "Payment" in header_text:
                log.debug(f"Skipping panel {i} (payment)")
                # skip if is a payment panel
                continue

            log.debug(f"Processing panel {i} (bill)")

            link_elem = panel.find_element_by_css_selector(
                "div.pge_coc-dashboard-viewPay_billed_history_panel_viewBill_para_block"
                " a.viewBill"
            )
            # Get date from the "data-date" attribute on link to download bill...
            # data-date is in milliseconds
            timestamp = int(link_elem.get_attribute("data-date")) / 1000.0

            # when bill was issued
            bill_date = datetime.fromtimestamp(timestamp).date()
            # bill issued about a week after end date; use this window to match dates
            approx_bill_end = bill_date - timedelta(days=7)
            approx_bill_start = approx_bill_end - timedelta(days=30)

            cost = re.search(self.HeaderCostPattern, header_text).group(1)
            # cost with $ and commas: $1,234.56 or -$1,234.56
            cost = float(cost.replace("$", "").replace(",", ""))

            log.info(f"Found bill issued {bill_date} with cost ${cost}")

            if approx_bill_end <= latest:
                log.info(f"ignoring bill, date: {approx_bill_end} already download")
                continue

            try:
                click(self._driver, elem=link_elem)
            except ElementNotInteractableException:
                log.info("Download link not visible; looking for other")

                link_elem = panel.find_element_by_css_selector(
                    "div#billSummaryContainer a.viewBill"
                )

                click(self._driver, elem=link_elem)
            except ElementClickInterceptedException as exc:
                log.info("download link failed: %s %s", exc, exc.msg)
                close_modal(self._driver)
                continue

            last4 = self.account_id.split("-")[0][6:10]
            filename = f"{last4}custbill{bill_date.strftime('%m%d%Y')}.pdf"
            download_dir = "%s/current" % config.WORKING_DIRECTORY

            try:
                self._driver.wait(30).until(
                    file_exists_in_dir(
                        # end pattern with $ to prevent matching filename.crdownload
                        directory=download_dir,
                        pattern=f"^{filename}$",
                    )
                )
            except TimeoutException:
                log.error(f"ERROR waiting for file {filename} to download...skipping")
                # close the download failed modal if there is one
                close_modal(self._driver)
                continue

            with open("%s/%s" % (download_dir, filename), "rb") as f:
                key = hash_bill(
                    self.account_id, approx_bill_start, approx_bill_end, cost, "", ""
                )

                upload_bill_to_s3(
                    file_handle=f,
                    key=key,
                    source="pge.com",
                    statement=bill_date,
                    utility=utility,
                    utility_account_id=utility_account,
                    gen_utility=gen_utility,
                    gen_utility_account_id=gen_utility_account_id,
                )

            log.info(f"Uploaded {filename} to {key}")
            pdfs.append(
                BillPdf(
                    utility_account_id=utility_account,
                    gen_utility_account_id=gen_utility,
                    start=approx_bill_start,
                    end=approx_bill_end,
                    s3_key=key,
                )
            )

        return pdfs


class LoginPage(CSSSelectorBasePageObject):
    UsernameFieldSelector = 'input[name="username"]'
    PasswordFieldSelector = 'input[name="password"]'
    SigninButtonSelector = 'button[id="home_login_submit"]'

    def login(self, username: str, password: str):
        """Authenticate with the web page https://www.pge.com/

        Fill in the username, password, then click "Sign in"
        """

        # wait for and click accept cookies
        try:
            # sometimes the log in button is intercepted by the accept cookies dialog
            # so make sure to click the accept cookies button before continuing
            log.info("waiting for accept cookies dialog")
            self.wait_until_ready("#onetrust-accept-btn-handler", seconds=30)
            self._driver.sleep(1)  # wait for cookies dialog animation
            log.info("clicking Accept All Cookies")
            click(self._driver, css_selector="#onetrust-accept-btn-handler")
        except TimeoutException:
            log.info(f"couldn't click accept cookies, continuing")

        self.wait_until_ready(self.UsernameFieldSelector)

        log.info("Filling in form")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)

        try:
            click(self._driver, css_selector=self.SigninButtonSelector)
            log.info("waiting for login to complete")
            wait_for_account(self._driver)
        except TimeoutException as exc:
            log.info(f"error logging in...")
            if self.shows_outage():
                log.info("Shows outage page")
                self.click_through_outage()
            else:
                msg = self.get_error_msg()
                # if it's a known login error, raise a LoginError; this will disable the login
                if msg:
                    log.error(f"error logging in: {msg}")
                    raise LoginError(msg)
                # otherwise raise the TimeoutError; don't want to disable the login
                raise exc

    def shows_outage(self) -> bool:
        outage_header_xpath = "//h1[contains(@class, 'pgeOutage')]"
        return len(self._driver.find_elements_by_xpath(outage_header_xpath)) > 0

    def click_through_outage(self):
        # Goes from "outage" page to main account dashboard
        click(self._driver, css_selector="a[title='Residential - Your Account']")
        click(
            self._driver,
            css_selector="a[title='Account Overview - View Your Account Dashboard']",
        )

        wait_for_account(self._driver)

    def get_error_msg(self) -> str:
        # There are a few(known) possible problems with logging in, so let's
        # disambiguate the login error a little bit...

        msg = None
        # Note: These are the actual error strings returned, so they are case - sensitive
        account_disabled = "Account temporarily disabled"
        invalid_credentials = "Invalid Username or Password"

        def has_no_accounts() -> bool:
            # Some logins will actually not have any accounts linked at all
            # and do not show any available
            na_text_xpath = (
                "//div[contains(text(), "
                "'You do not have any accounts linked to your username')]"
            )
            na_form_xpath = "//input[@id='cyai-accountNumber']"

            # Look for both "no accounts" message as well as account form,
            # because there's a chance (like on the account linking page)
            # there *could* be accounts and might not show the form
            return (
                len(self._driver.find_elements_by_xpath(na_text_xpath)) > 0
                and len(self._driver.find_elements_by_xpath(na_form_xpath)) > 0
            )

        def has_login_error(text) -> bool:
            # Failed logins will display a message of some sort
            sel = "//p[@class='login-error-msg' and contains(text(), '" + text + "')]"
            return len(self._driver.find_elements_by_xpath(sel)) > 0

        if has_login_error(account_disabled):
            msg = account_disabled
        elif has_login_error(invalid_credentials):
            msg = invalid_credentials
        elif has_no_accounts():
            msg = "No accounts for login"

        return msg


class PgeBillPdfScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "PGE Bill PDF"
        self.login_url = "https://www.pge.com"

    def _execute(self):
        # Direct the driver to the login page
        log.info("Navigating to PG&E")
        self._driver.get(self.login_url)

        # Create page helpers
        login_page = LoginPage(self._driver)
        dashboard_page = DashboardPage(self._driver)

        # Log in
        self.screenshot("before login")
        login_page.login(self.username, self.password)
        self.screenshot("after login")
        log.info("Successfully logged in")

        log.info("Visiting main dashboard")
        dashboard_page.visit_dashboard()

        # select account
        log.info(f"Visiting account summary for {self._configuration.utility_account}")
        dashboard_page.select_account(self._configuration.utility_account)
        self.screenshot("after select account")

        # get latest statement date already retrieved
        datasource = self._configuration.datasource
        latest = date_parser.parse(datasource.meta.get("latest", "2010-01-01")).date()
        # download bills
        pdfs = dashboard_page.download_bills(
            latest, self._configuration.utility_account, self._configuration.utility,
        )
        # set latest statement date
        if pdfs:
            latest_download = max([pdf.end for pdf in pdfs])
            datasource.meta["latest"] = latest_download.strftime("%Y-%m-%d")
        return Results(pdfs=pdfs)


class PgeBillPdfConfiguration(Configuration):
    def __init__(
        self,
        utility: str,
        utility_account: str,
        gen_utility: Optional[str],
        gen_utility_account_id: Optional[str],
        datasource: MeterDataSource,
    ):
        super().__init__(scrape_pdfs=True)
        self.utility_account = utility_account
        self.utility = utility
        self.gen_utility = gen_utility
        self.gen_utility_account_id = gen_utility_account_id
        self.datasource = datasource


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    utility_service = meter.utility_service
    configuration = PgeBillPdfConfiguration(
        utility_service.utility,
        utility_service.utility_account_id,
        utility_service.gen_utility,
        utility_service.gen_utility_account_id,
        datasource,
    )

    return run_datafeed(
        PgeBillPdfScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
        notify_on_login_error=False,
    )
