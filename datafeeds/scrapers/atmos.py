import logging

from typing import Optional, Tuple, List, Dict, Callable

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class AtmosScraperException(Exception):
    pass


class AtmosConfiguration(Configuration):
    def __init__(self, service_account: str, meter_serial: str):
        super().__init__(scrape_bills=True)
        self.service_account = service_account
        self.meter_serial = meter_serial


class BillHistoryPage:
    def __init__(self, driver):
        self.driver = driver

    def set_dates(self, start_date: date, end_date: date):
        """Request the maximum 23 months of billing history."""

        start = max(date.today() - relativedelta(months=23), start_date)
        end = min(date.today(), end_date)

        start_month = start.strftime("%B")
        start_year = start.year
        start_month_xpath = "//select[@name='startMonth']/option[@value='%s']" % start_month
        start_year_xpath = "//select[@name='startYear']/option[@value='%s']" % start_year

        end_month = end.strftime("%B")
        end_year = end.year
        end_month_xpath = "//select[@name='endMonth']/option[@value='%s']" % end_month
        end_year_xpath = "//select[@name='endYear']/option[@value='%s']" % end_year

        _log("Final scraping time interval: %s, %s - %s, %s" % (start_month, start_year, end_month, end_year))

        search_button = WebDriverWait(self.driver, 5).until(ec.presence_of_element_located((By.ID, "search")))

        self.driver.find_element_by_xpath(start_month_xpath).click()
        self.driver.find_element_by_xpath(start_year_xpath).click()
        self.driver.find_element_by_xpath(end_month_xpath).click()
        self.driver.find_element_by_xpath(end_year_xpath).click()

        search_button.click()

        view_all_link_xpath = "//div[@id='bilTab']//a[@id='viewAll']"
        WebDriverWait(self.driver, 5).until(ec.element_to_be_clickable((By.XPATH, view_all_link_xpath))).click()

    def gather_data(self) -> List[Tuple[Optional[bytes], Optional[bytes]]]:
        pdf_links = self.driver.find_elements_by_xpath("//a[text()='View Bills']")
        xls_links = self.driver.find_elements_by_xpath("//a[text()='Download Excel']")

        pdf_path = os.path.join(self.driver.download_dir, "my.pdf")
        xls_path = os.path.join(self.driver.download_dir, "invoice.xls")

        results = []
        for pdf_link, xls_link in zip_longest(pdf_links, xls_links):
            pdf_data = xls_data = None

            if pdf_link is not None:
                pdf_link.click()
                time.sleep(2)  # Wait for download to complete.
                if os.path.exists(pdf_path):
                    with open(pdf_path, 'rb') as f:
                        pdf_data = f.read()
                    os.remove(pdf_path)

            if len(self.driver.window_handles) > 1:
                # Close the bill PDF download popup.
                self.driver.switch_to.window(window_name=self.driver.window_handles[1])
                self.driver.close()
                self.driver.switch_to.window(window_name=self.driver.window_handles[0])

            if xls_link is not None:
                xls_link.click()
                time.sleep(2)  # Wait for download to complete.
                if os.path.exists(xls_path):
                    with open(xls_path, 'rb') as f:
                        xls_data = f.read()
                    os.remove(xls_path)

            results.append((pdf_data, xls_data))

        return results


class HomePage:
    def __init__(self, driver):
        self.driver = driver

    def to_bill_history(self):
        self.driver.get("https://www.atmosenergy.com/accountcenter/finance/FinancialTransaction.html?activeTab=2")
        return BillHistoryPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> HomePage:
        self.driver.get("https://www.atmosenergy.com/accountcenter/logon/login.html")

        WebDriverWait(self.driver, 5).until(ec.presence_of_element_located((By.ID, "username")))

        self.driver.find_element_by_id("username").send_keys(username)
        self.driver.find_element_by_id("password").send_keys(password)
        self.driver.find_element_by_xpath("//input[@value='Login']").click()

        try:
            WebDriverWait(self.driver, 5) \
                .until(ec.presence_of_element_located((By.XPATH, "//ul[@class='errorMessage']")))
            raise AtmosScraperException("Invalid credentials.")
        except TimeoutException:
            pass

        site_failure_page = "https://www.atmosenergy.com/accountcenter/successerror/successErrorMessage.html"
        if self.driver.current_url == site_failure_page:
            raise AtmosScraperException("Atmos site is down.")

        return HomePage(self.driver)


class AtmosScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = 'Chrome'
        self.name = 'Atmos'
        self.login_url = ''

    @property
    def service_account(self):
        return self._configuration.service_account

    @property
    def meter_serial(self):
        return self._configuration.meter_serial

    def _execute(self):
        login_page = LoginPage(self._driver)
        home_page = login_page.login(self.username, self.password)
        self.screenshot("home_page")
        bill_history_page = home_page.to_bill_history()
        bill_history_page.set_dates(self.start_date, self.end_date)
        self.screenshot("bill_history")

        history = bill_history_page.gather_data()

        pdf_bytes = sum(len(t[0]) for t in history if t[0])
        xls_bytes = sum(len(t[1]) for t in history if t[1])
        pdfs = sum(1 for t in history if t[0])
        xls = sum(1 for t in history if t[1])
        _log("Acquired %s pdfs (%s bytes) and %s excel files (%s bytes)." % (pdfs, pdf_bytes, xls, xls_bytes))

        bills = []
        for pdf, xls in history:

            bill_data = []
            if xls is not None:
                bill_data = bill_data_from_xls(xls, self.service_account)
            elif pdf is not None:
                bill_data = bill_data_from_pdf(pdf, self.service_account, self.meter_serial)

            if pdf is not None and bill_data:
                bill_data_prime = []
                for bill_datum in bill_data:
                    key = bill_upload.hash_bill_datum(self.service_account, bill_datum)
                    attachment_entry = bill_upload.upload_bill(BytesIO(pdf), key)
                    if attachment_entry:
                        bill_data_prime.append(bill_datum._replace(attachments=[attachment_entry]))
                    else:
                        bill_data_prime.append(bill_datum)
                bill_data = bill_data_prime

            if bill_data:
                bills += bill_data

        _log_bill_summary(bills, title="Final Bill Summary")

        final_bills = adjust_bill_dates(bills)
        return Results(bills=final_bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = AtmosConfiguration(meter_id=meter.service_id)

    return run_datafeed(
        AtmosScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
