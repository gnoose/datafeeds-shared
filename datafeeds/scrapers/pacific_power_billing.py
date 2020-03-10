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


class PacificPowerScraperException(Exception):
    pass


class PacificPowerConfiguration(Configuration):
    def __init__(self, account_number: str, meter_number: str):
        super().__init__(scrape_bills=True)
        self.account_number = account_number
        self.meter_number = meter_number


class BillHistoryPage:
    def __init__(self, driver):
        self.driver = driver

    def _scroll_to_top(self):
        self.driver.execute_script("window.scroll(0, 0);")  # Scroll back to top of page.

    def select_account(self, account_id: str):
        time.sleep(20)
        WebDriverWait(self.driver, 30) \
            .until(ec.invisibility_of_element_located((By.XPATH, "//p[text()='Loading...']")))

        self._scroll_to_top()
        xpath = "//div[@class='mat-select-value']"
        WebDriverWait(self.driver, 30) \
            .until(ec.element_to_be_clickable((By.XPATH, xpath))) \
            .click()

        # This account xpath is a bit odd; they append a space after the account number for some unknown reason.
        # This might be a bug in the Pacific Power site that will get fixed in the future.
        account_xpath = "//span[text() = '%s ']" % account_id
        self.driver.find_element_by_xpath(account_xpath).click()

        time.sleep(5)
        WebDriverWait(self.driver, 30) \
            .until(ec.invisibility_of_element_located((By.XPATH, "//p[text()='Loading...']")))

    def set_dates(self, start: date, end: date):
        start_str = start.strftime("%m/%d/%Y")
        end_str = end.strftime("%m/%d/%Y")

        self._scroll_to_top()
        from_input = self.driver.find_element_by_xpath("//input[@id='mat-input-1']")
        from_input.clear()
        from_input.send_keys(start_str)

        to_input = self.driver.find_element_by_xpath("//input[@id='mat-input-2']")
        to_input.clear()
        to_input.send_keys(end_str)

        self.driver.find_element_by_xpath("//span[text()='Update']/..").click()

        WebDriverWait(self.driver, 30) \
            .until(ec.invisibility_of_element_located((By.XPATH, "//p[text()='Loading Payment History details...']")))

        # Depending on the size of the date range, the results may be paginated.
        try:
            self.driver.find_element_by_xpath("//a[text()='SHOW ALL']").click()
        except NoSuchElementException:
            pass

    def _process_downloads(self) -> List[bytes]:
        results = []
        paths = glob(self.driver.download_dir + "/OnlineBill*.pdf")
        for path in paths:
            with open(path, 'rb') as f:
                data = f.read()
                results.append(data)
            os.remove(path)
        return results

    def gather_data(self) -> List[bytes]:
        self._scroll_to_top()
        bill_link = "//a[contains(text(), 'Regular Bill')]"
        links = self.driver.find_elements_by_xpath(bill_link)
        results = []

        for link in links:
            link.click()
            time.sleep(2)  # Wait for download to complete...
            results += self._process_downloads()

        return results


class HomePage:
    def __init__(self, driver):
        self.driver = driver

    def to_bill_history(self):
        self.driver.get("https://csapps.pacificpower.net/secure/my-account/billing-payment-history")
        return BillHistoryPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> HomePage:
        self.driver.get("https://csapps.pacificpower.net/idm/login")

        WebDriverWait(self.driver, 30) \
            .until(ec.presence_of_element_located((By.XPATH, "//input[@formcontrolname='username']")))\
            .send_keys(username)

        self.driver.find_element_by_xpath("//input[@formcontrolname='password']").send_keys(password)
        self.driver.find_element_by_xpath("//span[text()='Sign In']/..").click()

        try:
            WebDriverWait(self.driver, 10) \
                .until(ec.visibility_of_element_located((By.XPATH, "//mat-error")))
            raise PacificPowerScraperException("Invalid credentials.")
        except TimeoutException:
            pass

        return HomePage(self.driver)


class PacificPowerScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "Pacific Power"
        self.login_url = "https://www.pacificpower.net/index.html"

    @property
    def account_number(self):
        return self._configuration.account_number

    @property
    def meter_number(self):
        return self._configuration.meter_number

    def _execute(self):
        if self.end_date - self.start_date < timedelta(days=60):
            _log("Expanding date range to a minimum of 60 days.")
            self.start_date = self.end_date - timedelta(days=60)

        start_date = max(self.start_date, (datetime.now() - relativedelta(years=10)).date())
        end_date = min(self.end_date, (datetime.now().date()))

        _log("Final date range to %s - %s" % (start_date, end_date))

        login_page = LoginPage(self._driver)
        home_page = login_page.login(self.username, self.password)
        self.screenshot("home_screen")
        _log("Login successful.")

        bill_history_page = home_page.to_bill_history()
        self.screenshot("bill_history_page")
        _log("Loaded bill history.")

        bill_history_page.select_account(self.account_number)
        self.screenshot("account_selected")
        _log("Selected account.")

        bill_history_page.set_dates(start_date, end_date)
        self.screenshot("dates_selected")
        _log("Selected dates.")

        raw_pdfs = bill_history_page.gather_data()

        _log("PDF bills captured: %s" % len(raw_pdfs))
        _log("Net bill pdf bytes captured: %s" % (sum(len(x) for x in raw_pdfs)))

        ii = 0
        bill_data = []
        for b in raw_pdfs:
            ii += 1
            bill_datum = parse_bill_pdf(BytesIO(b), self.meter_number)

            if bill_datum is None:
                _log("There was a problem parsing a bill PDF #%d." % ii)
                continue

            key = bill_upload.hash_bill_datum(self.meter_number, bill_datum)
            attachment_entry = bill_upload.upload_bill(BytesIO(b), key)
            if attachment_entry:
                bill_data.append(bill_datum._replace(attachments=[attachment_entry]))
            else:
                bill_data.append(bill_datum)

        final_bills = adjust_bill_dates(bill_data)
        show_bill_summary(_log, final_bills, "Final Bill Summary")
        return Results(bills=final_bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = PacificPowerBillingConfiguration(meter_id=meter.service_id)

    return run_datafeed(
        PacificPowerBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
