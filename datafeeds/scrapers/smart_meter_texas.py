import time
import logging
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

from dateutil.parser import parse as dateparser
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

from datafeeds.common.batch import run_datafeed
from datafeeds.common.timeline import Timeline
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration
from datafeeds.common.support import Results
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

logger = None
log = logging.getLogger(__name__)


class SmartMeterTexasConfiguration(Configuration):
    def __init__(self, esiid):
        super().__init__(scrape_readings=True)  # SMT only provides interval readings.
        self.esiid = esiid  # An ESIID identifies a single meter, similar to Service ID.


class SmartMeterTexasScraperException(Exception):
    pass


class ViewEnergyDataPage:
    start_xpath = "//input[@id='startdatefield']"
    end_xpath = "//input[@id='enddatefield']"
    update_report_selector = "button.btn.updreport-button"

    def __init__(self, driver):
        self.driver = driver

    def error_message_visible(self) -> bool:
        error_message_xpath = "div.errorMessage.alert"
        try:
            WebDriverWait(self.driver, 1).until(
                ec.visibility_of_element_located((By.CSS_SELECTOR, error_message_xpath))
            )
            return True
        except TimeoutException:
            return False

    def _set_date(self, when: date, xpath: str):
        text_box = WebDriverWait(self.driver, 3).until(
            ec.visibility_of_element_located((By.XPATH, xpath))
        )
        date_str = when.strftime("%m/%d/%Y")
        text_box.click()
        text_box.send_keys(Keys.BACKSPACE * 10)
        text_box.send_keys(date_str)
        text_box.send_keys(Keys.ENTER)
        time.sleep(1)

    def set_start_date(self, when: date) -> None:
        """Set the initial date for the report."""
        self._set_date(when, self.start_xpath)

    def set_end_date(self, when: date) -> None:
        """Set the final date for the report."""
        self._set_date(when, self.end_xpath)

    def update_report(self) -> None:
        """Click the button to fetch interval data or order a report delivery."""
        self.driver.find_element_by_css_selector(self.update_report_selector).click()

    def interval_data(self) -> List[Tuple[datetime, float]]:
        """Gather all of the interval data available on the page.

        This includes pressing the "next" button to see each day's data, so that this one call may trigger
        multiple page renderings.
        """

        if self.error_message_visible():
            return []

        start_box_xpath = "//div[@class='interval-chart']//div//div//h2"
        WebDriverWait(self.driver, 3).until(
            ec.visibility_of_element_located((By.XPATH, start_box_xpath))
        )

        start_box = self.driver.find_element_by_xpath(start_box_xpath)
        date_val = start_box.get_attribute("innerHTML").split("</span>")[1]
        report_date = datetime.strptime(date_val, "%m/%d/%Y")

        row_xpath = "//div[@class='usage-grid']//table//tbody//tr"
        rows = self.driver.find_elements_by_xpath(row_xpath)

        data = []
        for row in rows:
            when_str, kwh_str = None, None
            try:
                when_str = row.find_element_by_xpath(".//th").get_attribute("innerText")
                kwh_str = row.find_element_by_xpath(".//td[2]").get_attribute(
                    "innerText"
                )
                when = dateparser(when_str, default=report_date)
                demand = 4.0 * float(kwh_str)
                data.append((when, demand))
            except (TypeError, AttributeError):
                log.error(
                    "Failed to parse interval datum: %s, %s" % (when_str, kwh_str)
                )

        return data


class ExportEnergyDataPage(ViewEnergyDataPage):
    start_xpath = "//input[@id='startdate']"
    end_xpath = "//input[@id='enddate']"
    update_report_selector = "//span[text()='Export']/parent::button"
    ftp_xpath = "//input[@id='ftp']"

    def update_report(self) -> None:
        """Click the button to fetch interval data or order a report delivery."""
        self.driver.find_element_by_xpath(self.update_report_selector).click()

    def get_order_number(self) -> Optional[str]:
        """Retrieve the serial number of the asynchronous report."""
        try:
            order_statement = (
                WebDriverWait(self.driver, 3)
                .until(
                    ec.visibility_of_element_located(
                        (By.CSS_SELECTOR, "div.histsubsuccess")
                    )
                )
                .text
            )
            return order_statement.split("\n")[1].strip()
        except TimeoutException:
            return None

    def click_ftps_delivery_type(self) -> None:
        self.driver.find_element_by_xpath(self.ftp_xpath).click()


class HomePage:
    def __init__(self, driver):
        self.driver = driver

    def _view_load_successful(self) -> bool:
        view_page_title_xpath = "//span[text()='Meter Read Data']"
        try:
            WebDriverWait(self.driver, 5).until(
                ec.presence_of_element_located((By.XPATH, view_page_title_xpath))
            )
            return True
        except TimeoutException:
            return False

    def _export_load_successful(self) -> bool:
        report_page_title_xpath = "//span[text()='EXPORT ENERGY DATA REPORT']"
        try:
            WebDriverWait(self.driver, 5).until(
                ec.presence_of_element_located((By.XPATH, report_page_title_xpath))
            )
            return True
        except TimeoutException:
            return False

    def go_to_view_energy_data(self) -> ViewEnergyDataPage:
        view_data_xpath = "//span[text()='View Energy Data']/parent::button"

        self.driver.find_element_by_xpath(
            view_data_xpath
        ).click()  # Now click the "View Energy Data" button.

        if not self._view_load_successful():
            raise SmartMeterTexasScraperException(
                "Failed to load View Energy Page for meter."
            )

        return ViewEnergyDataPage(self.driver)

    def go_to_export_energy_data(self):
        export_data_xpath = "//span[text()='Export Energy Data']/parent::button"
        self.driver.find_element_by_xpath(
            export_data_xpath
        ).click()  # Now click the "Export Energy Data" button.

        if not self._export_load_successful():
            raise SmartMeterTexasScraperException(
                "Failed to load Export Energy Page for meter."
            )

        return ExportEnergyDataPage(self.driver)

    def select_meter(self, esiid: str):
        """Select a meter for further reporting according to its ESIID."""

        esiid_radio_button_xpath = "//input[@value='esiid']"
        search_textbox_xpath = "//textarea[@id='search_terms']"
        search_button_selector = "button.meter-search-button"

        WebDriverWait(self.driver, 5).until(
            ec.element_to_be_clickable((By.XPATH, esiid_radio_button_xpath))
        ).click()
        self.driver.find_element_by_xpath(search_textbox_xpath).send_keys(esiid)
        WebDriverWait(self.driver, 5).until(
            ec.element_to_be_clickable((By.CSS_SELECTOR, search_button_selector))
        ).click()

        checkbox_xpath = "//input[@value='%s']" % esiid
        try:
            WebDriverWait(self.driver, 5).until(
                ec.presence_of_element_located((By.XPATH, checkbox_xpath))
            ).click()
        except TimeoutException:
            error = "ESIID not found. Confirm that this meter is authorized on Smart Meter Texas' site."
            raise SmartMeterTexasScraperException(error)

        return


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def _load_successful(self) -> bool:
        error_css_selector = "div.alert.alert-danger"
        try:
            WebDriverWait(self.driver, 5).until(
                ec.presence_of_element_located((By.CSS_SELECTOR, error_css_selector))
            )
            return False
        except TimeoutException:
            return True

    def login(self, userid: str, password: str) -> HomePage:
        self.driver.get("https://www.smartmetertexas.com/CAP/public/")

        try:
            userid_box = WebDriverWait(self.driver, 5).until(
                ec.presence_of_element_located((By.ID, "userid"))
            )
        except TimeoutException:
            raise SmartMeterTexasScraperException("Login page failed to load.")

        password_box = self.driver.find_element_by_id("password")
        signin_button = self.driver.find_element_by_css_selector(
            "button.btn-large.btn-primary"
        )

        userid_box.send_keys(userid)
        password_box.send_keys(password)
        signin_button.click()

        if not self._load_successful():
            raise SmartMeterTexasScraperException("Login failed.")

        return HomePage(self.driver)


class SmartMeterTexasScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "Smart Meter Texas"
        self.login_url = ""

    @property
    def esiid(self):
        return self._configuration.esiid

    def _execute(self):

        # No more than 24 months are available on this service.
        start = max(self.start_date, date.today() - relativedelta(months=23))
        end = min(self.end_date, date.today())
        log.info("Final date range: %s - %s" % (start, end))

        run_async_job = end - start > timedelta(days=30)
        log.info(
            "Data request type: %s"
            % ("asynchronous" if run_async_job else "synchronous")
        )

        self.screenshot("login")
        log.info("Logging in to Smart Meter Texas portal.")
        login_page = LoginPage(self._driver)
        home_page = login_page.login(self.username, self.password)

        self.screenshot("esiid_selection")
        log.info("Selecting ESIID %s" % self.esiid)
        home_page.select_meter(self.esiid)

        log.info("Gathering data...")

        timeline = Timeline(self.start_date, self.end_date)

        if run_async_job:
            detail_page = home_page.go_to_export_energy_data()
            detail_page.set_start_date(start)
            detail_page.set_end_date(end)
            detail_page.click_ftps_delivery_type()
            detail_page.update_report()
            order_number = detail_page.get_order_number()
            if order_number:
                log.info(
                    "Asynchronous data order is in place. Order number: %s"
                    % order_number
                )
            else:
                log.info(
                    "There was an error placing an asynchronous data order for this meter."
                )
            return Results(readings=[])

        # Synchronous Case --- Download interval data one day at a time.
        detail_page = home_page.go_to_view_energy_data()
        current = self.start_date
        while current <= self.end_date:
            log.info("\tFetching data for %s." % current)
            detail_page.set_start_date(current)
            detail_page.set_end_date(current)
            detail_page.update_report()

            self.screenshot(
                "interval_data_%s_%s_%s" % (current.year, current.month, current.day)
            )

            if not detail_page.error_message_visible():
                data = detail_page.interval_data()
                for dt, use_kw in data:
                    timeline.insert(dt, use_kw)
                log.info("\tRecorded %d intervals of data." % len(data))
            else:
                log.info("Failed to load page for date %s." % current)

            current += timedelta(days=1)

        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """Run the SMT Selenium scraper to gather interval data (<30 days) or request a report asynchronously."""
    esiid = (datasource.meta or {}).get("esiid")

    if esiid is None:
        log.info(
            "Missing ESIID for datasource {}, meter {}.".format(
                datasource.oid, meter.oid
            )
        )

    configuration = SmartMeterTexasConfiguration(esiid)

    return run_datafeed(
        SmartMeterTexasScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
