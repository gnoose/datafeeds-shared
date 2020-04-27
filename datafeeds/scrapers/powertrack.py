from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple
import csv
import logging
import os
import time

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.timeline import Timeline
from datafeeds.common.support import Configuration, Results
from datafeeds.common.util.selenium import file_exists_in_dir, clear_downloads
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

logger = None
log = logging.getLogger(__name__)


class PowerTrackConfiguration(Configuration):
    def __init__(self, meter_id: str, site_id: str = None):
        super().__init__(scrape_readings=True)
        self.meter_id = meter_id
        # site id is the Name - eg, "Granite Rock Quarry"
        self.site_id = site_id


class PowerTrackException(Exception):
    pass


class CSVParser:
    def __init__(self, filepath: str):
        self.filepath = filepath

    @staticmethod
    def kwh_to_kw(energy: str) -> float:
        return float(energy) * 4

    @staticmethod
    def csv_str_to_date(datestr: str) -> datetime:
        date_format = "%Y-%m-%d %H:%M:%S"
        return datetime.strptime(datestr, date_format)

    def process_csv(self) -> List[Tuple[datetime, float]]:
        results = []
        msg = "Processing csv at %s" % self.filepath
        log.info(msg)

        with open(self.filepath, mode="r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            header = next(csv_reader)
            msg = "CSV header: %s" % header
            log.info(msg)

            for row in csv_reader:
                msg = "%s, %s" % (row[0], row[1])
                log.debug(msg)
                # the exported csv's return blank values for part of the current day. Just return
                if len(row[1]) == 0:
                    break
                dt = self.csv_str_to_date(row[0])
                kw = self.kwh_to_kw(row[1])
                results.append((dt, kw))

        return results


class SiteStatusPage:
    def __init__(self, driver):
        self.driver = driver

    @staticmethod
    def string_to_date(date_string: str, date_format: str) -> date:
        return datetime.strptime(date_string, date_format).date()

    def get_install_date(self) -> date:
        string_contains_install_date = self.driver.find_element_by_xpath(
            "//div[contains(text(), 'Operating Since:')]"
        ).text
        install_date_string = string_contains_install_date.split(": ")[-1]
        msg = "Installation of site: %s" % install_date_string
        log.info(msg)
        date_format = "%m/%d/%Y"
        install_date = self.string_to_date(install_date_string, date_format)

        return install_date

    def month_view_select(self):
        month = self.driver.find_element_by_xpath("//div[contains(text(), 'Month')]")
        log.info("Selecting month view")
        month.click()

    def fifteen_minute_select(self):
        fifteen_minute = self.driver.find_element_by_xpath(
            "//div[contains(text(), '15 min')]"
        )
        log.info("Selecting 15 minute granularity")
        fifteen_minute.click()

    def hamburger_click(self):
        # TODO find a better selector if possible
        hamburger_selector = (
            "#classic-view > div > div > div:nth-child(2) > div:nth-child(2) > div > div > div > "
            "div > div:nth-child(1) > div:nth-child(2) > div:nth-child(2) > div > span > svg"
        )
        hamburger = self.driver.find_element_by_css_selector(hamburger_selector)
        log.info("Clicking hamburger")
        hamburger.click()

    def download_csv(self) -> str:
        download_csv_xpath = "//div[contains(text(), 'Download CSV Data')]"
        download_csv = self.driver.find_element_by_xpath(download_csv_xpath)
        download_csv.click()
        download_dir = self.driver.download_dir
        filename = self.driver.wait(60).until(
            file_exists_in_dir(download_dir, r".*\.csv$")
        )
        file_path = os.path.join(download_dir, filename)
        return file_path

    def calendar_back_click(self):
        # More svg with Selenium fun
        # https://github.com/seleniumhq/selenium-google-code-issue-archive/issues/6441#issuecomment-192146989
        back_button_xpath = (
            """//*[@class='svg-inline--fa fa-chevron-left fa-w-10 fa-fw hover']"""
        )
        back_button = self.driver.wait().until(
            ec.element_to_be_clickable((By.XPATH, back_button_xpath))
        )
        log.info("clicking back on the calendar")
        back_button.click()

    def get_earliest_shown(self) -> date:
        displayed_date_range = self.driver.find_element_by_class_name(
            "date-time-input-text"
        ).text
        # eg. "Jan 28, 2020 - Jan 30, 2020"
        earliest_shown = displayed_date_range.split("-")[0][:-1]
        date_format = "%b %d, %Y"
        earliest_shown_dt = datetime.strptime(earliest_shown, date_format)
        return datetime.date(earliest_shown_dt)


class PortfolioPage:
    def __init__(self, driver):
        self.driver = driver

    def _status_page_load_successful(self) -> bool:
        status_xpath = (
            """//*[@id="app-view-inner-inner"]/div[1]/div/div[2]/div/div[1]"""
        )
        try:
            WebDriverWait(self.driver, 10).until(
                ec.presence_of_element_located((By.XPATH, status_xpath))
            )
            log.info("Status page loaded")
            return True
        except TimeoutException:
            return False

    def go_to_status_page(self, site_name: Optional[str] = None) -> SiteStatusPage:
        if site_name is None:
            log.warning("Warning: site name not set. Choosing first site found")
            site_name_xpath = """//*[@id="Name0"]/div/div/div/div/div[1]/img[2]"""
        else:
            site_name_xpath = (
                '//*[contains(@class, "fixedDataTableRowLayout_rowWrapper")'
                ' and .//*[text()="{0}"]]'.format(site_name)
            )
        site_name_link = self.driver.find_element_by_xpath(site_name_xpath)
        site_name_link.click()

        if not self._status_page_load_successful():
            raise PowerTrackException("Site status page failed to load")

        return SiteStatusPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def _portfolio_page_load_successful(self) -> bool:
        portfolio_xpath = """//*[@id="portfolio-view"]/div/div[1]/span"""
        try:
            WebDriverWait(self.driver, 10).until(
                ec.presence_of_element_located((By.XPATH, portfolio_xpath))
            )
            return True
        except TimeoutException:
            return False

    def login(self, username: str, password: str) -> PortfolioPage:
        self.driver.get("https://apps.alsoenergy.com/Account/Login")

        try:
            username_box = WebDriverWait(self.driver, 5).until(
                ec.presence_of_element_located((By.ID, "Username"))
            )
        except TimeoutException:
            raise PowerTrackException("Login page failed to load")

        password_box = self.driver.find_element_by_id("Password")
        login_button = self.driver.find_element_by_xpath(
            """//*[@id="loginForm"]/form[1]/div[3]/div/input"""
        )

        username_box.send_keys(username)
        password_box.send_keys(password)
        login_button.click()

        if not self._portfolio_page_load_successful():
            raise PowerTrackException("Login failed")

        return PortfolioPage(self.driver)


class PowerTrackScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "PowerTrack"
        self.browser_name = "Chrome"
        self.install_date = None
        self.readings = {}
        self.login_url = ""
        if hasattr(self._configuration, "site_id"):
            self.site_id = self._configuration.site_id
        else:
            self.site_id = None

    def adjust_start_and_end_dates(self):
        if self.start_date < self.install_date:
            self.start_date = self.install_date
            log.info("Adjusting start date to {}.".format(self.start_date))

        today = datetime.today().date()
        if self.end_date > today:
            self.end_date = today
            log.info("Adjusting end date to {}".format(self.end_date))

        if self.start_date > self.end_date:
            self.end_date = self.start_date + timedelta(days=1)
            log.info("Adjusting end date to {}".format(self.end_date))

    def _execute(self):
        self.screenshot("login")
        log.info("Logging in to Powertrack.")
        login_page = LoginPage(self._driver)

        portfolio_page = login_page.login(self.username, self.password)

        self.screenshot("site selection")
        log.info("Selecting site")
        time.sleep(5)
        status_page = portfolio_page.go_to_status_page(self.site_id)
        time.sleep(15)

        self.install_date = status_page.get_install_date()
        msg = "Installation date is %s" % self.install_date

        self.adjust_start_and_end_dates()

        log.info(msg)
        status_page.month_view_select()
        time.sleep(10)
        status_page.fifteen_minute_select()
        time.sleep(10)
        earliest_shown = status_page.get_earliest_shown()
        one_month = timedelta(days=31)

        while self.end_date < (earliest_shown - one_month):
            msg = "finding where to start. earliest_shown is %s" % earliest_shown
            log.info(msg)
            status_page.calendar_back_click()
            time.sleep(5)
            earliest_shown = status_page.get_earliest_shown()

        # calendar picker is very hard to use; just cycle backwards month at a time getting data
        timeline = Timeline(self.start_date, self.end_date)
        while (self.start_date - one_month) < earliest_shown:
            msg = "gathering data. earliest_shown is %s" % earliest_shown
            log.info(msg)
            status_page.hamburger_click()
            file_path = status_page.download_csv()
            data = CSVParser(file_path).process_csv()
            for dt, use_kw in data:
                timeline.insert(dt, use_kw)
            log.info("\tRecorded %d intervals of data." % len(data))
            log.info("Cleaning up download.")
            clear_downloads(self._driver.download_dir)
            status_page.calendar_back_click()
            earliest_shown = status_page.get_earliest_shown()
            time.sleep(5)

        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    try:
        site_id = datasource.meta.get("site_id")
    except AttributeError:
        log.info("Site ID not set")
        site_id = None

    configuration = PowerTrackConfiguration(meter_id=meter.service_id, site_id=site_id)

    return run_datafeed(
        PowerTrackScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
