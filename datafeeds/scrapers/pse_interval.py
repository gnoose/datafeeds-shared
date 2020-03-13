import os
import sh
import csv
import json
import time
import logging

from datetime import date

from typing import Optional
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parse_date

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from datafeeds import config
from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, DateRange, Results
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class PSEIntervalConfiguration(Configuration):
    def __init__(self, service_id, site_name):
        super().__init__()
        self.service_id = service_id
        self.site_name = site_name


class LoginPage:
    LoginUrl = "https://mydatamanager.pse.com/#/"

    UsernameFieldSelector = 'input[ng-model="userName"]'
    PasswordFieldSelector = 'input[ng-model="password"]'
    LoginButtonSelector = 'button[type="submit"]'

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        self._driver.get(self.LoginUrl)
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.UsernameFieldSelector)
            )
        )

    def get_login_button(self):
        return self._driver.find_element_by_css_selector(self.LoginButtonSelector)

    def login(self, username, password):
        log.info("Entering username.")
        self._driver.fill(self.UsernameFieldSelector, username)
        log.info("Entering password.")
        self._driver.fill(self.PasswordFieldSelector, password)
        log.info("Clicking login button.")
        self.get_login_button().click()
        log.info("Completed login steps.")


class MainMenuPage:
    ReportDropdownSelector = '//span[text()="Reports"]'
    IntervalReportSelector = '//a[text()="Interval Report"]'

    def __init__(self, driver):
        self._driver = driver

    def _click_report_dropdown(self):
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.ReportDropdownSelector))
        )
        # Wait for angular JS to make this element interactive.
        time.sleep(5)

        return self._driver.find_element_by_xpath(self.ReportDropdownSelector).click()

    def _click_interval_report_link(self):
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.IntervalReportSelector))
        )
        # Wait for angular JS to make this element interactive.
        time.sleep(5)

        return self._driver.find_element_by_xpath(self.IntervalReportSelector).click()

    def select_interval_report(self):
        self._click_report_dropdown()
        self._click_interval_report_link()


class FailedDownloadException(Exception):
    pass


class ReportGenerationError(Exception):
    pass


RAW_REPORT_NAME = "PSEIntervalConsumptionReport.csv"


def wait_for_download(driver, timeout=60):
    """Wait for the report download to finish."""
    wait = WebDriverWait(driver, timeout)
    download_dir = driver.download_dir

    filename = wait.until(file_exists_in_dir(download_dir, r"^%s$" % RAW_REPORT_NAME))
    filepath = os.path.join(download_dir, filename)
    return filepath


class IntervalReportPage:
    SiteSelectorPattern = '//select[@name="siteList"]//option[@label="%s"]'
    MeterSelectorPattern = '//select[@name="meterList"]//option[@label="%s"]'

    DateStartSelector = "input[id=serviceStart]"
    DateEndSelector = "input[id=serviceEnd]"

    TableViewOption = '//a[text()="Table"]'

    HiddenProgressSpinner = '//loading-spinner[@class="ng-scope ng-hide"]'
    LoadButton = '//button[text()="Load"]'
    DownloadLink = '//a[@ng-click="csvDownloadData();"]'

    def __init__(self, driver, service_id, site_name):
        self._driver = driver
        self.site_selector = self.SiteSelectorPattern % site_name
        self.meter_selector = self.MeterSelectorPattern % service_id

    def wait_until_ready(self):
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.site_selector))
        )
        # We need to wait for angular JS to make this element
        # interactive.
        time.sleep(5)

    def configure_meter_target(self):
        """Specify which meter and site the report will address."""

        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.site_selector))
        )

        self._driver.find_element_by_xpath(self.site_selector).click()

        log.info("Selected site.")

        # At this point, Angular re-populates the meter dropdown based
        # on the site name.

        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.meter_selector))
        )

        self._driver.find_element_by_xpath(self.meter_selector).click()

    def select_report_window(self, from_dt, to_dt):
        from_dt_str = from_dt.strftime("%m/%d/%y")
        to_dt_str = to_dt.strftime("%m/%d/%y")

        self._driver.find_element_by_css_selector(self.DateStartSelector).clear()
        self._driver.fill(self.DateStartSelector, from_dt_str)

        self._driver.find_element_by_css_selector(self.DateEndSelector).clear()
        self._driver.fill(self.DateEndSelector, to_dt_str)

        self._driver.find_element_by_xpath(self.TableViewOption).click()

    def download_report(self):
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.LoadButton))
        )

        # A short wait here appears to be necessary so that Angular
        # can validate the report configuration and then enable the
        # "Load" button.
        time.sleep(5)

        self._driver.find_element_by_xpath(self.LoadButton).click()

        time.sleep(1)

        # Verify progress spinner is hidden, indicating we are ready
        # to download data.
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.HiddenProgressSpinner))
        )

        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.DownloadLink))
        )

        # Sometimes the table displaces the link to trigger a
        # download. Scroll to the bottom of the page to expose it.
        self._driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")

        self._driver.find_element_by_xpath(self.DownloadLink).click()

        time.sleep(3)
        wait_for_download(self._driver)

        # Now scroll back so that we can configure another download.
        self._driver.execute_script("window.scrollTo(0, 0)")


class PSEIntervalReportParser:
    def __init__(self, from_dt, to_dt):
        self.timeline = Timeline(from_dt, to_dt)

    def save_report(self, from_dt, to_dt):
        """Process the downloaded report and save it to be ingested."""

        # Save a copy of the raw report for inspection.
        # Tag the report by requested date range.
        filename = "pse_interval_raw_%s-%s.csv" % (
            from_dt.strftime("%Y-%m-%d"),
            to_dt.strftime("%Y-%m-%d"),
        )

        f1 = os.path.join(config.WORKING_DIRECTORY, "current", RAW_REPORT_NAME)
        f2 = os.path.join(config.WORKING_DIRECTORY, filename)

        try:
            sh.mv(f1, f2)
        except Exception as e:
            log.info("error moving %s to %s: %s" % (f1, f2, e))
        # Now ingest the stored CSV's data.
        self.parse_csv(f2)

    def parse_csv(self, filepath):
        lines = [line for line in open(filepath)]

        fields = [
            "compare_day_of_week",
            "compare_dt",
            "compare_kWh",
            "day_of_week",
            "dt",
            "kWh",
            "delta",
        ]
        rows = [row for row in csv.DictReader(lines[3:], fieldnames=fields)]

        for row in rows:
            try:
                kWhStr = row.get("kWh")
                if kWhStr == "null":
                    # This form of null data happens often enough that
                    # it's not worth alerting on.
                    continue
                kWh = float(row.get("kWh"))
                dt = parse_date(row.get("dt"))
                self.timeline.insert(dt, kWh * 4)  # Convert to demand.
            except ValueError:
                log.info(
                    "Failed to parse data point: (dt: %s, kWh: %s)"
                    % (row.get("dt"), row.get("kWh"))
                )

    def serialize(self):
        return self.timeline.serialize()


class PSEIntervalScraper(BaseWebScraper):
    # PSE's site has a known failure on these meters in November, 2017, as of 2019-01-18.
    # We need to skip that window.
    BadMeters = {"Z004214102", "Z005535310", "Z003444678"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "PSE Interval"

        global logger
        logger = self._logger

    @property
    def service_id(self):
        return self._configuration.service_id

    @property
    def site_name(self):
        return self._configuration.site_name

    def _execute(self):
        date_range = DateRange(self.start_date, self.end_date)
        parser = PSEIntervalReportParser(self.start_date, self.end_date)

        login_page = LoginPage(self._driver)

        login_page.wait_until_ready()
        self.screenshot("before login")
        login_page.login(self.username, self.password)
        self.screenshot("after login")

        menu_page = MainMenuPage(self._driver)
        menu_page.select_interval_report()

        self.screenshot("interval report")

        report_page = IntervalReportPage(self._driver, self.service_id, self.site_name)
        report_page.configure_meter_target()

        self.screenshot("meter target configured")

        step = relativedelta(months=1)
        for window in date_range.split_iter(step):
            # PSE has a known failure for some meters between 2017-11-05 and 2017-12-05.
            # We skip this month for now.
            if self._configuration.service_id in self.BadMeters:
                if window.start_date <= date(2017, 11, 5):
                    window.start_date = min(window.start_date, date(2017, 11, 4))
                    window.end_date = min(window.end_date, date(2017, 11, 4))
                elif window.end_date >= date(2017, 12, 5):
                    window.start_date = max(window.start_date, date(2017, 12, 6))
                    window.end_date = max(window.end_date, date(2017, 12, 6))
                else:
                    # Window is entirely inside the bad time region, just skip.
                    continue

                if window.start_date == window.end_date:
                    continue

            log.info(
                "Downloading data for period %s - %s."
                % (window.start_date, window.end_date)
            )
            report_page.select_report_window(window.start_date, window.end_date)
            try:
                report_page.download_report()
            except TimeoutException:
                msg = (
                    "The scraper failed to download interval data for "
                    "the date range {} to {}. This may be due to an "
                    "issue with the PSE website."
                ).format(window.start_date, window.end_date)
                raise ReportGenerationError(msg)

            parser.save_report(window.start_date, window.end_date)

        results = parser.serialize()

        # Write the raw interval JSON into the scraper log for easy
        # reference.
        with open(os.path.join(logger.outputpath, "interval_data.json"), "w") as f:
            f.write(json.dumps(results, sort_keys=True, indent=4))

        return Results(readings=results)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = PSEIntervalConfiguration(
        meter.service_id, datasource.meta.get("siteName")
    )

    return run_datafeed(
        PSEIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
