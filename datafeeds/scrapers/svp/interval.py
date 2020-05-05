import logging

from datetime import date, timedelta
from typing import Optional
from dateutil.parser import parse as parse_date

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.exceptions import DataSourceConfigurationError
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, IntervalReadings
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

from selenium.webdriver.common.keys import Keys

log = logging.getLogger(__name__)


class SVPIntervalConfiguration(Configuration):
    def __init__(self, point_id: str):
        super().__init__(scrape_readings=True)
        self.point_id = point_id


class ReportPage:
    def __init__(self, driver):
        self.driver = driver

    def parse_readings(self) -> IntervalReadings:
        """Parse data from report table."""

        # dict of {"2017-04-02" : [59.1, 30.2, None, ...], ...}
        readings: IntervalReadings = {}

        for reading_row in self.driver.find_elements_by_css_selector("tr.chartColumn"):
            ts_text = reading_row.find_elements_by_css_selector("td")[0].text.strip()
            kwh_text = reading_row.find_elements_by_css_selector("td")[1].text.strip()
            try:
                #  the timestamps are for the END of the interval
                timestamp = parse_date(ts_text) - timedelta(minutes=15)
                # The kWh reading is easier, but we store kW
                kW = float(kwh_text) * 4
            except Exception:
                log.info(f"skipping invalid data row... text: {reading_row.text}")
                continue

            day = timestamp.strftime("%Y-%m-%d")
            if day not in readings:
                readings[day] = [0.0] * 96

            idx = int((timestamp.hour * 4) + (timestamp.minute / 15))
            readings[day][idx] = kW

        return readings


class SetupPage:
    def __init__(self, driver):
        self.driver = driver
        driver.get("http://198.182.15.116/itron/data_analyst/asp/data_setup.asp")

    def create_report(self, point_id: str, start: date, end: date) -> ReportPage:
        """Enter the start and end dates and click Create"""

        # Switch to the iframe with points list
        self.driver.switch_to.frame("frmPointList")
        self.driver.switch_to.frame("nodeNav_pointList_main")

        # uncheck all point ids
        chkbx = self.driver.find_element_by_xpath('//input[@id="ch_all"]')
        chkbx.click()

        # make sure the ch_all checkbox is unchecked before continuing
        while chkbx.is_selected():
            chkbx.click()

        # check the required point id
        self.driver.find_element_by_xpath(f'//input[@ptid="{point_id}"]').click()

        # exit points list iframe
        self.driver.switch_to.default_content()

        # Set Time Period to Custom Time Period
        self.driver.get_select("#periodSelect").select_by_value("0")

        start_date_input_elem = self.driver.find_element_by_xpath(
            '//input[@id="startDate_NativeVal"]'
        )
        end_date_input_elem = self.driver.find_element_by_xpath(
            '//input[@id="endDate_NativeVal"]'
        )

        # clear existing data first
        clear_keys = "".join([Keys.BACKSPACE * 10])
        log.debug("set start date to %s", start.strftime("%m/%d/%Y"))
        start_date_input_elem.send_keys(clear_keys)
        start_date_input_elem.send_keys(start.strftime("%m/%d/%Y"))
        log.debug("set end date to %s", end.strftime("%m/%d/%Y"))
        end_date_input_elem.send_keys(clear_keys)
        end_date_input_elem.send_keys(end.strftime("%m/%d/%Y"))

        # Click the Create Button
        self.driver.find_element_by_xpath('//input[@id="createButton"]').click()

        return ReportPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> SetupPage:
        self.driver.get("http://198.182.15.116/itron/default.asp")
        self.driver.find_element_by_xpath("//input[@name='username']").send_keys(
            username
        )
        self.driver.find_element_by_xpath("//input[@name='password']").send_keys(
            password
        )
        self.driver.find_element_by_xpath("//input[@name='submitButton']").click()

        return SetupPage(self.driver)


class SVPIntervalScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SVPInterval"

    @property
    def point_id(self):
        return self._configuration.point_id

    def _execute(self):
        login_page = LoginPage(self._driver)
        setup_page = login_page.login(self.username, self.password)
        log.info("Login successful. Loading Data View and Export setup")
        self.screenshot("after login")

        report_page = setup_page.create_report(
            self.point_id, self.start_date, self.end_date
        )
        log.info("Created report.")
        self.screenshot("created report")
        readings = report_page.parse_readings()

        return Results(readings=readings)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    meta = datasource.meta or {}
    if not meta.get("pointid"):
        raise DataSourceConfigurationError("missing pointid in data source config")
    configuration = SVPIntervalConfiguration(meta["pointid"])

    return run_datafeed(
        SVPIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
