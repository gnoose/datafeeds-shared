from datetime import date
import logging
from typing import Optional

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


log = logging.getLogger(__name__)


class SVPIntervalConfiguration(Configuration):
    def __init__(self, point_id: str):
        super().__init__(scrape_readings=True)
        self.point_id = point_id


class ReportPage:
    def __init__(self, driver):
        self.driver = driver

    def parse_readings(self) -> IntervalReadings:
        readings: IntervalReadings = {}
        """
        parse data from table (tr.chartColumn)
        4/21/2020 12:15:00 AM 35.75
        for row in find by class chartColumn
            #  the timestamps are for the END of the interval
            timestamp = date_parser.parse(first td) - timedelta(minutes=15)
            # The kWh reading is easier, but we store kW
            kW = float(second td) * 4
            day = timestamp.strftime('%Y-%m-%d')
            if day not in readings:
                data[day] = [0.0] * 96

            idx = int((timestamp.hour * 4) + (timestamp.minute / 15))
            readings[day][idx] = kW
        """
        return readings


class SetupPage:
    def __init__(self, driver):
        self.driver = driver
        driver.get("http://198.182.15.116/itron/data_analyst/asp/data_setup.asp")

    def create_report(self, point_id: str, start: date, end: date) -> ReportPage:
        """
    - switch to frame: frmPointList (self._driver.switch_to.frame(frame_reference=self._driver.find_element_by_xpath(x‌​path="//iframe[@name='frmPointList']"))
      - then to frame name = nodeNav_pointList_main
      - click //input[@name="ch_all"] to clear all
      - click //input[@value=""] where value matches point_id param
    - in main frame
      - #periodSelect - select option with value = 0
      - set #startDate_NativeVal to start (mm/dd/yyyy)
      - set #endDate_NativeVal to send (mm/dd/yyyy)
      - click Create #createButton
        """
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
