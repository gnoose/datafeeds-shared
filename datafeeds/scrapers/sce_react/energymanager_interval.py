import os
import time
import logging

import datafeeds.scrapers.sce_react.pages as sce_pages
import datafeeds.scrapers.sce_react.errors as sce_errors
from datafeeds.scrapers.sce_react.parser import parse_sce_csv_file

from typing import Optional, List, Dict
from dateutil.relativedelta import relativedelta

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from datafeeds.common.util.pagestate.pagestate import PageStateMachine
from datafeeds.common.util.selenium import file_exists_in_dir

from datafeeds.common.timeline import Timeline
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import DateRange
from datafeeds.common.support import Results
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)

IntermediateReading = Dict[str, Dict[str, List[str]]]


class SceReactEnergyManagerIntervalConfiguration(Configuration):
    """React Energy Manager Scraper configuration

    Current configuration options:
        service_id: The SCE service id to extract data for
    """

    def __init__(self, service_id: str):
        super().__init__(scrape_bills=False, scrape_readings=True)
        self.service_id = service_id


class SceReactEnergyManagerIntervalScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SCE React Energy Manager Interval"
        self.interval_data_timeline = None

    @property
    def service_id(self):
        return self._configuration.service_id

    def define_state_machine(self):
        """Define the flow of this scraper as a state machine"""

        # When we enter a new state, take a screenshot
        def enter_state_callback(state_name):
            self.screenshot("enter_state_{}".format(state_name))

        state_machine = PageStateMachine(self._driver)

        state_machine.on_enter_state(enter_state_callback)

        # We start in the init state, which navigates to the login page
        state_machine.add_state(
            name="init", action=self.init_action, transitions=["login"]
        )

        # Next, we login. On success, we get transferred to the SCE landing page. Else, we go to an error page.
        state_machine.add_state(
            name="login",
            page=sce_pages.SceLoginPage(self._driver),
            action=self.login_action,
            transitions=["landing_page", "login_failed"],
            wait_time=30,
        )

        # We arrive at this state when a login fails
        state_machine.add_state(
            name="login_failed",
            page=sce_pages.SceLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        # This is the landing page, reached upon successful login. From here we load the energy
        # manager application.
        state_machine.add_state(
            name="landing_page",
            page=sce_pages.SceLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=["energy_manager_landing"],
        )

        # After navigating to Energy Manager, we need to specify the "Basic Usage" report type
        state_machine.add_state(
            name="energy_manager_landing",
            page=sce_pages.SceEnergyManagerLandingPage(self._driver),
            action=self.energy_manager_landing_action,
            transitions=["energy_manager_basic_usage"],
        )

        # Finally, we interact with the "Basic Usage" report to dump out some interval data.
        state_machine.add_state(
            name="energy_manager_basic_usage",
            page=sce_pages.SceEnergyManagerBasicUsagePage(self._driver),
            action=self.energy_manager_basic_usage_action,
            transitions=["done"],
        )

        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
        return self.scrape_interval_data()

    def scrape_interval_data(self):
        self.interval_data_timeline = None
        state_machine = self.define_state_machine()
        final_state = state_machine.run()
        if final_state == "done":
            if self.interval_data_timeline:
                serialized = self.interval_data_timeline.serialize()
                self.log_readings(serialized)
                return Results(readings=serialized)
            return Results(readings={})
        raise Exception(
            "The scraper did not reach a finished state, this will require developer attention."
        )

    def init_action(self, _):
        self._driver.get("https://www.sce.com/mysce/login")

    def login_action(self, page: sce_pages.SceLoginPage):
        page.login(self.username, self.password)

    def login_failed_action(self, page: sce_pages.SceLoginFailedPage):
        # Throw an exception on failure to login
        page.raise_on_error()

    def landing_page_action(self, page: sce_pages.SceLandingPage):
        self._driver.get("https://www.sce.com/mysce/energymanager")

    def energy_manager_landing_action(
        self, page: sce_pages.SceEnergyManagerLandingPage
    ):
        # A popup can show up here that ruins our day, so close it
        sce_pages.detect_and_close_survey(self._driver)
        time.sleep(5)
        page.select_basic_usage_report()

    def energy_manager_basic_usage_action(
        self, page: sce_pages.SceEnergyManagerBasicUsagePage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        rval = page.select_service_id(self.service_id)
        log.info("Result of select service id %s: %s", self.service_id, rval)
        self.screenshot("select_service_id")
        page.configure_report()

        date_range = DateRange(self.start_date, self.end_date)
        interval_size = relativedelta(days=30)
        timeline = Timeline(self.start_date, self.end_date)

        for subrange in date_range.split_iter(delta=interval_size):
            log.info("Requesting interval data for dates: %s", subrange)
            start = subrange.start_date
            end = subrange.end_date

            page.set_time_range(start, end)
            self.screenshot("set_time_range")

            try:
                page.generate_report()
                time.sleep(5)
                WebDriverWait(self._driver, 180).until(
                    EC.invisibility_of_element_located(
                        sce_pages.GenericBusyIndicatorLocator
                    )
                )
            except Exception as e:
                raise sce_errors.EnergyManagerReportException(
                    "Failed to load data from Energy Manager"
                ) from e

            try:
                page.raise_on_report_error()
            except sce_errors.EnergyManagerDataNotFoundException:
                log.info("No data found for this time range, continuing...")
                # If a given date range has no interval data, just move on to the next one
                continue

            log.info("Downloading the interval data report.")
            self.clear_csv_downloads()

            try:
                page.download_report()
            except Exception as e:
                raise sce_errors.EnergyManagerReportException(
                    "Failed to load data from Energy Manager"
                ) from e

            try:
                # Wait two minutes for the download to finish
                wait = WebDriverWait(self._driver, 120)
                csv_file_name = wait.until(
                    file_exists_in_dir(self._driver.download_dir, r".*\.csv")
                )
                csv_file_path = os.path.join(self._driver.download_dir, csv_file_name)
                for reading in parse_sce_csv_file(csv_file_path, self.service_id):
                    timeline.insert(reading.dt, reading.value)
            except TimeoutException:
                raise TimeoutException(
                    "Downloading interval data from Energy Manager failed."
                )

        self.interval_data_timeline = timeline

    def clear_csv_downloads(self):
        to_remove = []
        download_dir = self._driver.download_dir
        for filename in os.listdir(download_dir):
            if filename.endswith(".csv"):
                to_remove.append(os.path.join(download_dir, filename))

        for path in to_remove:
            os.remove(path)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceReactEnergyManagerIntervalConfiguration(
        service_id=meter.service_id
    )

    return run_datafeed(
        SceReactEnergyManagerIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
