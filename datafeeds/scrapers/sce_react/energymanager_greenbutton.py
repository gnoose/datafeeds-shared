import os
import re
import logging
from typing import Optional, Dict, Any

from dateutil.parser import parse as parse_date

from datafeeds.common.exceptions import (
    DataSourceConfigurationError,
    InvalidMeterDataException,
)
from datafeeds.common.util.pagestate.pagestate import PageStateMachine

from datafeeds.common.timeline import Timeline
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status
from datafeeds import config
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
import datafeeds.scrapers.sce_react.pages as sce_pages
from datafeeds.scrapers.sce_react.energymanager_interval import (
    SceReactEnergyManagerIntervalScraper,
)

log = logging.getLogger(__name__)


class SceReactEnergyManagerGreenButtonConfiguration(Configuration):
    """React Energy Manager Scraper configuration

    Current configuration options:
        service_id: The SCE service id to extract data for
        meta: datasource meta
        meter: meter being scraped
    """

    def __init__(
        self, service_id: str, meta: Dict[str, Any], meter: Meter,
    ):
        super().__init__(
            scrape_bills=False, scrape_readings=True,
        )
        self.service_id = service_id
        self.meta = meta
        self.meter = meter


class SceReactEnergyManagerGreenButtonScraper(SceReactEnergyManagerIntervalScraper):
    def _execute(self):
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
            "The scraper did not reach a finished state. This will require developer attention."
        )

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

        # This is the landing page, reached upon successful login. From here click through to the select account page.
        state_machine.add_state(
            name="landing_page",
            page=sce_pages.SceLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=["select_account_page"],
        )

        state_machine.add_state(
            name="select_account_page",
            page=sce_pages.SceEnergyManagerGreenButtonSelectAccounts(self._driver),
            action=self.select_account_page_action,
            transitions=["download_page"],
        )

        state_machine.add_state(
            name="download_page",
            page=sce_pages.SceEnergyManagerGreenButtonDownload(self._driver),
            action=self.download_page_action,
            transitions=["done"],
        )
        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def landing_page_action(self, page: sce_pages.SceLandingPage):
        log.debug("click data sharing")
        sce_pages.detect_and_close_survey(page.driver)
        page.driver.find_element_by_css_selector(
            'a[href="/mysce/abs/dataSharing"]'
        ).click()
        sce_pages.detect_and_close_survey(page.driver)
        log.debug("click green button download")
        sce_pages.detect_and_close_survey(page.driver)
        page.driver.find_element_by_css_selector(
            'a[href="/sma/ESCAA/EscGreenButtonData"'
        ).click()
        sce_pages.detect_and_close_survey(page.driver)

    def select_account_page_action(
        self, page: sce_pages.SceEnergyManagerGreenButtonSelectAccounts
    ):
        page.select_account(self.service_id)

    def download_page_action(self, page: sce_pages.SceEnergyManagerGreenButtonDownload):
        page.download(self.start_date, self.end_date)
        # get filename
        prefix = f"{config.WORKING_DIRECTORY}/current"
        # filename looks like SCE_Usage_3-049-8416-02_10-01-20_to_10-15-20.csv
        filenames = [fn for fn in os.listdir(prefix) if self.service_id in fn]
        if not filenames:
            raise InvalidMeterDataException(
                "missing downloaded file containing %s" % self.service_id
            )
        with open("%s/%s" % (prefix, filenames[0])) as f:
            data = f.read()

        # parse out fields starting with date; save start (1st) date and value
        # add to timeline: self.interval_data_timeline.insert(dt, val)
        # regex to match line starting with: "date time
        starts_with_date_re = re.compile(r'^"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

        lines = data.splitlines()
        log.info(f"downloaded {len(lines)} lines")
        if not lines:
            raise DataSourceConfigurationError(
                "no data downloaded; may need to set serviceAddress metadata"
            )

        to_kw = 60 / self._configuration.meter.interval
        timeline = Timeline(self.start_date, self.end_date)
        for data_line in lines:
            if not starts_with_date_re.match(data_line):
                continue

            # data_line has a \xa0 space before; remove that
            data_line = data_line.replace("\xa0", " ")
            #  "2020-08-15 00:00:00 to 2020-08-15 00:15:00","2.800",""
            from_dt_string = data_line.split(" to ")[0].replace('"', "")
            from_dt = parse_date(from_dt_string)
            _value = data_line.split('"')[3].replace(
                ",", ""
            )  # not sure if there can be commas in the value but remove them if there are...
            # values are kWh: Usage(Real energy in kilowatt-hours); convert to kW using the meter interval
            value = float(_value) * to_kw
            timeline.insert(from_dt, value)

        self.interval_data_timeline = timeline


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceReactEnergyManagerGreenButtonConfiguration(
        service_id=meter.service_id, meta=datasource.meta, meter=meter,
    )

    return run_datafeed(
        SceReactEnergyManagerGreenButtonScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
